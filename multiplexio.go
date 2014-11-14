// Package multiplexio exposes structs implementing and wrapping
// canonical I/O interfaces, allowing aggregation in a real time
// fashion.
package multiplexio

import (
	"bufio"
	"io"
	"sort"
	"time"
)

// NewReader returns an io.ReadCloser aggregating, according to a given
// ordering, tokens extracted concurrently from a set of io.Reader
// wrapped in a set of Source. Tokens of a specific io.Reader go through
// the Transform function passed together wih that io.Reader in the
// Source struct, or NewLineTransform if it isn't set.
//
// By the corresponding functions are not passed in Options,
// bufio.ScanLines is used for scanning and extracting tokens from the
// wrapped io.Reader objects, and ByStringLess is invoked for defining
// the order of these onto the aggregated stream.
func NewReader(options Options, sources ...Source) io.ReadCloser {
	// configuration
	var (
		firstTimeout = time.Second      // how long do we wait for an initial token from each reader?
		timeout      = time.Millisecond // how long do we wait for tokens after at least one reader produced one?
		split        = bufio.ScanLines  // tokenizing function
		less         = ByStringLess     // sorting function defining which token gets out first
	)
	if options.Split != nil {
		split = options.Split
	}
	if options.Less != nil {
		less = options.Less
	}

	// plumbing tools
	var (
		pipeReader, pipeWriter = io.Pipe()
		ch                     = make(chan sourceToken)
	)

	// goroutines scanning & extracting tokens to feed them into
	// the main goroutine via the channel
	source2chan := func(source Source) {
		var (
			scanSemaphore = make(chan struct{})
			scanner       = bufio.NewScanner(source.Reader)
			transform     = source.Transform
		)
		if transform == nil {
			transform = NewLineTransform
		}
		scanner.Split(split)
		for scanner.Scan() {
			bytes := scanner.Bytes()
			transform(&bytes)
			// send the transformed bytes along with a semaphore to
			// let the main goroutine throttle the scanning
			ch <- sourceToken{
				bytes:         bytes,
				scanSemaphore: scanSemaphore,
			}
			// block until we are asked to consume more
			<-scanSemaphore
		}
		// signal that there is nothing else coming from that routine
		ch <- sourceToken{}
		// TODO: better error handling: check scanner.Err()
	}
	for _, source := range sources {
		go source2chan(source)
	}

	// goroutine feeding into PipeWriter as tokens arrive
	go func() {
		var (
			scanning     = len(sources)
			sourceTokens = make([]sourceToken, 0, len(sources))
			blockMax     = firstTimeout
		)
		for scanning != 0 {
			var (
				timeoutOccured = false
				tokenTimer     = time.After(blockMax)
			)
			for scanning != 0 && !timeoutOccured {
				timer := tokenTimer
				if len(sourceTokens) == 0 {
					// nothing is extracted yet so we need that token to do
					// anything, so no need for timeout that would result in
					// busy-polling
					timer = nil
				}
				select {
				case sourceToken, ok := <-ch:
					if ok && sourceToken.scanSemaphore != nil {
						sourceTokens = append(sourceTokens, sourceToken)
					}
					scanning--
				case <-timer:
					timeoutOccured = true
				}
			}
			if len(sourceTokens) > 0 {
				// sort to get the token we want at the tail
				sort.Sort(byTokenSort{less, &sourceTokens})
				// extract the tail from the sorted list
				sourceToken := extractTail(&sourceTokens)
				// dump the bytes, blocking until they are consumed
				if _, err := pipeWriter.Write(sourceToken.bytes); err != nil {
					// TODO: gracefully cleanup reader2chan goroutines? close semaphores?
					close(ch)
					break
				}
				// signal that we want more data from the scanner we got that token from
				sourceToken.scanSemaphore <- struct{}{}
				scanning++
			}
			blockMax = timeout
		}
		pipeWriter.Close()
	}()

	return pipeReader
}

// Source combines an io.Reader from which tokens will be extracted with
// the Transform function that will process them before they make it
// into the aggregated stream.
type Source struct {
	Reader    io.Reader           // incoming stream
	Transform func(token *[]byte) // function used for transforming extracted tokens
}

// Implementation of Source.Transformer adding a line break after each token
func NewLineTransform(token *[]byte) {
	*token = append(*token, byte('\n'))
}

// Options are the options for creating a new Reader.
type Options struct {
	Split bufio.SplitFunc        // function used for scanning and extracting tokens
	Less  func(i, j []byte) bool // function used for ordering extracted tokens, with sort.Interface.Less semantics
}

// Implementation of Options.Less using the canonical string ordering
func ByStringLess(i, j []byte) bool {
	return string(i) < string(j)
}

type sourceToken struct {
	bytes         []byte
	scanSemaphore chan struct{}
}

type byTokenSort struct {
	less   func(i, j []byte) bool
	tokens *[]sourceToken
}

func (b byTokenSort) Len() int {
	return len(*b.tokens)
}
func (b byTokenSort) Swap(i, j int) {
	(*b.tokens)[i], (*b.tokens)[j] = (*b.tokens)[j], (*b.tokens)[i]
}
func (b byTokenSort) Less(i, j int) bool {
	return !b.less((*b.tokens)[i].bytes, (*b.tokens)[j].bytes)
}

func extractTail(sourceTokens *[]sourceToken) sourceToken {
	l := len(*sourceTokens)
	tail := (*sourceTokens)[l-1]
	*sourceTokens = (*sourceTokens)[0 : l-1]
	return tail
}
