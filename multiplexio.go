package multiplexio

import (
	"bufio"
	"io"
	"sort"
	"time"
)

// Reader aggregating, according to a given ordering, tokens extracted
// concurrently from a set of io.Reader
type Reader struct {
	io.ReadCloser
}

type extractedToken struct {
	bytes         []byte
	scanSemaphore chan struct{}
}

func sortByTokenDescString(extractedTokens []extractedToken) sort.Interface {
	return byTokenDescString(extractedTokens)
}

type byTokenDescString []extractedToken

func (b byTokenDescString) Len() int {
	return len(b)
}
func (b byTokenDescString) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
func (b byTokenDescString) Less(i, j int) bool {
	return string(b[i].bytes) > string(b[j].bytes)
}

// NewReader returns a new io.ReadCloser multiplexing a set of io.Reader
func NewReader(readers ...io.Reader) *Reader {
	// configuration
	var (
		firstTimeout = time.Second           // how long do we wait for an initial token from each reader?
		timeout      = time.Millisecond      // how long do we wait for tokens after at least one reader produced one?
		split        = bufio.ScanLines       // tokenizing function
		sortBy       = sortByTokenDescString // sorting function defining which token gets out first
	)

	// plumbing tools
	var (
		pipeReader, pipeWriter = io.Pipe()
		ch                     = make(chan extractedToken)
	)

	// goroutines scanning & extracting tokens to feed them into
	// the main goroutine via the channel
	reader2chan := func(reader io.Reader) {
		var (
			scanSemaphore = make(chan struct{})
			scanner       = bufio.NewScanner(reader)
		)
		scanner.Split(split)
		for scanner.Scan() {
			bytes := append(scanner.Bytes(), byte('\n'))
			// send the extracted bytes along with a semaphore to
			// let the main goroutine throttle the scanning
			ch <- extractedToken{
				bytes:         bytes,
				scanSemaphore: scanSemaphore,
			}
			// block until we are asked to consume more
			<-scanSemaphore
		}
		// signal that there is nothing else coming from that routine
		ch <- extractedToken{}
		// TODO: better error handling: check scanner.Err()
	}
	for _, reader := range readers {
		go reader2chan(reader)
	}

	// goroutine feeding into PipeWriter as tokens arrive
	go func() {
		var (
			scanning        = len(readers)
			extractedTokens = make([]extractedToken, 0, len(readers))
			blockMax        = firstTimeout
		)
		for scanning != 0 {
			var (
				timeoutOccured = false
				tokenTimer     = time.After(blockMax)
			)
			for scanning != 0 && !timeoutOccured {
				timer := tokenTimer
				if len(extractedTokens) == 0 {
					// nothing is extracted yet so we need that token to do
					// anything, so no need for timeout that would result in
					// busy-polling
					timer = nil
				}
				select {
				case extractedToken, ok := <-ch:
					if ok && extractedToken.scanSemaphore != nil {
						extractedTokens = append(extractedTokens, extractedToken)
					}
					scanning--
				case <-timer:
					timeoutOccured = true
				}
			}
			if len(extractedTokens) > 0 {
				// sort to get the token we want at the tail
				sort.Sort(sortBy(extractedTokens))
				// get the tail of the list
				extractedToken := extractedTokens[len(extractedTokens)-1]
				// strip that tail for the next run
				extractedTokens = extractedTokens[0 : len(extractedTokens)-1]
				// dump the bytes, blocking until they are consumed
				if _, err := pipeWriter.Write(extractedToken.bytes); err != nil {
					// TODO: gracefully cleanup reader2chan goroutines? close semaphores?
					close(ch)
					break
				}
				// signal that we want more data from the scanner we got that token from
				extractedToken.scanSemaphore <- struct{}{}
				scanning++
			}
			blockMax = timeout
		}
		pipeWriter.Close()
	}()

	return &Reader{pipeReader}
}
