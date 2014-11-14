package multiplexio

import (
	"bufio"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	line1          = "1 foo\n"
	line2          = "2 barbar\n"
	line3          = "3 quxquxqux\n"
	line4          = "4 bazbazbazbaz\n"
	unfinishedLine = "5 thisisnotacompletetoken"
)

func concatenatedStringsAsBytes(strings ...string) []byte {
	buf := make([]byte, 0, 1024)
	for _, s := range strings {
		buf = append(buf, []byte(s)...)
	}

	return buf
}

func readOneByteAtTheTime(src io.Reader, written *int) []byte {
	buf := make([]byte, 1024)
	for {
		n, err := io.ReadAtLeast(src, buf[*written:], 1)
		if err != nil {
			break
		}
		*written = *written + n
	}
	return buf[:*written]
}

func wrapToken(prefix string, token *[]byte, suffix string) {
	*token = append([]byte(prefix), *token...)
	*token = append(*token, []byte(suffix)...)
}

func TestLazyWrappedReaderFetching(t *testing.T) {
	var (
		pipeReader, pipeWriter = io.Pipe()
		reader                 = NewReader(
			Options{},
			Source{Reader: pipeReader},
		)
	)
	go func() {
		io.WriteString(pipeWriter, line1)
		io.WriteString(pipeWriter, line1)
		io.WriteString(pipeWriter, line1)
		pipeWriter.Close()
	}()
	// ask for enough bytes to get the first token
	io.CopyN(ioutil.Discard, reader, int64(len(line1)))
	go func() {
		// asking for one byte should consume only one more token
		io.CopyN(ioutil.Discard, reader, 1)
	}()
	time.Sleep(20 * time.Millisecond)
	unconsumed, _ := io.Copy(ioutil.Discard, pipeReader)
	// leaving one token that we had no reason to fetch
	expected := int64(len(line1))
	if unconsumed != expected {
		t.Errorf("%v bytes unconsumed by the implementation, %v expected", unconsumed, expected)
	}
}

func TestForwardingSingleSlowReader(t *testing.T) {
	var (
		pipeReader, pipeWriter = io.Pipe()
		reader                 = NewReader(
			Options{},
			Source{Reader: pipeReader},
		)
	)
	go func() {
		time.Sleep(20 * time.Millisecond)
		io.WriteString(pipeWriter, line1)
		time.Sleep(time.Second)
		io.WriteString(pipeWriter, line2)
		time.Sleep(20 * time.Millisecond)
		pipeWriter.Close()
	}()
	written, _ := io.Copy(ioutil.Discard, reader)
	expected := int64(len(line1 + line2))
	if written != expected {
		t.Errorf("%v bytes copied, %v expected", written, expected)
	}
}

func TestForwardingOneSlowerReader(t *testing.T) {
	var (
		pipeReader1, pipeWriter1 = io.Pipe()
		pipeReader2, pipeWriter2 = io.Pipe()
		reader                   = NewReader(
			Options{},
			Source{Reader: pipeReader1},
			Source{Reader: pipeReader2},
		)
	)
	go func() {
		io.WriteString(pipeWriter1, line1)
		pipeWriter1.Close()
	}()
	go func() {
		io.WriteString(pipeWriter2, line2)
		time.Sleep(20 * time.Millisecond)
		io.WriteString(pipeWriter2, line3)
		time.Sleep(20 * time.Millisecond)
		io.WriteString(pipeWriter2, line4)
		pipeWriter2.Close()
	}()
	written, _ := io.Copy(ioutil.Discard, reader)
	expected := int64(len(line1 + line2 + line3 + line4))
	if written != expected {
		t.Errorf("%v bytes copied, %v expected", written, expected)
	}
}

func TestForwardingSingleHangingReader(t *testing.T) {
	var (
		pipeReader, pipeWriter = io.Pipe()
		reader                 = NewReader(
			Options{},
			Source{Reader: pipeReader},
		)
		written     int
		doneReading bool
	)
	go func() {
		io.WriteString(pipeWriter, line1)
		io.WriteString(pipeWriter, unfinishedLine)
	}()
	go func() {
		readOneByteAtTheTime(reader, &written)
		doneReading = true
	}()
	time.Sleep(20 * time.Millisecond)
	expected := len(line1)
	if written != expected {
		t.Errorf("%v bytes copied, %v expected", written, expected)
	}
	if doneReading {
		t.Errorf("reader expected to block but was done reading")
	}
}

func TestForwardingOneHangingReader(t *testing.T) {
	var (
		pipeReader1, pipeWriter1 = io.Pipe()
		pipeReader2, pipeWriter2 = io.Pipe()
		pipeReader3, _           = io.Pipe()
		reader                   = NewReader(
			Options{},
			Source{Reader: pipeReader1},
			Source{Reader: pipeReader2},
			Source{Reader: pipeReader3},
		)
		written     int
		doneReading bool
	)
	go func() {
		io.WriteString(pipeWriter1, line1)
		pipeWriter1.Close()
	}()
	go func() {
		io.WriteString(pipeWriter2, line2)
		pipeWriter2.Close()
	}()
	go func() {
		readOneByteAtTheTime(reader, &written)
		doneReading = true
	}()
	time.Sleep(time.Second + 20*time.Millisecond)
	expected := len(line1 + line2)
	if written != expected {
		t.Errorf("%v bytes copied, %v expected", written, expected)
	}
	if doneReading {
		t.Errorf("reader expected to block but was done reading")
	}
}

func TestForwardingUnfinishedTrailingToken(t *testing.T) {
	var (
		pipeReader, pipeWriter = io.Pipe()
		reader                 = NewReader(
			Options{},
			Source{Reader: pipeReader},
		)
	)
	go func() {
		io.WriteString(pipeWriter, line1)
		io.WriteString(pipeWriter, line2)
		io.WriteString(pipeWriter, unfinishedLine)
		pipeWriter.Close()
	}()
	written, _ := io.Copy(ioutil.Discard, reader)
	expectedAtLeast := int64(len(line1 + line2))
	if written < expectedAtLeast {
		t.Errorf("%v bytes copied, at least %v expected", written, expectedAtLeast)
	}
}

func TestOrderingSequential(t *testing.T) {
	var (
		pipeReader1, pipeWriter1 = io.Pipe()
		pipeReader2, pipeWriter2 = io.Pipe()
		reader                   = NewReader(
			Options{},
			Source{Reader: pipeReader1},
			Source{Reader: pipeReader2},
		)
		expected = make([]byte, 0, 1024)
	)
	go func() {
		// exercise the initial waiting by delaying token
		// availability in the stream that should come first
		time.Sleep(20 * time.Millisecond)
		for i := 0; i < 10; i++ {
			io.WriteString(pipeWriter1, line1)
		}
		pipeWriter1.Close()
	}()
	go func() {
		for i := 0; i < 100; i++ {
			io.WriteString(pipeWriter2, line2)
		}
		pipeWriter2.Close()
	}()
	actual := readOneByteAtTheTime(reader, new(int))
	for i := 0; i < 10; i++ {
		expected = append(expected, []byte(line1)...)
	}
	for i := 0; i < 100; i++ {
		expected = append(expected, []byte(line2)...)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("`%v` read, `%v` expected", string(actual), string(expected))
	}
}

func TestOrderingInterlaced(t *testing.T) {
	var (
		pipeReader1, pipeWriter1 = io.Pipe()
		pipeReader2, pipeWriter2 = io.Pipe()
		reader                   = NewReader(
			Options{},
			Source{Reader: pipeReader1},
			Source{Reader: pipeReader2},
		)
	)
	go func() {
		// exercise the initial waiting by delaying token
		// availability in the stream that should come first
		time.Sleep(20 * time.Millisecond)
		io.WriteString(pipeWriter1, line1)
		io.WriteString(pipeWriter1, line1)
		io.WriteString(pipeWriter1, line3)
		pipeWriter1.Close()
	}()
	go func() {
		io.WriteString(pipeWriter2, line2)
		io.WriteString(pipeWriter2, line4)
		io.WriteString(pipeWriter2, line4)
		pipeWriter2.Close()
	}()
	actual := readOneByteAtTheTime(reader, new(int))
	expected := concatenatedStringsAsBytes(
		line1,
		line1,
		line2,
		line3,
		line4,
		line4,
	)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("`%v` read, `%v` expected", string(actual), string(expected))
	}
}

func TestCustomSplit(t *testing.T) {
	var (
		pipeReader, pipeWriter = io.Pipe()
		reader                 = NewReader(
			Options{Split: bufio.ScanWords},
			Source{Reader: pipeReader},
		)
	)
	go func() {
		io.WriteString(pipeWriter, "1 2 3")
		pipeWriter.Close()
	}()
	actual := readOneByteAtTheTime(reader, new(int))
	expected := []byte("1\n2\n3\n")
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("`%v` read, `%v` expected", string(actual), string(expected))
	}
}

func TestCustomLess(t *testing.T) {
	var (
		ByDescLenLess            = func(i, j []byte) bool { return len(i) > len(j) }
		pipeReader1, pipeWriter1 = io.Pipe()
		pipeReader2, pipeWriter2 = io.Pipe()
		reader                   = NewReader(
			Options{Less: ByDescLenLess},
			Source{Reader: pipeReader1},
			Source{Reader: pipeReader2},
		)
	)
	go func() {
		// exercise the initial waiting by delaying token
		// availability in the stream that should come first
		time.Sleep(20 * time.Millisecond)
		io.WriteString(pipeWriter1, line3)
		io.WriteString(pipeWriter1, line1)
		io.WriteString(pipeWriter1, line1)
		pipeWriter1.Close()
	}()
	go func() {
		io.WriteString(pipeWriter2, line4)
		io.WriteString(pipeWriter2, line4)
		io.WriteString(pipeWriter2, line2)
		pipeWriter2.Close()
	}()
	actual := readOneByteAtTheTime(reader, new(int))
	expected := concatenatedStringsAsBytes(
		line4,
		line4,
		line3,
		line2,
		line1,
		line1,
	)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("`%v` read, `%v` expected", string(actual), string(expected))
	}
}

func TestCustomTransformers(t *testing.T) {
	var (
		pipeReader1, pipeWriter1 = io.Pipe()
		Transform1               = func(token *[]byte) { wrapToken("ZZZZ<", token, ">\n") }
		pipeReader2, pipeWriter2 = io.Pipe()
		Transform2               = func(token *[]byte) { wrapToken("AAAA<", token, ">\n") }
		reader                   = NewReader(
			Options{},
			Source{pipeReader1, Transform1},
			Source{pipeReader2, Transform2},
		)
	)
	go func() {
		// exercise the initial waiting by delaying token
		// availability in the stream that should come first
		io.WriteString(pipeWriter1, line1)
		pipeWriter1.Close()
	}()
	go func() {
		io.WriteString(pipeWriter2, line2)
		pipeWriter2.Close()
	}()
	actual := readOneByteAtTheTime(reader, new(int))
	// Transform2's prefix is "before" Transform1's but it
        // must not affect affect ordering
	expected := concatenatedStringsAsBytes(
		"ZZZZ<",
		strings.Trim(line1, "\n"),
		">\n",
		"AAAA<",
		strings.Trim(line2, "\n"),
		">\n",
	)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("`%v` read, `%v` expected", string(actual), string(expected))
	}
}
