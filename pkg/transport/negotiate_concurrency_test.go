package transport

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestNegotiatorConcurrentUse proves one Negotiator is safe for concurrent
// Compress + Decompress from many goroutines (run with -race). After
// construction the compressor set and lookup map are immutable and every call
// allocates its own state, so the shared request fleet can use one Negotiator.
// NEGATION: if the Negotiator held per-call mutable shared state, -race would
// report a data race and the round-trip check would intermittently FAIL.
func TestNegotiatorConcurrentUse(t *testing.T) {
	n := Default()
	const workers = 32
	const iters = 200

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Each worker uses a distinct body so a cross-goroutine buffer share
			// would surface as a mismatched round trip.
			body := bytes.Repeat([]byte(fmt.Sprintf("w%02d-", id)), 500+id)
			for i := 0; i < iters; i++ {
				enc, err := n.Compress(body)
				if err != nil {
					errs <- fmt.Errorf("worker %d compress: %w", id, err)
					return
				}
				got, err := n.Decompress(enc.Encoding, enc.Body)
				if err != nil {
					errs <- fmt.Errorf("worker %d decompress: %w", id, err)
					return
				}
				if !bytes.Equal(got, body) {
					errs <- fmt.Errorf("worker %d: round trip mismatch (coding %q)", id, enc.Encoding)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
