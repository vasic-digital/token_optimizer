package transport

import (
	"bytes"
	"errors"
	"io"
	"net/http"
)

// HTTP-glue errors. Sentinel values so callers can classify with errors.Is.
var (
	// ErrNilRequest is returned by CompressRequestBody for a nil *http.Request.
	ErrNilRequest = errors.New("transport: nil http request")
	// ErrNilResponse is returned by DecodeResponseBody for a nil *http.Response.
	ErrNilResponse = errors.New("transport: nil http response")
)

const headerContentEncoding = "Content-Encoding"

// CompressRequestBody reads req's body in full, compresses it with the
// negotiator (smallest lossless coding), and rewrites req so it carries the
// compressed body: it replaces req.Body, sets req.ContentLength and req.GetBody
// to the compressed bytes, and sets the Content-Encoding header to EXACTLY the
// winning coding — or, when the winner is identity, deletes any Content-Encoding
// header (per RFC 7231 identity is not a sendable coding). Because the header is
// driven by Encoded.ContentEncoding and the winning body already passed the
// round-trip check, the declared Content-Encoding always matches the body and
// the peer can always reconstruct it (§11.4.6). The consumer keeps ownership of
// the *http.Client and the URL; this only rewrites the body + encoding headers,
// so it works against ANY endpoint the consumer chose. A nil req yields
// ErrNilRequest; a req with a nil Body compresses an empty body.
func (n *Negotiator) CompressRequestBody(req *http.Request) (Encoded, error) {
	if req == nil {
		return Encoded{}, ErrNilRequest
	}
	raw, err := drainBody(req.Body)
	if err != nil {
		return Encoded{}, err
	}
	enc, err := n.Compress(raw)
	if err != nil {
		return Encoded{}, err
	}

	if req.Header == nil {
		req.Header = make(http.Header)
	}
	body := enc.Body
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	// GetBody lets net/http transparently retry/redirect the request with the
	// same compressed bytes.
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	if value, set := enc.ContentEncoding(); set {
		req.Header.Set(headerContentEncoding, value)
	} else {
		req.Header.Del(headerContentEncoding)
	}
	return enc, nil
}

// DecodeResponseBody reads resp's body in full and decompresses it according to
// the response's Content-Encoding header, using the negotiator's matching decode
// path. An empty or absent Content-Encoding is treated as identity (no coding).
// It returns ErrUnknownEncoding if the header names a coding the negotiator does
// not have. A nil resp yields ErrNilResponse. The response body is always closed.
func (n *Negotiator) DecodeResponseBody(resp *http.Response) ([]byte, error) {
	if resp == nil {
		return nil, ErrNilResponse
	}
	raw, err := drainBody(resp.Body)
	if err != nil {
		return nil, err
	}
	encoding := ""
	if resp.Header != nil {
		encoding = resp.Header.Get(headerContentEncoding)
	}
	return n.Decompress(encoding, raw)
}

// drainBody reads an http body to completion and closes it. A nil body reads as
// empty (no bytes), so callers need not special-case a request/response with no
// body.
func drainBody(body io.ReadCloser) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	defer body.Close()
	return io.ReadAll(body)
}
