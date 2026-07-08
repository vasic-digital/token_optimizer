package transport

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHTTPRoundTripRealServer drives a REAL net/http server (httptest, stdlib,
// no external endpoint) end to end: the client compresses the request body, the
// server decompresses using the SAME negotiator + the request's Content-Encoding
// header, echoes the payload back (server-compressed), and the client decodes
// the response. The consumer supplies the client + URL; the package supplies
// only the compression + Content-Encoding negotiation, as designed.
//
// It asserts, per body shape: (1) the payload survives the full round trip
// byte-for-byte; (2) the Content-Encoding the client SENT exactly names the
// coding whose bytes are on the wire (large body => a real coding header set +
// present; tiny body => header absent, i.e. identity); (3) the server's
// independent decode reproduces the input. NEGATION: if CompressRequestBody
// declared a Content-Encoding that did not match the body, the server's decode
// would produce wrong bytes and the echo assertion FAILs.
func TestHTTPRoundTripRealServer(t *testing.T) {
	n := Default()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqEnc := r.Header.Get("Content-Encoding")
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Server independently decodes using the declared coding. If the client
		// lied about Content-Encoding this fails here.
		payload, err := n.Decompress(reqEnc, raw)
		if err != nil {
			http.Error(w, "server decode: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Echo the payload back, itself compressed + labelled.
		respEnc, err := n.Compress(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if v, set := respEnc.ContentEncoding(); set {
			w.Header().Set("Content-Encoding", v)
		}
		_, _ = w.Write(respEnc.Body)
	}))
	defer srv.Close()

	cases := map[string]struct {
		body          []byte
		wantHeaderSet bool // large/compressible => real coding header present
		wantIdentity  bool // tiny/incompressible => identity, header omitted
	}{
		"large-compressible": {bytes.Repeat([]byte("payload-block-"), 4000), true, false},
		"tiny":               {[]byte("ok"), false, true},
		"empty":              {nil, false, true},
	}

	client := srv.Client()
	for name, tc := range cases {
		req, err := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader(tc.body))
		if err != nil {
			t.Fatalf("%s: NewRequest: %v", name, err)
		}
		// Disable transport-level auto-gzip so we observe OUR coding untouched.
		req.Header.Set("Accept-Encoding", "identity")

		enc, err := n.CompressRequestBody(req)
		if err != nil {
			t.Fatalf("%s: CompressRequestBody: %v", name, err)
		}

		// Assertion (2): declared header matches the chosen coding.
		gotHeader := req.Header.Get("Content-Encoding")
		if tc.wantIdentity {
			if enc.Encoding != EncodingIdentity {
				t.Errorf("%s: chose %q, expected identity", name, enc.Encoding)
			}
			if gotHeader != "" {
				t.Errorf("%s: identity must omit Content-Encoding, got %q", name, gotHeader)
			}
		}
		if tc.wantHeaderSet {
			if gotHeader == "" {
				t.Errorf("%s: expected a Content-Encoding header, got none (coding=%q)", name, enc.Encoding)
			}
			if gotHeader != enc.Encoding {
				t.Errorf("%s: header %q != chosen coding %q (would corrupt the wire)", name, gotHeader, enc.Encoding)
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: client.Do: %v", name, err)
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("%s: status %d: %s", name, resp.StatusCode, b)
		}

		got, err := n.DecodeResponseBody(resp)
		if err != nil {
			t.Fatalf("%s: DecodeResponseBody: %v", name, err)
		}
		// Assertion (1): full round trip is byte-exact.
		if !bytes.Equal(got, tc.body) {
			t.Errorf("%s: round trip lost data: got %d bytes, sent %d", name, len(got), len(tc.body))
		}
	}
}

// TestCompressRequestBodySetsLengthAndGetBody proves the rewrite is complete: a
// consumer's http.Client (which may retry/redirect) gets a correct
// ContentLength and a working GetBody producing the SAME compressed bytes.
func TestCompressRequestBodySetsLengthAndGetBody(t *testing.T) {
	n := Default()
	body := bytes.Repeat([]byte("length-check-"), 1000)
	req, err := http.NewRequest(http.MethodPost, "http://example.invalid/ignored", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	enc, err := n.CompressRequestBody(req)
	if err != nil {
		t.Fatal(err)
	}
	if req.ContentLength != int64(enc.Size) {
		t.Errorf("ContentLength = %d, want %d", req.ContentLength, enc.Size)
	}
	if req.GetBody == nil {
		t.Fatal("GetBody was not set")
	}
	rc, err := req.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(replay, enc.Body) {
		t.Error("GetBody produced different bytes than the compressed body")
	}
	// And req.Body itself carries the compressed bytes.
	primary, _ := io.ReadAll(req.Body)
	if !bytes.Equal(primary, enc.Body) {
		t.Error("req.Body != compressed bytes")
	}
}

// TestCompressRequestBodyNilRequest / NilResponse cover the guards.
func TestNilGuards(t *testing.T) {
	n := Default()
	if _, err := n.CompressRequestBody(nil); err != ErrNilRequest {
		t.Errorf("CompressRequestBody(nil): want ErrNilRequest, got %v", err)
	}
	if _, err := n.DecodeResponseBody(nil); err != ErrNilResponse {
		t.Errorf("DecodeResponseBody(nil): want ErrNilResponse, got %v", err)
	}
}

// TestDecodeResponseBodyUnknownEncoding proves the client refuses to hand back a
// still-compressed body as if it were plaintext when the server declares a
// coding the client cannot decode.
func TestDecodeResponseBodyUnknownEncoding(t *testing.T) {
	n := Default()
	resp := &http.Response{
		Header: http.Header{"Content-Encoding": []string{"br"}},
		Body:   io.NopCloser(bytes.NewReader([]byte("brotli-bytes"))),
	}
	if _, err := n.DecodeResponseBody(resp); err == nil {
		t.Fatal("want error for unknown Content-Encoding, got nil")
	}
}
