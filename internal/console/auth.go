package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/OpenCHAMI/jwtauth/v5"
	"github.com/lestrrat-go/jwx/jwk"
)

// TokenAuth holds the JWT authentication token
var TokenAuth *jwtauth.JWTAuth

// statusCheckTransport is a custom HTTP transport that checks for non-200 status codes
type statusCheckTransport struct {
	http.RoundTripper
}

// RoundTrip implements http.RoundTripper, returning an error for non-200 responses
func (ct *statusCheckTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return resp, err
}

// newHTTPClient creates an HTTP client with custom transport for status checking
func newHTTPClient() *http.Client {
	return &http.Client{Transport: &statusCheckTransport{}}
}

// FetchPublicKeyFromURL fetches the public key from a JWKS URL and initializes TokenAuth
func FetchPublicKeyFromURL(url string) error {
	client := newHTTPClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	set, err := jwk.Fetch(ctx, url, jwk.WithHTTPClient(client))
	if err != nil {
		msg := "%w"

		// if the error tree contains an EOF, it means that the response was empty,
		// so add a more descriptive message to the error tree
		if errors.Is(err, io.EOF) {
			msg = "received empty response for key: %w"
		}

		return fmt.Errorf(msg, err)
	}

	jwks, err := json.Marshal(set)
	if err != nil {
		return fmt.Errorf("failed to marshal JWKS: %w", err)
	}

	TokenAuth, err = jwtauth.NewKeySet(jwks)
	if err != nil {
		return fmt.Errorf("failed to initialize JWKS: %w", err)
	}

	return nil
}
