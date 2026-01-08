// ...existing code...
//
//  MIT License
//
//  (C) Copyright 2019-2022, 2024 Hewlett Packard Enterprise Development LP
//
//  Permission is hereby granted, free of charge, to any person obtaining a
//  copy of this software and associated documentation files (the "Software"),
//  to deal in the Software without restriction, including without limitation
//  the rights to use, copy, modify, merge, publish, distribute, sublicense,
//  and/or sell copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included
//  in all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
//  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
//  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
//  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.
//

// This file contains helper functions for http interactions

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

type ErrResponse struct {
	E      int    `json:"e"`
	ErrMsg string `json:"err_msg"`
}

func SendJSONError(w http.ResponseWriter, ecode int, message string) {
	httpCode := ecode
	if ecode >= 200 && ecode <= 299 {
		ecode = 0
	}
	data := ErrResponse{
		E:      ecode,
		ErrMsg: message,
	}
	SendResponseJSON(w, httpCode, data)
}

func SendResponseJSON(w http.ResponseWriter, sc int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(sc)
	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		slog.Error("encoding/sending JSON response", "error", err)
		return
	}
}

func PostURL(ctx context.Context, httpClient *http.Client, URL string, requestBody []byte, requestHeaders map[string]string) ([]byte, int, error) {
	if ctx == nil {
		return nil, -1, fmt.Errorf("nil context")
	}
	if httpClient == nil {
		return nil, -1, fmt.Errorf("nil httpClient")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", URL, bytes.NewReader(requestBody))
	if err != nil {
		slog.Error("Error creating new request", "url", URL, "error", err)
		return nil, -1, err
	}
	req.Header.Add("Content-Type", "application/json")
	for k, v := range requestHeaders {
		req.Header.Add(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		slog.Error("Error on request", "url", URL, "error", err)
		return nil, -1, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Error reading response", "error", err)
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, err
}

func GetURL(ctx context.Context, httpClient *http.Client, URL string, requestHeaders map[string]string) ([]byte, int, error) {
	if ctx == nil {
		return nil, -1, fmt.Errorf("nil context")
	}
	if httpClient == nil {
		return nil, -1, fmt.Errorf("nil httpClient")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", URL, nil)
	if err != nil {
		slog.Error("Error creating new request", "url", URL, "error", err)
		return nil, -1, err
	}
	for k, v := range requestHeaders {
		req.Header.Add(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		slog.Error("Error on request", "url", URL, "error", err)
		return nil, -1, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Error reading response", "error", err)
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, err
}
