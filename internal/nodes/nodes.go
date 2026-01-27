//package nodes

//  MIT License
//
//  (C) Copyright 2019-2024 Hewlett Packard Enterprise Development LP
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

// Package nodes manages node discovery and state from HSM
package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ConsoleConnectionType string

// componentEndpoints represents components endpoints response from SMD
type componentEndpoints struct {
	ComponentEndpoints []componentEndpoint
}

// TODO do we in need to take into account ManagedBy information?
// componentEndpoint represents SMD component endpoint
type componentEndpoint struct {
	ID                  string              `json:"ID"`
	Type                string              `json:"Type"`
	Enabled             bool                `json:"Enabled,omitempty"`
	RedfishEndpointFQDN string              `json:"RedfishEndpointFQDN,omitempty"`
	RedfishSystemInfo   *redfishSystemInfo  `json:"RedfishSystemInfo,omitempty"`
	RedfishManagerInfo  *redfishManagerInfo `json:"RedfishManagerInfo,omitempty"`
}

// redfishSystemInfo contains computer system information
type redfishSystemInfo struct {
	Name          string         `json:"Name,omitempty"`
	SerialConsole *serialConsole `json:"SerialConsole,omitempty"`
}

// redfishManagerInfo contains BMC/manager information
type redfishManagerInfo struct {
	Name         string        `json:"Name,omitempty"`
	CommandShell *commandShell `json:"CommandShell,omitempty"`
}

// serialConsole describes the serial console capabilities
type serialConsole struct {
	MaxConcurrentSessions int                 `json:"MaxConcurrentSessions,omitempty"`
	SSH                   *consoleServiceInfo `json:"SSH,omitempty"`
	IPMI                  *consoleServiceInfo `json:"IPMI,omitempty"`
	Telnet                *consoleServiceInfo `json:"Telnet,omitempty"`
	WebSocket             *webSocketConsole   `json:"WebSocket,omitempty"`
}

// commandShell describes the command shell capabilities
type commandShell struct {
	ServiceEnabled        bool     `json:"ServiceEnabled,omitempty"`
	MaxConcurrentSessions int      `json:"MaxConcurrentSessions,omitempty"`
	ConnectTypesSupported []string `json:"ConnectTypesSupported,omitempty"`
}

// consoleServiceInfo indicates if a console service is enabled
type consoleServiceInfo struct {
	ServiceEnabled        bool   `json:"ServiceEnabled,omitempty"`
	Port                  int    `json:"Port,omitempty"`
	HotKeySequenceDisplay string `json:"HotKeySequenceDisplay,omitempty"`
	SharedWithManagerCLI  bool   `json:"SharedWithManagerCLI,omitempty"`
	ConsoleEntryCommand   string `json:"ConsoleEntryCommand,omitempty"`
}

type webSocketConsole struct {
	ServiceEnabled bool   `json:"ServiceEnabled"`
	Interactive    bool   `json:"Interactive"`
	ConsoleURI     string `json:"ConsoleURI"`
}

var (
	hardwareUpdateTime      string = "Unknown"
	hardwareUpdateTimeMutex sync.RWMutex
)

// CurrNodesMutex protects access to CurrentNodes
var currNodesMutex = &sync.Mutex{}

// CurrentNodes is the map of all nodes being monitored
var currentNodes map[string]*NodeConsoleInfo = make(map[string]*NodeConsoleInfo)

func getURL(ctx context.Context, httpClient *http.Client, URL string, requestHeaders map[string]string) ([]byte, int, error) {
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
			if closeErr := resp.Body.Close(); closeErr != nil {
				slog.Debug("Failed to close response body after request error", "error", closeErr)
			}
		}
		slog.Error("Error on request", "url", URL, "error", err)
		return nil, -1, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Debug("Failed to close response body", "error", err)
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Error("Error reading response", "error", err)
		return nil, resp.StatusCode, err
	}
	return data, resp.StatusCode, err
}

// getComponentEndpoints queries SMD for the component endpoints
func getComponentEndpoints(ctx context.Context, httpClient *http.Client, smdURL string) ([]componentEndpoint, error) {
	var response componentEndpoints

	// Query smd to get the component endpoints
	URL := smdURL + "hsm/v2/Inventory/ComponentEndpoints"
	data, _, err := getURL(ctx, httpClient, URL, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get component endpoints from hsm: %w", err)
	}

	// decode the response
	err = json.Unmarshal(data, &response)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal component endpoints response: %w", err)
	}

	return response.ComponentEndpoints, nil
}

func serialConsoleToNodeConsoleInfo(endpoint componentEndpoint) *NodeConsoleInfo {
	rf := endpoint.RedfishSystemInfo
	if rf == nil {
		return nil
	}

	sc := rf.SerialConsole
	if sc == nil {
		return nil
	}

	if sc.SSH != nil && sc.SSH.ServiceEnabled {
		return &NodeConsoleInfo{
			ID:                  endpoint.ID,
			ConnectionType:      SSH,
			ConnectionHost:      endpoint.RedfishEndpointFQDN,
			ConnectionPort:      sc.SSH.Port,
			ConsoleEntryCommand: sc.SSH.ConsoleEntryCommand,
		}
	} else if sc.IPMI != nil && sc.IPMI.ServiceEnabled {
		return &NodeConsoleInfo{
			ID:             endpoint.ID,
			ConnectionType: IPMI,
			ConnectionHost: endpoint.RedfishEndpointFQDN,
			ConnectionPort: sc.IPMI.Port,
		}
	} else if sc.Telnet != nil && sc.Telnet.ServiceEnabled || sc.WebSocket != nil && sc.WebSocket.ServiceEnabled {
		slog.Warn("telnet and websocket not supported", "nodeID", endpoint.ID)
	}

	return nil
}

func commandShellToNodeConsoleInfo(endpoint componentEndpoint) *NodeConsoleInfo {
	rf := endpoint.RedfishManagerInfo
	if rf == nil {
		return nil
	}

	cs := rf.CommandShell
	if cs == nil {
		return nil
	}

	for _, ct := range cs.ConnectTypesSupported {
		ctLower := strings.ToLower(ct)
		switch ctLower {
		case SSH:
			return &NodeConsoleInfo{
				ID:             endpoint.ID,
				ConnectionType: SSH,
				ConnectionHost: endpoint.RedfishEndpointFQDN,
			}
		case IPMI:
			return &NodeConsoleInfo{
				ID:             endpoint.ID,
				ConnectionType: IPMI,
				ConnectionHost: endpoint.RedfishEndpointFQDN,
			}
		default:
			slog.Error("unsupported connection type", "type", ct, "nodeID", endpoint.ID)
		}
	}

	return nil
}

// currentNodesFromSMD queries HSM for all node information and returns a slice of NodeConsoleInfo
func currentNodesFromSMD(ctx context.Context, httpClient *http.Client, smdURL string) (nodes []NodeConsoleInfo, err error) {

	slog.Info("Starting to get current nodes on the system")

	endpoints, err := getComponentEndpoints(ctx, httpClient, smdURL)
	if err != nil {
		return nil, fmt.Errorf("unable to get component endpoints: %w", err)
	}

	for _, ep := range endpoints {

		if !ep.Enabled {
			continue
		}

		var nci *NodeConsoleInfo

		// We have SerialConsole info
		if ep.RedfishSystemInfo != nil && ep.RedfishSystemInfo.SerialConsole != nil {
			nci = serialConsoleToNodeConsoleInfo(ep)
		} else if ep.RedfishManagerInfo != nil && ep.RedfishManagerInfo.CommandShell != nil {
			nci = commandShellToNodeConsoleInfo(ep)
		}

		// If we have extracted console information add it to the list
		if nci != nil {
			nodes = append(nodes, *nci)
		}
	}

	slog.Info("Completed getting current nodes on the system")

	return nodes, nil
}

func updateNodes(nodes []NodeConsoleInfo) bool {
	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()

	changed := false
	nodesByID := make(map[string]NodeConsoleInfo, len(nodes))
	for _, nci := range nodes {
		nodesByID[nci.ID] = nci
	}

	for id := range currentNodes {
		if _, ok := nodesByID[id]; !ok {
			delete(currentNodes, id)
			changed = true
		}
	}

	for id, nci := range nodesByID {
		existing, ok := currentNodes[id]
		isNew := !ok
		isNil := ok && existing == nil
		isUpdated := ok && existing != nil && *existing != nci
		if isNew || isNil || isUpdated {
			nciCopy := nci
			currentNodes[id] = &nciCopy
			changed = true
		}
	}

	return changed
}

func CheckForUpdates(ctx context.Context, httpClient *http.Client, smdURL string) bool {
	hardwareUpdateTimeMutex.Lock()
	hardwareUpdateTime = time.Now().Format(time.RFC3339)
	hardwareUpdateTimeMutex.Unlock()

	slog.Info("Getting current nodes from HSM")
	// keep track of if we need to redo the configuration
	changed := false

	fetched_nodes, err := currentNodesFromSMD(ctx, httpClient, smdURL)
	if err != nil {
		slog.Error("Error getting current nodes from SMD", "error", err)
		return false
	}

	slog.Info("Fetched nodes from SMD", "count", len(fetched_nodes))

	changed = updateNodes(fetched_nodes)

	slog.Info("Completed getting current nodes from SMD")

	return changed
}

func CurrentNodes() map[string]*NodeConsoleInfo {
	currNodesMutex.Lock()

	defer currNodesMutex.Unlock()

	// create a copy of the current nodes to return
	nodesCopy := make(map[string]*NodeConsoleInfo)
	for k, v := range currentNodes {
		nodesCopy[k] = v
	}

	return nodesCopy
}

func IsCurrentNode(nodeID string) bool {
	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()

	_, ok := currentNodes[nodeID]
	return ok
}

func GetHardwareUpdateTime() string {
	hardwareUpdateTimeMutex.RLock()
	defer hardwareUpdateTimeMutex.RUnlock()
	return hardwareUpdateTime
}
