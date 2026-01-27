package console

import (
	"net/http"

	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

type ConsolesResponse struct {
	Consoles []nodes.NodeConsoleInfo `json:"consoles"`
}

// doConsoles handles the /consoles endpoint to list all available consoles
func doConsoles(w http.ResponseWriter, r *http.Request) {
	// get the current list of consoles
	nodeList := nodes.CurrentNodes()
	var resp ConsolesResponse
	for _, consoleInfo := range nodeList {
		resp.Consoles = append(resp.Consoles, *consoleInfo)
	}

	// write the output
	sendResponseJSON(w, http.StatusOK, resp)
}
