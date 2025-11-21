package console

import (
	"fmt"
	"net/http"

	"github.com/OpenCHAMI/remote-console/internal/utils"
	"github.com/OpenCHAMI/remote-console/internal/nodes"
)

type ConsolesResponse struct {
	Consoles []nodes.NodeConsoleInfo `json:"consoles"`
}


func doConsoles(w http.ResponseWriter, r *http.Request) {
	// only allow 'GET' calls
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		utils.SendJSONError(w, http.StatusMethodNotAllowed,
			fmt.Sprintf("(%s) Not Allowed", r.Method))
		return
	}

	// get the current list of consoles
	nodeList := nodes.CurrentNodes()
	var resp ConsolesResponse
	for _, consoleInfo := range nodeList {
		resp.Consoles = append(resp.Consoles, *consoleInfo)
	}

	// write the output
	utils.SendResponseJSON(w, http.StatusOK, resp)
}