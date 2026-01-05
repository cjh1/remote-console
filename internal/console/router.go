//
//  MIT License
//
//  (C) Copyright 2023 Hewlett Packard Enterprise Development LP
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

package console

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

func sendResponseJSON(w http.ResponseWriter, sc int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(sc)

	if data != nil {
		err := json.NewEncoder(w).Encode(data)
		if err != nil {
			slog.Error("Failed to encode JSON response", "error", err)
			return
		}
	}
}

var RequestRouter = chi.NewRouter()

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// Allow connections from any origin for now
		// In production, you should check the origin
		return true
	},
}

func SetupRoutes(consoleLogsPath string) {
	// k8s routes
	RequestRouter.Get("/remote-console/liveness", doLiveness)
	RequestRouter.Get("/remote-console/readiness", doReadiness)
	RequestRouter.Get("/remote-console/health", doHealth)

	RequestRouter.Get("/remote-console/consoles", doConsoles)

	// WebSocket console access endpoints
	// These handle their own errors via WebSocket close frames after upgrade
	RequestRouter.Get("/remote-console/consoles/{nodeID}/tail", func(w http.ResponseWriter, r *http.Request) {
		doTailConsole(consoleLogsPath, w, r)
	})
	RequestRouter.Get("/remote-console/consoles/{nodeID}", doInteractiveConsole)

	// debug only routes
	// router.Get("/remote-console/info", dbs.doInfo)
	// router.Delete("/remote-console/clearData", dbs.doClearData)
	// router.Post("/remote-console/suspend", dbs.doSuspend)
	// router.Post("/remote-console/resume", dbs.doResume)
}
