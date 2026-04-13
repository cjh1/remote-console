// Copyright © 2026 OpenCHAMI a Series of LF Projects, LLC
// Copyright © 2023 Hewlett Packard Enterprise Development LP
//
// SPDX-License-Identifier: MIT

package console

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/OpenCHAMI/jwtauth/v5"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/websocket"
	openchami_authenticator "github.com/openchami/chi-middleware/auth"

	"github.com/OpenCHAMI/remote-console/internal/ssh"
)

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// JWT authentication is handled by middleware before upgrade
		// No need for Origin checking since this is for CLI clients
		return true
	},
}

// doConsole dispatches to either interactive or tail mode based on the mode query parameter
func doConsole(consoleLogsPath string, sshMgr *ssh.SSHConsoleManager, w http.ResponseWriter, r *http.Request) {
	// Parse mode parameter (defaults to "interactive")
	params := r.URL.Query()
	mode := params.Get("mode")
	if mode == "" {
		mode = "interactive"
	}

	switch mode {
	case "tail":
		doTailConsole(consoleLogsPath, w, r)
	case "interactive":
		doInteractiveConsole(w, r, sshMgr)
	default:
		http.Error(w, fmt.Sprintf("Invalid mode parameter: %s (must be 'interactive' or 'tail')", mode), http.StatusBadRequest)
	}
}

func SetupRoutes(consoleLogsPath string, sshMgr *ssh.SSHConsoleManager) *chi.Mux {
	router := chi.NewRouter()

	// Add common middleware
	router.Use(middleware.RedirectSlashes)

	// Public routes (no authentication required)
	router.Get("/remote-console/liveness", doLiveness)
	router.Get("/remote-console/readiness", doReadiness)
	router.Get("/remote-console/health", doHealth)

	// Protected routes - add to a sub-router with JWT middleware
	router.Group(func(r chi.Router) {
		// Conditionally add JWT authentication middleware
		if TokenAuth != nil {
			r.Use(
				jwtauth.Verifier(TokenAuth),
				openchami_authenticator.AuthenticatorWithRequiredClaims(TokenAuth, []string{"sub", "iss", "aud"}),
			)
		} else {
			slog.Warn("JWT authentication is disabled - all console endpoints are unprotected")
		}

		r.Get("/remote-console/consoles", doConsoles)
		r.Get("/remote-console/consoles/{nodeID}", func(w http.ResponseWriter, r *http.Request) {
			doConsole(consoleLogsPath, sshMgr, w, r)
		})
	})

	// debug only routes
	// router.Get("/remote-console/info", dbs.doInfo)
	// router.Delete("/remote-console/clearData", dbs.doClearData)
	// router.Post("/remote-console/suspend", dbs.doSuspend)
	// router.Post("/remote-console/resume", dbs.doResume)

	return router
}
