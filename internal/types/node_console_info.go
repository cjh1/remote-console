// MIT License
// (C) Copyright 2019-2024 Hewlett Packard Enterprise Development LP
//
// NodeConsoleInfo type and helpers for console node metadata

package types

import "fmt"

// TODO - Does this have to be a separate package?


const (
	IPMI    = "ipmi"
	SSH  = "ssh"
	WebSocket = "websocket"
	Telnet  = "telnet"
	Oem   = "oem"
)


// NodeConsoleInfo holds all node level information needed to form a console connection
// NOTE: this is the basic unit of information required for each node
// Exported for use by console and creds packages


type NodeConsoleInfo struct {
	ID string // node xname
	ConnectionType string // connection type
	ConnectionHost string // connection host
	ConnectionPort int    // connection port
}

func (nc NodeConsoleInfo) String() string {
	return fmt.Sprintf("ID:%s, ConnectionType:%s, ConnectionHost:%s, ConnectionPort:%d",
		nc.ID, nc.ConnectionType, nc.ConnectionHost, nc.ConnectionPort)
}
