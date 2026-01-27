package nodes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func resetCurrentNodes() {
	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()
	currentNodes = make(map[string]*NodeConsoleInfo)
}

func TestUpdateNodesAdd(t *testing.T) {
	resetCurrentNodes()

	newNodes := []NodeConsoleInfo{
		{
			ID:             "x0c0s1b0",
			ConnectionType: SSH,
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 22,
		},
		{
			ID:             "x0c0s1b2",
			ConnectionType: IPMI,
			ConnectionHost: "x0c0s1b2",
			ConnectionPort: 623,
		},
	}

	changed := updateNodes(newNodes)
	require.True(t, changed, "expected changes when nodes are added")

	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()
	require.Equal(t, 2, len(currentNodes), "There should be 2 nodes after update")
	require.Contains(t, currentNodes, "x0c0s1b0")
	require.Contains(t, currentNodes, "x0c0s1b2")
	require.Equal(t, "x0c0s1b0", currentNodes["x0c0s1b0"].ConnectionHost)
	require.Equal(t, 22, currentNodes["x0c0s1b0"].ConnectionPort)
}

func TestUpdateNodesUpdate(t *testing.T) {
	resetCurrentNodes()

	currNodesMutex.Lock()
	currentNodes["x0c0s1b0"] = &NodeConsoleInfo{
		ID:             "x0c0s1b0",
		ConnectionType: SSH,
		ConnectionHost: "x0c0s1b0",
		ConnectionPort: 22,
	}
	currNodesMutex.Unlock()

	newNodes := []NodeConsoleInfo{
		{
			ID:             "x0c0s1b0",
			ConnectionType: SSH,
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 2222,
		},
	}

	changed := updateNodes(newNodes)
	require.True(t, changed, "expected changes when nodes are updated")

	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()
	require.Equal(t, 1, len(currentNodes))
	require.Equal(t, "x0c0s1b0", currentNodes["x0c0s1b0"].ConnectionHost)
	require.Equal(t, 2222, currentNodes["x0c0s1b0"].ConnectionPort)
}

func TestUpdateNodesRemove(t *testing.T) {
	resetCurrentNodes()

	currNodesMutex.Lock()
	currentNodes["x0c0s1b0"] = &NodeConsoleInfo{
		ID:             "x0c0s1b0",
		ConnectionType: SSH,
		ConnectionHost: "x0c0s1b0",
		ConnectionPort: 22,
	}
	currentNodes["x0c0s1b1"] = &NodeConsoleInfo{
		ID:             "x0c0s1b1",
		ConnectionType: IPMI,
		ConnectionHost: "x0c0s1b1",
		ConnectionPort: 623,
	}
	currNodesMutex.Unlock()

	newNodes := []NodeConsoleInfo{
		{
			ID:             "x0c0s1b0",
			ConnectionType: SSH,
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 22,
		},
	}

	changed := updateNodes(newNodes)
	require.True(t, changed, "expected changes when nodes are removed")

	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()
	require.Equal(t, 1, len(currentNodes))
	require.Contains(t, currentNodes, "x0c0s1b0")
	require.NotContains(t, currentNodes, "x0c0s1b1")
}

func TestUpdateNodesNoChange(t *testing.T) {
	resetCurrentNodes()

	currNodesMutex.Lock()
	currentNodes["x0c0s1b0"] = &NodeConsoleInfo{
		ID:             "x0c0s1b0",
		ConnectionType: SSH,
		ConnectionHost: "x0c0s1b0",
		ConnectionPort: 22,
	}
	currentNodes["x0c0s1b1"] = &NodeConsoleInfo{
		ID:             "x0c0s1b1",
		ConnectionType: IPMI,
		ConnectionHost: "x0c0s1b1",
		ConnectionPort: 623,
	}
	currNodesMutex.Unlock()

	newNodes := []NodeConsoleInfo{
		{
			ID:             "x0c0s1b0",
			ConnectionType: SSH,
			ConnectionHost: "x0c0s1b0",
			ConnectionPort: 22,
		},
		{
			ID:             "x0c0s1b1",
			ConnectionType: IPMI,
			ConnectionHost: "x0c0s1b1",
			ConnectionPort: 623,
		},
	}

	changed := updateNodes(newNodes)
	require.False(t, changed, "expected no changes when data is identical")

	currNodesMutex.Lock()
	defer currNodesMutex.Unlock()
	require.Equal(t, 2, len(currentNodes))
	require.Contains(t, currentNodes, "x0c0s1b0")
	require.Contains(t, currentNodes, "x0c0s1b1")
}
