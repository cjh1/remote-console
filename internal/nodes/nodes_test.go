package nodes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateNodes(t *testing.T) {
	newNodes := []NodeConsoleInfo{
		{ID: "x0c0s1b0"},
		{ID: "x0c0s1b1"},
	}

	updateNodes(newNodes)

	// Verify that the nodes were updated correctly
	currentNodes := CurrentNodes()
	require.Equal(t, 2, len(currentNodes), "There should be 2 nodes after update")
	require.Contains(t, currentNodes, "x0c0s1b0", "Node x0c0s1b0 should be present")
	require.Contains(t, currentNodes, "x0c0s1b1", "Node x0c0s1b1 should be present")

	// Remove one node and update again
	newNodes = []NodeConsoleInfo{
		{ID: "x0c0s1b0"},
	}

	updateNodes(newNodes)

	// Verify that the nodes were updated correctly
	currentNodes = CurrentNodes()
	require.Equal(t, 1, len(currentNodes), "There should be 1 node after update")
	require.Contains(t, currentNodes, "x0c0s1b0", "Node x0c0s1b0 should be present")
	require.NotContains(t, currentNodes, "x0c0s1b1", "Node x0c0s1b1 should not be present")

	// Add a new node and update again
	newNodes = []NodeConsoleInfo{
		{ID: "x0c0s1b0"},
		{ID: "x0c0s1b2"},
	}

	updateNodes(newNodes)

	// Verify that the nodes were updated correctly
	currentNodes = CurrentNodes()
	require.Equal(t, 2, len(currentNodes), "There should be 2 nodes after update")
	require.Contains(t, currentNodes, "x0c0s1b0", "Node x0c0s1b0 should be present")
	require.Contains(t, currentNodes, "x0c0s1b2", "Node x0c0s1b2 should be present")
}
