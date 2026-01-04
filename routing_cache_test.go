//go:build all || unit
// +build all unit

package gocql

import (
	"testing"
)

func TestSession_SchemaEventClearsRoutingCache(t *testing.T) {
	s := &Session{
		routingMetadataCache: newRoutingKeyInfoLRU(10),
		schemaDescriber:      newSchemaDescriber(nil),
	}

	// Populate cache
	key := s.routingMetadataCache.keyFor("ks", "stmt")
	s.routingMetadataCache.set(key, &StatementMetadata{})

	if s.routingMetadataCache.size() != 1 {
		t.Fatalf("expected cache size 1, got %d", s.routingMetadataCache.size())
	}

	// Trigger schema event (Table change)
	// We use schemaChangeTable to avoid nil pointer in handleKeyspaceChange (which uses s.control)
	event := &schemaChangeTable{
		keyspace: "ks",
		object:   "tb",
		change:   "UPDATED",
	}
	s.handleSchemaEvent([]frame{event})

	// Verify cache cleared
	if s.routingMetadataCache.size() != 0 {
		t.Errorf("expected cache size 0 after schema event, got %d", s.routingMetadataCache.size())
	}
}
