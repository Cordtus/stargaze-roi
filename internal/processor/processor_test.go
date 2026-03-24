package processor

import (
	"testing"
)

func TestProcessorIdlesWithNoContracts(t *testing.T) {
	p := &Processor{contractAddresses: nil}
	if len(p.contractAddresses) != 0 {
		t.Error("expected empty contract list")
	}
}

func TestProcessorHasContracts(t *testing.T) {
	p := &Processor{contractAddresses: []string{"cosmos1abc", "cosmos1def"}}
	if len(p.contractAddresses) != 2 {
		t.Errorf("expected 2 contracts, got %d", len(p.contractAddresses))
	}
}
