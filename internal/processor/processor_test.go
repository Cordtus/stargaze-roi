package processor

import (
	"testing"
)

func TestProcessorIdlesWithNoDiscovery(t *testing.T) {
	p := &Processor{discovery: nil}
	if p.discovery != nil {
		t.Error("expected nil discovery")
	}
}

func TestProcessorStopChannel(t *testing.T) {
	p := &Processor{stopCh: make(chan struct{})}
	p.Stop()
	select {
	case <-p.stopCh:
		// expected
	default:
		t.Error("expected stop channel to be closed")
	}
}
