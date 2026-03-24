package chain

import (
	"testing"
)

func TestParseUatomAmount(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"6417uatom", 6417},
		{"100000uatom", 100000},
		{"0uatom", 0},
		{"1uatom", 1},
		{"uatom", 0},
		{"", 0},
		{"100ustars", 0},
		{"not_a_number", 0},
		{" 500uatom ", 500}, // TrimSpace handles whitespace
	}

	for _, tt := range tests {
		got := parseUatomAmount(tt.input)
		if got != tt.want {
			t.Errorf("parseUatomAmount(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestExtractContractTxs(t *testing.T) {
	c := &Client{}

	txResps := []txResponse{
		{
			Height: "30000",
			TxHash: "ABCDEF123456",
			Code:   0,
			Events: []txEvent{
				{
					Type: "coin_spent",
					Attributes: []txAttribute{
						{Key: "spender", Value: "cosmos1sender"},
						{Key: "amount", Value: "5000uatom"},
					},
				},
				{
					Type: "wasm",
					Attributes: []txAttribute{
						{Key: "_contract_address", Value: "cosmos1contract"},
						{Key: "action", Value: "create_minter"},
					},
				},
			},
			Timestamp: "2026-01-01T00:00:00Z",
		},
		{
			// Failed tx - should be skipped
			Height: "30001",
			TxHash: "FAILED",
			Code:   1,
			Events: []txEvent{},
		},
	}

	txs := c.extractContractTxs(txResps, "cosmos1contract")

	if len(txs) != 1 {
		t.Fatalf("expected 1 tx, got %d", len(txs))
	}

	tx := txs[0]
	if tx.TxHash != "ABCDEF123456" {
		t.Errorf("TxHash = %q", tx.TxHash)
	}
	if tx.Height != 30000 {
		t.Errorf("Height = %d", tx.Height)
	}
	if tx.FeeUatom != 5000 {
		t.Errorf("FeeUatom = %d, want 5000", tx.FeeUatom)
	}
	if tx.Action != "create_minter" {
		t.Errorf("Action = %q, want create_minter", tx.Action)
	}
	if tx.Sender != "cosmos1sender" {
		t.Errorf("Sender = %q", tx.Sender)
	}
}
