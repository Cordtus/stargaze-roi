package processor

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestIsBurnAction(t *testing.T) {
	p := &Processor{burnAction: "burn"}

	tests := []struct {
		action string
		want   bool
	}{
		{"burn", true},
		{"burn_tokens", true},
		{"burn_from", true},
		{"transfer", false},
		{"mint", false},
		{"", false},
	}

	for _, tt := range tests {
		got := p.isBurnAction(tt.action)
		if got != tt.want {
			t.Errorf("isBurnAction(%q) = %v, want %v", tt.action, got, tt.want)
		}
	}
}

func TestExtractBurnAmount(t *testing.T) {
	p := &Processor{burnAttribute: "amount"}

	tests := []struct {
		name   string
		attrs  map[string]string
		want   string
		wantOK bool
	}{
		{
			name:   "standard amount",
			attrs:  map[string]string{"amount": "1000000"},
			want:   "1000000",
			wantOK: true,
		},
		{
			name:   "amount with uatom suffix",
			attrs:  map[string]string{"amount": "500000uatom"},
			want:   "500000",
			wantOK: true,
		},
		{
			name:   "burn_amount fallback",
			attrs:  map[string]string{"burn_amount": "250000"},
			want:   "250000",
			wantOK: true,
		},
		{
			name:   "missing amount",
			attrs:  map[string]string{"action": "burn"},
			wantOK: false,
		},
		{
			name:   "zero amount",
			attrs:  map[string]string{"amount": "0"},
			wantOK: false,
		},
		{
			name:   "negative amount",
			attrs:  map[string]string{"amount": "-100"},
			wantOK: false,
		},
		{
			name:   "invalid amount",
			attrs:  map[string]string{"amount": "not_a_number"},
			wantOK: false,
		},
		{
			name:   "empty attributes",
			attrs:  map[string]string{},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := p.extractBurnAmount(tt.attrs)
			if ok != tt.wantOK {
				t.Fatalf("extractBurnAmount ok = %v, want %v", ok, tt.wantOK)
			}
			if ok {
				expected, _ := decimal.NewFromString(tt.want)
				if !got.Equal(expected) {
					t.Errorf("extractBurnAmount = %s, want %s", got, expected)
				}
			}
		})
	}
}
