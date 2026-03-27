// wl-audit queries all WL Merkle (code 417) contracts via gRPC to find
// any execute/migrate transactions beyond their initial instantiation.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cordtus/libyaci"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	grpcEndpoint   = "grpc-bd79106337ec-archive.cosmoshub-main.ccvalidators.com:443"
	methodGetTxs   = "cosmos.tx.v1beta1.Service.GetTxsEvent"
	dbConnStr      = "postgres://cosmoflow@10.70.48.113:5432/cosmoflow?sslmode=disable"
	concurrency    = 5
)

type txsResponse struct {
	TxResponses []txResponse `json:"txResponses"`
	Pagination  *pagination  `json:"pagination,omitempty"`
}

type txResponse struct {
	Height string    `json:"height"`
	TxHash string    `json:"txhash"`
	Code   uint32    `json:"code"`
	Events []txEvent `json:"events"`
}

type txEvent struct {
	Type       string        `json:"type"`
	Attributes []txAttribute `json:"attributes"`
}

type txAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type pagination struct {
	NextKey string `json:"nextKey,omitempty"`
	Total   string `json:"total,omitempty"`
}

type result struct {
	Address    string
	Label      string
	ExecuteTxs int
	WasmTxs    int
	MigrateTxs int
	TotalFee   int64
	Actions    []string
	TxHashes   []string
}

func main() {
	fileFlag := flag.String("file", "", "Load contracts from file (address|label per line) instead of DB")
	flag.Parse()

	ctx := context.Background()

	type contract struct {
		addr  string
		label string
	}
	var contracts []contract

	if *fileFlag != "" {
		f, err := os.Open(*fileFlag)
		if err != nil {
			log.Fatalf("Open file: %v", err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "|", 2)
			c := contract{addr: parts[0]}
			if len(parts) > 1 {
				c.label = parts[1]
			}
			contracts = append(contracts, c)
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("Read file: %v", err)
		}
	} else {
		pool, err := pgxpool.New(ctx, dbConnStr)
		if err != nil {
			log.Fatalf("DB connect: %v", err)
		}
		defer pool.Close()

		rows, err := pool.Query(ctx, `
			SELECT address, label FROM roi_tracker.discovered_contracts
			WHERE code_id = 417
			ORDER BY address
		`)
		if err != nil {
			log.Fatalf("Query contracts: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var c contract
			if err := rows.Scan(&c.addr, &c.label); err != nil {
				log.Fatalf("Scan: %v", err)
			}
			contracts = append(contracts, c)
		}
	}

	fmt.Fprintf(os.Stderr, "Loaded %d code-417 contracts\n", len(contracts))

	// Connect gRPC
	client, err := libyaci.Dial(ctx, grpcEndpoint,
		libyaci.WithMaxRecvMsgSize(50*1024*1024),
		libyaci.WithMaxRetries(3),
		libyaci.WithDialTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("gRPC connect: %v", err)
	}
	defer client.Close()

	// Process contracts concurrently
	var (
		processed  atomic.Int64
		withExtra  atomic.Int64
		totalExtra atomic.Int64
		mu         sync.Mutex
		results    []result
	)

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, c := range contracts {
		wg.Add(1)
		sem <- struct{}{}
		go func(addr, label string) {
			defer wg.Done()
			defer func() { <-sem }()

			r := result{Address: addr, Label: label}

			// Query execute and wasm prefixes (instantiate already captured)
			for _, prefix := range []string{"execute._contract_address", "wasm._contract_address"} {
				query := fmt.Sprintf("%s='%s'", prefix, addr)
				params := map[string]any{
					"query":    query,
					"order_by": "ORDER_BY_ASC",
					"pagination": map[string]any{
						"limit": 100,
					},
				}
				paramsJSON, _ := json.Marshal(params)

				resp, err := client.Invoke(methodGetTxs, paramsJSON)
				if err != nil {
					fmt.Fprintf(os.Stderr, "WARN: %s query %s failed: %v\n", addr[:20], prefix, err)
					continue
				}

				var txsResp txsResponse
				if err := json.Unmarshal(resp, &txsResp); err != nil {
					continue
				}

				for _, tx := range txsResp.TxResponses {
					if tx.Code != 0 {
						continue
					}

					isInstantiate := false
					var actions []string
					var fee int64

					for _, evt := range tx.Events {
						switch evt.Type {
						case "instantiate":
							isInstantiate = true
						case "wasm":
							for _, a := range evt.Attributes {
								if a.Key == "action" {
									actions = append(actions, a.Value)
								}
							}
						case "coin_spent":
							for _, a := range evt.Attributes {
								if a.Key == "amount" && strings.HasSuffix(a.Value, "uatom") {
									numStr := strings.TrimSuffix(a.Value, "uatom")
									fmt.Sscanf(numStr, "%d", &fee)
								}
							}
						default:
							if strings.HasPrefix(evt.Type, "wasm-") {
								actions = append(actions, strings.TrimPrefix(evt.Type, "wasm-"))
							}
						}
					}

					if !isInstantiate {
						r.TxHashes = append(r.TxHashes, tx.TxHash)
						r.TotalFee += fee
						r.Actions = append(r.Actions, actions...)
						if strings.Contains(prefix, "execute") {
							r.ExecuteTxs++
						}
					}
				}

				if strings.Contains(prefix, "wasm") {
					r.WasmTxs = len(txsResp.TxResponses)
				}
			}

			// Also check migrate
			query := fmt.Sprintf("migrate._contract_address='%s'", addr)
			params := map[string]any{
				"query":    query,
				"order_by": "ORDER_BY_ASC",
				"pagination": map[string]any{"limit": 100},
			}
			paramsJSON, _ := json.Marshal(params)
			resp, err := client.Invoke(methodGetTxs, paramsJSON)
			if err == nil {
				var txsResp txsResponse
				if json.Unmarshal(resp, &txsResp) == nil {
					r.MigrateTxs = len(txsResp.TxResponses)
				}
			}

			n := processed.Add(1)
			extraTxs := r.ExecuteTxs + r.MigrateTxs
			if extraTxs > 0 {
				withExtra.Add(1)
				totalExtra.Add(int64(extraTxs))
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			}

			if n%100 == 0 {
				fmt.Fprintf(os.Stderr, "Progress: %d/%d checked, %d with extra txs (%d total extra)\n",
					n, len(contracts), withExtra.Load(), totalExtra.Load())
			}
		}(c.addr, c.label)
	}

	wg.Wait()

	// Output results
	fmt.Printf("\n=== WL Merkle (code 417) Audit Results ===\n")
	fmt.Printf("Total contracts checked: %d\n", len(contracts))
	fmt.Printf("Contracts with execute/migrate txs: %d\n", withExtra.Load())
	fmt.Printf("Total extra txs found: %d\n", totalExtra.Load())
	fmt.Printf("\n")

	if len(results) > 0 {
		fmt.Printf("--- Contracts with extra transactions ---\n")
		var totalFee int64
		actionCounts := map[string]int{}
		for _, r := range results {
			fmt.Printf("\n%s (%s)\n", r.Address, r.Label)
			fmt.Printf("  execute_txs=%d  wasm_total=%d  migrate_txs=%d  fee=%d uatom\n",
				r.ExecuteTxs, r.WasmTxs, r.MigrateTxs, r.TotalFee)
			fmt.Printf("  actions: %v\n", r.Actions)
			fmt.Printf("  tx_hashes: %v\n", r.TxHashes)
			totalFee += r.TotalFee
			for _, a := range r.Actions {
				actionCounts[a]++
			}
		}
		fmt.Printf("\n--- Summary ---\n")
		fmt.Printf("Total extra fee: %d uatom (%.6f ATOM)\n", totalFee, float64(totalFee)/1_000_000)
		fmt.Printf("Action breakdown:\n")
		for action, count := range actionCounts {
			fmt.Printf("  %s: %d\n", action, count)
		}
	}
}
