/*
	go build  .\Tx_sync_observer.go
	.\Tx_sync_observer.exe

	Open your web browser and visit http://localhost:6060/debug/pprof/.

	Download: https://graphviz.org/download/
	In another terminal
	go tool pprof --http=localhost:8080 http://localhost:6060/debug/pprof/profile

*/

package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

// Transaction represents a transaction.
type Transaction struct {
	ID     string
	Amount int
}

// TransactionPool represents a pool of transaction envelopes.
type TransactionPool struct {
	mu           sync.Mutex
	transactions []Transaction
	observers    map[string]chan *Transaction
}

// AcceptTransaction adds a transaction to the pool.
func (tp *TransactionPool) AcceptTransaction(tx Transaction) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.transactions = append(tp.transactions, tx)

	// Notify the waiting observer, if any
	if observer, ok := tp.observers[tx.ID]; ok {
		select {
		case observer <- &tx:
			// Transaction sent to observer
			delete(tp.observers, tx.ID)
			return nil
		case <-time.After(1 * time.Microsecond):
			fmt.Printf("Timeout occurred while notifying observer for transaction %s\n", tx.ID)
			delete(tp.observers, tx.ID)
			return fmt.Errorf("no oberver was listsening for %s", tx.ID)
		}
	}
	return nil
}

// FetchTransaction fetches transactions with the specified IDs from the pool.
// If any transaction is not found, it waits for the transactions to be added and returns them.
func (tp *TransactionPool) FetchTransaction(ids []string) []*Transaction {
	tp.mu.Lock()

	var fetchedTransactions []*Transaction
	missingIDs := make(map[string]bool)

	// Check if the transactions already exist in the pool
	for _, id := range ids {
		found := false
		for _, tx := range tp.transactions {
			if tx.ID == id {
				fetchedTransactions = append(fetchedTransactions, &tx)
				found = true
				break
			}
		}
		if !found {
			missingIDs[id] = true
		}
	}

	// If all transactions are found, return them
	if len(missingIDs) == 0 {
		tp.mu.Unlock()
		return fetchedTransactions
	}

	// Register observers for missing transactions
	observers := make(map[string]chan *Transaction)
	for id := range missingIDs {
		observer := make(chan *Transaction)
		observers[id] = observer
		tp.observers[id] = observer
	}

	tp.mu.Unlock()

	// Wait for missing transactions to be added with a timeout
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(observers))

	for id, observer := range observers {
		go func(id string, observer chan *Transaction) {
			defer waitGroup.Done()
			fmt.Printf("Waiting for transaction %s to be added to the pool...\n", id)
			select {
			case fetchedTx := <-observer:
				fetchedTransactions = append(fetchedTransactions, fetchedTx)
			case <-time.After(1 * time.Second):
				fmt.Printf("Timeout exceeded while waiting for transaction %s\n", id)
				// Handle the timeout scenario (e.g., return an error or perform other actions)
			}
			tp.mu.Lock()
			delete(tp.observers, id)
			tp.mu.Unlock()
		}(id, observer)
	}

	// Wait for all goroutines to finish
	waitGroup.Wait()

	return fetchedTransactions
}

func main() {
	// Enable memory profiling
	f, err := os.Create("memprofile")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	runtime.GC() // Trigger garbage collection to get accurate memory usage
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}

	// Register pprof handlers
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Create a transaction pool
	transactionPool := &TransactionPool{
		observers: make(map[string]chan *Transaction),
	}

	// Start the Goroutine for accepting and pushing transactions
	go func() {
		for i := 1; ; i++ {
			// Generate a new transaction
			tx := Transaction{
				ID:     fmt.Sprintf("Tx%d", i),
				Amount: i * 100,
			}

			// Push the transaction to the pool
			err := transactionPool.AcceptTransaction(tx)
			if err != nil {
				fmt.Printf("Not able to pushed transaction %s to the pool\n", tx.ID)
				fmt.Println(err)
			} else {
				fmt.Printf("Pushed transaction %s to the pool\n", tx.ID)
			}

			// time.Sleep(2 * time.Second) // Wait for 2 seconds before pushing the next transaction
		}
	}()

	// Start the Goroutine for fetching and processing transactions
	go func() {
		for i := 1; ; i++ {
			// Fetch transactions
			txIDs := []string{fmt.Sprintf("Tx%d", i)}
			tx := transactionPool.FetchTransaction(txIDs)
			fmt.Printf("Fetched transactions: %s\n", tx)

			// time.Sleep(1 * time.Second) // Wait for 5 seconds before fetching transactions again
		}
	}()

	// Sleep indefinitely to allow Goroutines to execute
	select {}
}
