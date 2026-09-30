// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"context"
	"math/big"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
)

func TestReceiptStreamSubscription(t *testing.T) {
	stream := newReceiptStream()
	server := rpc.NewServer()
	defer server.Stop()
	require.NoError(t, server.RegisterName("arbstream", NewReceiptStreamAPI(stream)))
	ws := httptest.NewServer(server.WebsocketHandler([]string{"*"}))
	defer ws.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := rpc.DialContext(ctx, "ws"+strings.TrimPrefix(ws.URL, "http"))
	require.NoError(t, err)
	defer client.Close()
	events := make(chan ReceiptStreamEvent, 4)
	sub, err := client.Subscribe(ctx, "arbstream", events, "receipts")
	require.NoError(t, err)
	defer sub.Unsubscribe()

	header := &types.Header{Number: big.NewInt(7), Time: 1700000000}
	pool := common.HexToAddress("0x1234")
	topic := common.HexToHash("0xabcd")
	receipt := &types.Receipt{TxHash: common.HexToHash("0x77"), TransactionIndex: 1, Logs: []*types.Log{{Address: pool, Topics: []common.Hash{topic}, Data: []byte{1, 2}, Index: 3}}}
	stream.publishReceipt(header, receipt)
	tx := types.NewTx(&types.LegacyTx{Nonce: 1})
	block := types.NewBlock(header, &types.Body{Transactions: types.Transactions{tx}}, nil, trie.NewStackTrie(nil))
	stream.publishBlock(block)

	next := func() ReceiptStreamEvent {
		t.Helper()
		select {
		case event := <-events:
			return event
		case err := <-sub.Err():
			t.Fatalf("subscription: %v", err)
		case <-ctx.Done():
			t.Fatal("event timeout")
		}
		return ReceiptStreamEvent{}
	}
	got := next()
	require.Equal(t, uint64(7), uint64(got.BlockNumber))
	require.Equal(t, uint64(1700000000), uint64(got.BlockTimestamp))
	require.Nil(t, got.Block)
	require.Equal(t, &StreamedReceipt{TransactionIndex: 1, TxHash: receipt.TxHash, Logs: []StreamedLog{{Address: pool, Topics: []common.Hash{topic}, Data: []byte{1, 2}, Index: 3}}}, got.Receipt)
	got = next()
	require.Nil(t, got.Receipt)
	require.Equal(t, &StreamedBlock{Hash: block.Hash(), TransactionCount: 1}, got.Block)
}

// A subscriber that falls behind must not block block production; the dropped events show up as a gap.
func TestReceiptStreamDropsForLaggingSubscriber(t *testing.T) {
	stream := newReceiptStream()
	sub := stream.subscribe()
	header := &types.Header{Number: big.NewInt(1)}
	publish := func(index uint) {
		stream.publishReceipt(header, &types.Receipt{TransactionIndex: index})
	}
	for i := range uint(receiptStreamBuffer + 2) {
		publish(i)
	}
	for i := range uint(receiptStreamBuffer) {
		require.Equal(t, i, uint((<-sub.events).Receipt.TransactionIndex))
	}
	publish(receiptStreamBuffer + 2)
	require.Equal(t, uint(receiptStreamBuffer+2), uint((<-sub.events).Receipt.TransactionIndex))
}
