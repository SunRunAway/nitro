// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

// receiptStreamBuffer is the number of events queued per subscriber, about 40 seconds of the chain.
const receiptStreamBuffer = 4096

// ReceiptStreamEvent is one notification of the arbstream_subscribe("receipts") subscription.
// Only blocks digested from the transaction streamer are streamed. While such a block is produced,
// one event with Receipt set is sent as soon as each transaction has been executed and accepted into the block,
// before its state is finalised, in transaction order.
// Once the whole block is written to the database, one event with Block set is sent, so reads at that block
// succeed. Events of a block that fails to be produced or written are not retracted; the block is produced again
// from its first transaction. Events a subscriber cannot keep up with are dropped, which the subscriber sees as a gap in
// block numbers or transaction indexes.
type ReceiptStreamEvent struct {
	BlockNumber    hexutil.Uint64   `json:"blockNumber"`
	BlockTimestamp hexutil.Uint64   `json:"blockTimestamp"`
	Receipt        *StreamedReceipt `json:"receipt,omitempty"`
	Block          *StreamedBlock   `json:"block,omitempty"`
}

// StreamedReceipt is the receipt of one accepted transaction; the block hash is not known yet.
type StreamedReceipt struct {
	TransactionIndex hexutil.Uint  `json:"transactionIndex"`
	TxHash           common.Hash   `json:"transactionHash"`
	Logs             []StreamedLog `json:"logs"`
}

// StreamedLog is a log of a streamed receipt; Index is its position in the block.
type StreamedLog struct {
	Address common.Address `json:"address"`
	Topics  []common.Hash  `json:"topics"`
	Data    hexutil.Bytes  `json:"data"`
	Index   hexutil.Uint   `json:"logIndex"`
}

// StreamedBlock marks the end of a produced block whose receipts have all been streamed.
type StreamedBlock struct {
	Hash             common.Hash  `json:"hash"`
	TransactionCount hexutil.Uint `json:"transactionCount"`
}

// ReceiptStream fans out receipt stream events to subscribers without blocking block production.
type ReceiptStream struct {
	mu          sync.Mutex
	subscribers map[*receiptStreamSubscriber]struct{}
}

type receiptStreamSubscriber struct {
	events  chan *ReceiptStreamEvent
	lagging bool // protected by ReceiptStream.mu
}

func newReceiptStream() *ReceiptStream {
	return &ReceiptStream{subscribers: make(map[*receiptStreamSubscriber]struct{})}
}

func (s *ReceiptStream) subscribe() *receiptStreamSubscriber {
	sub := &receiptStreamSubscriber{events: make(chan *ReceiptStreamEvent, receiptStreamBuffer)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscribers[sub] = struct{}{}
	return sub
}

func (s *ReceiptStream) unsubscribe(sub *receiptStreamSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers, sub)
}

// publishReceipt is the receipt hook of arbos.ProduceBlockWithReceiptHook.
func (s *ReceiptStream) publishReceipt(header *types.Header, txIndex int, txHash common.Hash, txLogs []*types.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.subscribers) == 0 {
		return
	}
	// Copy now: the block producer later rewrites the block hash of these logs.
	logs := make([]StreamedLog, len(txLogs))
	for i, entry := range txLogs {
		logs[i] = StreamedLog{Address: entry.Address, Topics: entry.Topics, Data: entry.Data, Index: hexutil.Uint(entry.Index)}
	}
	s.sendLocked(&ReceiptStreamEvent{
		BlockNumber:    hexutil.Uint64(header.Number.Uint64()),
		BlockTimestamp: hexutil.Uint64(header.Time),
		Receipt:        &StreamedReceipt{TransactionIndex: hexutil.Uint(txIndex), TxHash: txHash, Logs: logs},
	})
}

func (s *ReceiptStream) publishBlock(block *types.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.subscribers) == 0 {
		return
	}
	s.sendLocked(&ReceiptStreamEvent{
		BlockNumber:    hexutil.Uint64(block.NumberU64()),
		BlockTimestamp: hexutil.Uint64(block.Time()),
		Block:          &StreamedBlock{Hash: block.Hash(), TransactionCount: hexutil.Uint(len(block.Transactions()))},
	})
}

func (s *ReceiptStream) sendLocked(event *ReceiptStreamEvent) {
	for sub := range s.subscribers {
		select {
		case sub.events <- event:
			sub.lagging = false
		default:
			if !sub.lagging {
				sub.lagging = true
				log.Warn("receipt stream subscriber is lagging, dropping events", "block", uint64(event.BlockNumber))
			}
		}
	}
}

// ReceiptStreamAPI serves the arbstream namespace.
type ReceiptStreamAPI struct {
	stream *ReceiptStream
}

func NewReceiptStreamAPI(stream *ReceiptStream) *ReceiptStreamAPI {
	return &ReceiptStreamAPI{stream: stream}
}

// Receipts subscribes to ReceiptStreamEvent notifications.
func (api *ReceiptStreamAPI) Receipts(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	rpcSub := notifier.CreateSubscription()
	sub := api.stream.subscribe()
	go func() {
		defer api.stream.unsubscribe(sub)
		for {
			select {
			case <-rpcSub.Err():
				return
			case event := <-sub.events:
				// Notify closes the connection on error, which ends the subscription on the client too.
				if err := notifier.Notify(rpcSub.ID, event); err != nil {
					return
				}
			}
		}
	}()
	return rpcSub, nil
}
