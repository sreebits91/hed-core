package router

import("errors";"sync")

var(ErrBufferFull=errors.New("ring buffer full");ErrBufferEmpty=errors.New("ring buffer empty"))

type TransactionPayload struct{TxID string;Namespace string;Payload []byte;Key string}

// RingBuffer is a bounded MPMC queue. The previous CAS implementation advanced
// head before publishing the payload, allowing consumers to observe an unwritten
// slot. This implementation publishes under one short mutex, preserving the API
// while making ordering, race-detector behaviour and shutdown deterministic.
type RingBuffer struct{mu sync.Mutex;items []TransactionPayload;capacity uint64}
func NewRingBuffer(capacity uint64)*RingBuffer{if capacity==0||(capacity&(capacity-1))!=0{panic("capacity must be a power of 2")};return &RingBuffer{items:make([]TransactionPayload,0,capacity),capacity:capacity}}
func(rb *RingBuffer)Push(tx TransactionPayload)error{rb.mu.Lock();defer rb.mu.Unlock();if uint64(len(rb.items))>=rb.capacity{return ErrBufferFull};rb.items=append(rb.items,tx);return nil}
func(rb *RingBuffer)PopBatch(maxBatch uint64)([]TransactionPayload,error){rb.mu.Lock();defer rb.mu.Unlock();if len(rb.items)==0{return nil,ErrBufferEmpty};n:=int(maxBatch);if n<=0||n>len(rb.items){n=len(rb.items)};out:=make([]TransactionPayload,n);copy(out,rb.items[:n]);copy(rb.items,rb.items[n:]);rb.items=rb.items[:len(rb.items)-n];return out,nil}
func(rb *RingBuffer)Length()uint64{rb.mu.Lock();defer rb.mu.Unlock();return uint64(len(rb.items))}
