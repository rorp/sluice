package main

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/swap"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/routing/route"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// loopIn is a loop in swap: the client locks the swap amount in an on-chain htlc, we pay its invoice off-chain and
// claim the htlc with the preimage we obtained.
type loopIn struct {
	status swapStatus

	hash                lntypes.Hash
	amt                 btcutil.Amount
	expiry              int32
	senderKey           [33]byte
	senderInternalKey   [33]byte
	receiverKey         *btcec.PrivateKey
	receiverInternalKey *btcec.PrivateKey
	htlc                *swap.Htlc
	swapInvoice         string
	lastHop             *route.Vertex

	mu                    sync.Mutex
	senderInternalPrivKey *btcec.PrivateKey
}

// payInvoice pays an invoice and waits for the final payment status.
func (s *server) payInvoice(ctx context.Context, invoice string, maxFee btcutil.Amount, lastHop *route.Vertex) (*lndclient.PaymentStatus, error) {
	statuses, errs, err := s.lnd.Router.SendPayment(ctx, lndclient.SendPaymentRequest{
		Invoice:       invoice,
		MaxFee:        maxFee,
		Timeout:       s.cfg.paymentTimout,
		LastHopPubkey: lastHop,
		MaxParts:      16,
	})
	if err != nil {
		return nil, err
	}
	for {
		select {
		case st := <-statuses:
			if st.State == lnrpc.Payment_SUCCEEDED || st.State == lnrpc.Payment_FAILED {
				return &st, nil
			}
		case err := <-errs:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func parseKey33(b []byte, name string) ([33]byte, error) {
	var k [33]byte
	if len(b) != 33 {
		return k, status.Errorf(codes.InvalidArgument, "invalid %v", name)
	}
	if _, err := btcec.ParsePubKey(b); err != nil {
		return k, status.Errorf(codes.InvalidArgument, "invalid %v: %v", name, err)
	}
	copy(k[:], b)
	return k, nil
}

func (s *server) NewLoopInSwap(ctx context.Context, req *swapserverrpc.ServerLoopInRequest) (*swapserverrpc.ServerLoopInResponse, error) {
	log.Infof("NewLoopInSwap hash=%x amt=%v last_hop=%x user_agent=%q protocol=%v", req.SwapHash, req.Amt, req.LastHop, req.UserAgent, req.ProtocolVersion)
	if err := s.checkProtocol(req.ProtocolVersion); err != nil {
		return nil, err
	}
	amt := btcutil.Amount(req.Amt)
	if err := s.checkAmount(amt); err != nil {
		return nil, err
	}
	hash, err := lntypes.MakeHash(req.SwapHash)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	li := &loopIn{hash: hash, amt: amt, swapInvoice: req.SwapInvoice}
	if li.senderKey, err = parseKey33(req.SenderKey, "sender key"); err != nil {
		return nil, err
	}
	if li.senderInternalKey, err = parseKey33(req.SenderInternalPubkey, "sender internal key"); err != nil {
		return nil, err
	}
	if len(req.LastHop) > 0 {
		v, err := route.NewVertexFromBytes(req.LastHop)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid last hop: %v", err)
		}
		li.lastHop = &v
	}
	s.mu.Lock()
	_, exists := s.loopIns[hash]
	s.mu.Unlock()
	if exists {
		return nil, status.Error(codes.AlreadyExists, "swap already exists")
	}

	// The invoice we'll pay must be for the swap hash, and for the swap amount minus our fee.
	_, _, invoiceHash, invoiceAmt, err := swap.DecodeInvoice(s.chainParams, req.SwapInvoice)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid swap invoice: %v", err)
	}
	if invoiceHash != hash {
		return nil, status.Error(codes.InvalidArgument, "swap invoice hash doesn't match swap hash")
	}
	if expected := amt - s.swapFee(amt); invoiceAmt > expected {
		return nil, status.Errorf(codes.InvalidArgument, "swap invoice amount %v exceeds expected %v", invoiceAmt, expected)
	}

	// Probe the route to the client with the probe invoice: the client never settles it, so a successful probe is a
	// payment that fails at the destination with incorrect_or_unknown_payment_details.
	if req.ProbeInvoice != "" {
		if err := s.probe(ctx, req.ProbeInvoice, li.lastHop); err != nil {
			if s.cfg.strictProbe {
				return nil, status.Errorf(codes.FailedPrecondition, "probe failed: %v", err)
			}
			log.Warnf("swap %v: probe failed: %v", hash, err)
		}
	}

	if li.receiverKey, err = btcec.NewPrivateKey(); err != nil {
		return nil, err
	}
	if li.receiverInternalKey, err = btcec.NewPrivateKey(); err != nil {
		return nil, err
	}
	li.expiry = s.currentHeight() + int32(s.cfg.loopInDelta)
	receiverKey := pubKey33(li.receiverKey.PubKey())
	receiverInternalKey := pubKey33(li.receiverInternalKey.PubKey())
	if li.htlc, err = s.htlcV3(li.expiry, li.senderInternalKey, receiverInternalKey, li.senderKey, receiverKey, hash); err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.loopIns[hash] = li
	s.mu.Unlock()
	log.Infof("swap %v: loop in htlc address %v expiry %v", hash, li.htlc.Address, li.expiry)
	go s.runLoopIn(li)

	return &swapserverrpc.ServerLoopInResponse{
		ReceiverKey:            receiverKey[:],
		ReceiverInternalPubkey: receiverInternalKey[:],
		Expiry:                 li.expiry,
		ServerMessage:          "regtest loop server",
	}, nil
}

func (s *server) probe(ctx context.Context, invoice string, lastHop *route.Vertex) error {
	st, err := s.payInvoice(ctx, invoice, 1000, lastHop)
	if err != nil {
		return err
	}
	switch {
	case st.State == lnrpc.Payment_FAILED && st.FailureReason == lnrpc.PaymentFailureReason_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS:
		log.Infof("probe reached the client")
		return nil
	case st.State == lnrpc.Payment_SUCCEEDED:
		return fmt.Errorf("probe invoice was settled")
	default:
		return fmt.Errorf("probe payment failed: %v", st.FailureReason)
	}
}

func (s *server) runLoopIn(li *loopIn) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	fail := func(state swapserverrpc.ServerSwapState, format string, args ...any) {
		log.Warnf("swap %v: "+format, append([]any{li.hash}, args...)...)
		li.status.set(li.hash, state)
	}

	blocks, blockErrs, err := s.lnd.ChainNotifier.RegisterBlockEpochNtfn(ctx)
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "block epochs: %v", err)
		return
	}
	confs, confErrs, err := s.lnd.ChainNotifier.RegisterConfirmationsNtfn(ctx, nil, li.htlc.PkScript, int32(s.cfg.loopInConfs), s.currentHeight())
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "register conf: %v", err)
		return
	}

	// Wait for the client's htlc.
	var htlcTx *wire.MsgTx
	for htlcTx == nil {
		select {
		case conf := <-confs:
			htlcTx = conf.Tx
		case err := <-confErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "conf notification: %v", err)
			return
		case h := <-blocks:
			if h >= li.expiry {
				fail(swapserverrpc.ServerSwapState_SERVER_FAILED_NO_HTLC, "htlc not confirmed before expiry")
				return
			}
		case err := <-blockErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "block epochs: %v", err)
			return
		case <-ctx.Done():
			return
		}
	}
	var outpoint *wire.OutPoint
	var value btcutil.Amount
	for i, out := range htlcTx.TxOut {
		if bytes.Equal(out.PkScript, li.htlc.PkScript) {
			if outpoint != nil {
				fail(swapserverrpc.ServerSwapState_SERVER_FAILED_MULTIPLE_SWAP_SCRIPTS, "multiple htlc outputs in %v", htlcTx.TxHash())
				return
			}
			outpoint = &wire.OutPoint{Hash: htlcTx.TxHash(), Index: uint32(i)}
			value = btcutil.Amount(out.Value)
		}
	}
	if outpoint == nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "htlc output not found in %v", htlcTx.TxHash())
		return
	}
	if value != li.amt {
		fail(swapserverrpc.ServerSwapState_SERVER_FAILED_INVALID_HTLC_AMOUNT, "htlc value %v doesn't match swap amount %v", value, li.amt)
		return
	}
	log.Infof("swap %v: htlc %v confirmed", li.hash, outpoint)
	li.status.set(li.hash, swapserverrpc.ServerSwapState_SERVER_HTLC_CONFIRMED)

	// Pay the client and claim the htlc with the preimage.
	st, err := s.payInvoice(ctx, li.swapInvoice, li.amt/100, li.lastHop)
	if err != nil || st.State != lnrpc.Payment_SUCCEEDED {
		reason := fmt.Sprintf("%v", err)
		if st != nil {
			reason = st.FailureReason.String()
		}
		fail(swapserverrpc.ServerSwapState_SERVER_FAILED_OFF_CHAIN_TIMEOUT, "cannot pay swap invoice: %v", reason)
		return
	}
	log.Infof("swap %v: swap invoice paid (fee %v)", li.hash, st.Fee)

	// Like loop's server, we prefer the key path once the client gave us its internal key, which it does right after
	// we paid it: we give it a few seconds.
	var clientKey *btcec.PrivateKey
	for i := 0; i < 30 && clientKey == nil; i++ {
		li.mu.Lock()
		clientKey = li.senderInternalPrivKey
		li.mu.Unlock()
		if clientKey == nil {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
	if clientKey != nil {
		sweep, err := s.sweepKeyPath(ctx, "loop in sweep "+li.hash.String()[:8], li.htlc, *outpoint, value,
			li.senderInternalKey, pubKey33(li.receiverInternalKey.PubKey()), []*btcec.PrivateKey{clientKey, li.receiverInternalKey})
		if err == nil {
			log.Infof("swap %v: swept htlc in %v (key path)", li.hash, sweep.TxHash())
			li.status.set(li.hash, swapserverrpc.ServerSwapState_SERVER_SUCCESS)
			return
		}
		log.Warnf("swap %v: key path sweep failed, using the script path: %v", li.hash, err)
	} else {
		log.Infof("swap %v: client didn't push its internal key, using the script path", li.hash)
	}
	htlcScript := li.htlc.HtlcScript.(*swap.HtlcScriptV3)
	sweep, err := s.sweepScriptPath(ctx, "loop in sweep "+li.hash.String()[:8], li.htlc, *outpoint, value,
		htlcScript.SuccessScript(), li.receiverKey, htlcScript.SuccessSequence(), 0,
		func(sig []byte) (wire.TxWitness, error) { return li.htlc.GenSuccessWitness(sig, st.Preimage) })
	if err != nil {
		// We've been paid off-chain: the client won't care, but we may lose the htlc if we don't retry.
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "cannot sweep htlc: %v", err)
		return
	}
	log.Infof("swap %v: swept htlc in %v", li.hash, sweep.TxHash())
	li.status.set(li.hash, swapserverrpc.ServerSwapState_SERVER_SUCCESS)
}

// PushKey is called by loop in clients once their invoice is settled, so that we can sweep the htlc through the key
// path without their cooperation. We sweep through the script path right away, so we only check the key.
func (s *server) PushKey(_ context.Context, req *swapserverrpc.ServerPushKeyReq) (*swapserverrpc.ServerPushKeyRes, error) {
	hash, err := lntypes.MakeHash(req.SwapHash)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	log.Infof("PushKey hash=%v", hash)
	s.mu.Lock()
	li, ok := s.loopIns[hash]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown swap %v", hash)
	}
	priv, pub := btcec.PrivKeyFromBytes(req.InternalPrivkey)
	if pubKey33(pub) != li.senderInternalKey {
		return nil, status.Error(codes.InvalidArgument, "internal key doesn't match")
	}
	li.mu.Lock()
	li.senderInternalPrivKey = priv
	li.mu.Unlock()
	return &swapserverrpc.ServerPushKeyRes{}, nil
}
