package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/loop/swap"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightninglabs/loop/utils"
	invpkg "github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// loopOut is a loop out swap: the client pays our hold invoices off-chain and we lock the swap amount in an on-chain
// htlc that the client can claim with the preimage.
type loopOut struct {
	status swapStatus

	hash          lntypes.Hash
	amt           btcutil.Amount
	expiry        int32
	senderKey     *btcec.PrivateKey
	receiverKey   [33]byte
	htlc          *swap.Htlc
	swapInvoice   string
	prepayInvoice string
	paymentAddr   [32]byte
	prepayPreimg  lntypes.Preimage

	mu        sync.Mutex
	outpoint  *wire.OutPoint
	preimage  *lntypes.Preimage
	cancelled bool
	cancelCh  chan struct{}
}

func (lo *loopOut) getOutpoint() *wire.OutPoint {
	lo.mu.Lock()
	defer lo.mu.Unlock()
	return lo.outpoint
}

func (lo *loopOut) getPreimage() *lntypes.Preimage {
	lo.mu.Lock()
	defer lo.mu.Unlock()
	return lo.preimage
}

func (s *server) NewLoopOutSwap(ctx context.Context, req *swapserverrpc.ServerLoopOutRequest) (*swapserverrpc.ServerLoopOutResponse, error) {
	log.Infof("NewLoopOutSwap hash=%x amt=%v expiry=%v user_agent=%q protocol=%v", req.SwapHash, req.Amt, req.Expiry, req.UserAgent, req.ProtocolVersion)
	if err := s.checkProtocol(req.ProtocolVersion); err != nil {
		return nil, err
	}
	amt := btcutil.Amount(req.Amt)
	if err := s.checkAmount(amt); err != nil {
		return nil, err
	}
	if err := s.checkLoopOutExpiry(req.Expiry); err != nil {
		return nil, err
	}
	hash, err := lntypes.MakeHash(req.SwapHash)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(req.ReceiverKey) != 33 {
		return nil, status.Error(codes.InvalidArgument, "invalid receiver key")
	}
	if _, err := btcec.ParsePubKey(req.ReceiverKey); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid receiver key: %v", err)
	}
	s.mu.Lock()
	_, exists := s.loopOuts[hash]
	s.mu.Unlock()
	if exists {
		return nil, status.Error(codes.AlreadyExists, "swap already exists")
	}

	lo := &loopOut{hash: hash, amt: amt, expiry: req.Expiry, cancelCh: make(chan struct{})}
	copy(lo.receiverKey[:], req.ReceiverKey)
	if lo.senderKey, err = btcec.NewPrivateKey(); err != nil {
		return nil, err
	}
	senderKey := pubKey33(lo.senderKey.PubKey())
	if lo.htlc, err = s.htlcV3(req.Expiry, senderKey, lo.receiverKey, senderKey, lo.receiverKey, hash); err != nil {
		return nil, err
	}
	if _, err := rand.Read(lo.prepayPreimg[:]); err != nil {
		return nil, err
	}

	// The off-chain swap htlc must outlive the on-chain htlc: we only learn the preimage when the client claims it.
	finalCltvDelta := uint64(req.Expiry-s.currentHeight()) + uint64(s.cfg.cltvExtra)
	swapFee := s.swapFee(amt)
	prepayAmt := btcutil.Amount(s.cfg.prepayAmt)
	lo.swapInvoice, err = s.lnd.Invoices.AddHoldInvoice(ctx, &invoicesrpc.AddInvoiceData{
		Memo:       "swap " + hash.String()[:8],
		Hash:       &hash,
		Value:      lnwire.NewMSatFromSatoshis(amt + swapFee - prepayAmt),
		Expiry:     3600,
		CltvExpiry: finalCltvDelta,
	})
	if err != nil {
		return nil, err
	}
	prepayHash := hashOf(lo.prepayPreimg)
	lo.prepayInvoice, err = s.lnd.Invoices.AddHoldInvoice(ctx, &invoicesrpc.AddInvoiceData{
		Memo:   "prepay " + hash.String()[:8],
		Hash:   &prepayHash,
		Value:  lnwire.NewMSatFromSatoshis(prepayAmt),
		Expiry: 3600,
	})
	if err != nil {
		return nil, err
	}
	paymentAddr, err := utils.ObtainSwapPaymentAddr(lo.swapInvoice, s.chainParams)
	if err != nil {
		return nil, err
	}
	lo.paymentAddr = *paymentAddr

	s.mu.Lock()
	s.loopOuts[hash] = lo
	s.mu.Unlock()
	log.Infof("swap %v: loop out htlc address %v", hash, lo.htlc.Address)
	go s.runLoopOut(lo)

	return &swapserverrpc.ServerLoopOutResponse{
		SwapInvoice:   lo.swapInvoice,
		PrepayInvoice: lo.prepayInvoice,
		SenderKey:     senderKey[:],
		Expiry:        req.Expiry,
		ServerMessage: "regtest loop server",
	}, nil
}

func (s *server) cancelLoopOutInvoices(lo *loopOut) {
	for _, h := range []lntypes.Hash{lo.hash, hashOf(lo.prepayPreimg)} {
		if err := s.lnd.Invoices.CancelInvoice(context.Background(), h); err != nil {
			log.Warnf("swap %v: cannot cancel invoice %v: %v", lo.hash, h, err)
		}
	}
}

func (s *server) runLoopOut(lo *loopOut) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	fail := func(state swapserverrpc.ServerSwapState, format string, args ...any) {
		log.Warnf("swap %v: "+format, append([]any{lo.hash}, args...)...)
		lo.status.set(lo.hash, state)
	}

	blocks, blockErrs, err := s.lnd.ChainNotifier.RegisterBlockEpochNtfn(ctx)
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "block epochs: %v", err)
		return
	}

	// Wait for the client to lock both payments.
	swapUpdates, swapErrs, err := s.lnd.Invoices.SubscribeSingleInvoice(ctx, lo.hash)
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "subscribe swap invoice: %v", err)
		return
	}
	prepayUpdates, prepayErrs, err := s.lnd.Invoices.SubscribeSingleInvoice(ctx, hashOf(lo.prepayPreimg))
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "subscribe prepay invoice: %v", err)
		return
	}
	swapAccepted, prepayAccepted := false, false
	for !swapAccepted || !prepayAccepted {
		select {
		case u := <-swapUpdates:
			if u.State == invpkg.ContractAccepted {
				log.Infof("swap %v: swap payment accepted (%v)", lo.hash, u.AmtPaid)
				swapAccepted = true
			}
		case u := <-prepayUpdates:
			if u.State == invpkg.ContractAccepted {
				log.Infof("swap %v: prepay accepted (%v)", lo.hash, u.AmtPaid)
				prepayAccepted = true
			}
		case err := <-swapErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "swap invoice subscription: %v", err)
			return
		case err := <-prepayErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "prepay invoice subscription: %v", err)
			return
		case h := <-blocks:
			if h >= lo.expiry-int32(s.cfg.minCltvDelta)/2 {
				s.cancelLoopOutInvoices(lo)
				fail(swapserverrpc.ServerSwapState_SERVER_FAILED_OFF_CHAIN_TIMEOUT, "payments not received in time")
				return
			}
		case err := <-blockErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "block epochs: %v", err)
			return
		case <-lo.cancelCh:
			return
		case <-ctx.Done():
			return
		}
	}

	// We publish the htlc right away: we ignore the publication deadline, there's nothing to batch on regtest.
	lo.mu.Lock()
	cancelled := lo.cancelled
	lo.mu.Unlock()
	if cancelled {
		return
	}
	heightHint := s.currentHeight()
	tx, outpoint, err := s.fundHtlc(ctx, lo.htlc, lo.amt, "loop out htlc "+lo.hash.String()[:8])
	if err != nil {
		s.cancelLoopOutInvoices(lo)
		fail(swapserverrpc.ServerSwapState_SERVER_FAILED_HTLC_PUBLICATION, "cannot publish htlc: %v", err)
		return
	}
	lo.mu.Lock()
	lo.outpoint = &outpoint
	lo.mu.Unlock()
	log.Infof("swap %v: published htlc %v", lo.hash, outpoint)
	lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_HTLC_PUBLISHED)
	if err := s.lnd.Invoices.SettleInvoice(ctx, lo.prepayPreimg); err != nil {
		log.Errorf("swap %v: cannot settle prepay: %v", lo.hash, err)
	}

	txid := tx.TxHash()
	confs, confErrs, err := s.lnd.ChainNotifier.RegisterConfirmationsNtfn(ctx, &txid, lo.htlc.PkScript, 1, heightHint)
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "register conf: %v", err)
		return
	}
	spends, spendErrs, err := s.lnd.ChainNotifier.RegisterSpendNtfn(ctx, &outpoint, lo.htlc.PkScript, heightHint)
	if err != nil {
		fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "register spend: %v", err)
		return
	}
	timeoutPublished := false
	for {
		select {
		case <-confs:
			confs = nil
			log.Infof("swap %v: htlc confirmed", lo.hash)
			lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_HTLC_CONFIRMED)
		case err := <-confErrs:
			log.Errorf("swap %v: conf error: %v", lo.hash, err)
			confErrs = nil
		case spend := <-spends:
			witness := spend.SpendingTx.TxIn[spend.SpenderInputIndex].Witness
			switch {
			case len(witness) == 1:
				// Key path: we only co-sign once we know the preimage, the swap invoice is already settled.
				log.Infof("swap %v: htlc claimed by client (key path) in %v", lo.hash, spend.SpenderTxHash)
				lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_SUCCESS)
			case lo.htlc.IsSuccessWitness(witness):
				preimage, err := lntypes.MakePreimage(witness[0])
				if err == nil && lo.getPreimage() == nil {
					if err := s.settleLoopOut(ctx, lo, preimage); err != nil {
						log.Errorf("swap %v: cannot settle swap invoice: %v", lo.hash, err)
					}
				}
				log.Infof("swap %v: htlc claimed by client (script path) in %v", lo.hash, spend.SpenderTxHash)
				lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_SUCCESS)
			default:
				if lo.getPreimage() == nil {
					if err := s.lnd.Invoices.CancelInvoice(ctx, lo.hash); err != nil {
						log.Warnf("swap %v: cannot cancel swap invoice: %v", lo.hash, err)
					}
				}
				log.Infof("swap %v: htlc timed out, swept in %v", lo.hash, spend.SpenderTxHash)
				lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_FAILED_TIMEOUT)
			}
			return
		case err := <-spendErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "spend notification: %v", err)
			return
		case h := <-blocks:
			if h >= lo.expiry && !timeoutPublished && lo.getPreimage() == nil {
				htlcScript := lo.htlc.HtlcScript.(*swap.HtlcScriptV3)
				sweep, err := s.sweepScriptPath(ctx, "loop out timeout "+lo.hash.String()[:8], lo.htlc, outpoint, lo.amt,
					htlcScript.TimeoutScript(), lo.senderKey, wire.MaxTxInSequenceNum-1, uint32(lo.expiry), htlcScript.GenTimeoutWitness)
				if err != nil {
					log.Errorf("swap %v: cannot publish timeout sweep: %v", lo.hash, err)
					continue
				}
				timeoutPublished = true
				log.Infof("swap %v: published timeout sweep %v", lo.hash, sweep.TxHash())
				lo.status.set(lo.hash, swapserverrpc.ServerSwapState_SERVER_TIMEOUT_PUBLISHED)
			}
		case err := <-blockErrs:
			fail(swapserverrpc.ServerSwapState_SERVER_UNEXPECTED_FAILURE, "block epochs: %v", err)
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *server) settleLoopOut(ctx context.Context, lo *loopOut, preimage lntypes.Preimage) error {
	if err := s.lnd.Invoices.SettleInvoice(ctx, preimage); err != nil {
		return err
	}
	lo.mu.Lock()
	lo.preimage = &preimage
	lo.mu.Unlock()
	log.Infof("swap %v: swap invoice settled", lo.hash)
	return nil
}

func (s *server) getLoopOut(hashBytes []byte, paymentAddr []byte) (*loopOut, error) {
	hash, err := lntypes.MakeHash(hashBytes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.mu.Lock()
	lo, ok := s.loopOuts[hash]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown swap %v", hash)
	}
	if paymentAddr != nil && !bytes.Equal(paymentAddr, lo.paymentAddr[:]) {
		return nil, status.Error(codes.PermissionDenied, "invalid payment address")
	}
	return lo, nil
}

func (s *server) LoopOutPushPreimage(ctx context.Context, req *swapserverrpc.ServerLoopOutPushPreimageRequest) (*swapserverrpc.ServerLoopOutPushPreimageResponse, error) {
	preimage, err := lntypes.MakePreimage(req.Preimage)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	hash := hashOf(preimage)
	log.Infof("LoopOutPushPreimage hash=%v", hash)
	lo, err := s.getLoopOut(hash[:], nil)
	if err != nil {
		return nil, err
	}
	if lo.getOutpoint() == nil {
		return nil, status.Error(codes.FailedPrecondition, "htlc not published yet")
	}
	if lo.getPreimage() == nil {
		if err := s.settleLoopOut(ctx, lo, preimage); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot settle invoice: %v", err)
		}
	}
	return &swapserverrpc.ServerLoopOutPushPreimageResponse{}, nil
}

func (s *server) MuSig2SignSweep(_ context.Context, req *swapserverrpc.MuSig2SignSweepReq) (*swapserverrpc.MuSig2SignSweepRes, error) {
	log.Infof("MuSig2SignSweep hash=%x", req.SwapHash)
	if err := s.checkProtocol(req.ProtocolVersion); err != nil {
		return nil, err
	}
	lo, err := s.getLoopOut(req.SwapHash, req.PaymentAddress)
	if err != nil {
		return nil, err
	}
	outpoint := lo.getOutpoint()
	if outpoint == nil {
		return nil, status.Error(codes.FailedPrecondition, "htlc not published yet")
	}
	if s.cfg.noCoopSweep {
		return nil, status.Error(codes.Unavailable, "cooperative sweeps are disabled")
	}
	// Co-signing the key path is equivalent to giving the funds away: we must have been paid first.
	if lo.getPreimage() == nil {
		return nil, status.Error(codes.FailedPrecondition, "preimage not received yet")
	}
	senderKey := pubKey33(lo.senderKey.PubKey())
	nonce, sig, err := muSig2PartialSign(lo.htlc, *outpoint, senderKey, lo.receiverKey, lo.senderKey, req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot sign sweep: %v", err)
	}
	return &swapserverrpc.MuSig2SignSweepRes{Nonce: nonce, PartialSignature: sig}, nil
}

func (s *server) CancelLoopOutSwap(_ context.Context, req *swapserverrpc.CancelLoopOutSwapRequest) (*swapserverrpc.CancelLoopOutSwapResponse, error) {
	if rc := req.GetRouteCancel(); rc != nil {
		hops := make([]uint32, 0, len(rc.Attempts))
		for _, a := range rc.Attempts {
			hops = append(hops, a.RemainingHops)
		}
		log.Infof("CancelLoopOutSwap hash=%x route=%v failure=%v remaining_hops=%v", req.SwapHash, rc.RouteType, rc.Failure, hops)
	} else {
		log.Infof("CancelLoopOutSwap hash=%x", req.SwapHash)
	}
	lo, err := s.getLoopOut(req.SwapHash, req.PaymentAddress)
	if err != nil {
		return nil, err
	}
	lo.mu.Lock()
	if lo.outpoint != nil {
		lo.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "htlc already published")
	}
	if !lo.cancelled {
		lo.cancelled = true
		close(lo.cancelCh)
	}
	lo.mu.Unlock()
	s.cancelLoopOutInvoices(lo)
	state := swapserverrpc.ServerSwapState_SERVER_CLIENT_INVOICE_CANCEL
	if rc := req.GetRouteCancel(); rc != nil && rc.RouteType == swapserverrpc.RoutePaymentType_PREPAY_ROUTE {
		state = swapserverrpc.ServerSwapState_SERVER_CLIENT_PREPAY_CANCEL
	}
	lo.status.set(lo.hash, state)
	return &swapserverrpc.CancelLoopOutSwapResponse{}, nil
}
