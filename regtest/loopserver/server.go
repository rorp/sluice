package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/swap"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// swapStatus is the part of a swap shared by loop out and loop in: its state and the clients subscribed to it.
type swapStatus struct {
	mu        sync.Mutex
	state     swapserverrpc.ServerSwapState
	listeners []chan swapserverrpc.ServerSwapState
}

func isFinal(state swapserverrpc.ServerSwapState) bool {
	switch state {
	case swapserverrpc.ServerSwapState_SERVER_INITIATED,
		swapserverrpc.ServerSwapState_SERVER_HTLC_PUBLISHED,
		swapserverrpc.ServerSwapState_SERVER_HTLC_CONFIRMED,
		swapserverrpc.ServerSwapState_SERVER_TIMEOUT_PUBLISHED:
		return false
	}
	return true
}

func (s *swapStatus) get() swapserverrpc.ServerSwapState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *swapStatus) set(hash lntypes.Hash, state swapserverrpc.ServerSwapState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == state {
		return
	}
	log.Infof("swap %v: %v -> %v", hash, s.state, state)
	s.state = state
	for _, l := range s.listeners {
		select {
		case l <- state:
		default:
		}
	}
}

func (s *swapStatus) subscribe() (swapserverrpc.ServerSwapState, chan swapserverrpc.ServerSwapState, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan swapserverrpc.ServerSwapState, 32)
	s.listeners = append(s.listeners, ch)
	return s.state, ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, l := range s.listeners {
			if l == ch {
				s.listeners = append(s.listeners[:i], s.listeners[i+1:]...)
				break
			}
		}
	}
}

type server struct {
	swapserverrpc.UnimplementedSwapServerServer

	ctx         context.Context
	cfg         config
	lnd         *lndclient.LndServices
	chainParams *chaincfg.Params

	mu       sync.Mutex
	height   int32
	loopOuts map[lntypes.Hash]*loopOut
	loopIns  map[lntypes.Hash]*loopIn
}

func newServer(ctx context.Context, cfg config, lnd *lndclient.LndServices) (*server, error) {
	s := &server{
		ctx:         ctx,
		cfg:         cfg,
		lnd:         lnd,
		chainParams: lnd.ChainParams,
		loopOuts:    make(map[lntypes.Hash]*loopOut),
		loopIns:     make(map[lntypes.Hash]*loopIn),
	}
	blocks, errs, err := lnd.ChainNotifier.RegisterBlockEpochNtfn(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case h := <-blocks:
		s.height = h
	case err := <-errs:
		return nil, err
	}
	go func() {
		for {
			select {
			case h := <-blocks:
				s.mu.Lock()
				s.height = h
				s.mu.Unlock()
			case err := <-errs:
				log.Errorf("block epoch error: %v", err)
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return s, nil
}

func (s *server) currentHeight() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.height
}

func (s *server) swapFee(amt btcutil.Amount) btcutil.Amount {
	return btcutil.Amount(s.cfg.swapFeeBase) + amt*btcutil.Amount(s.cfg.swapFeePPM)/1_000_000
}

func (s *server) checkProtocol(version swapserverrpc.ProtocolVersion) error {
	if version < swapserverrpc.ProtocolVersion_MUSIG2 {
		return status.Errorf(codes.InvalidArgument, "protocol version %v not supported, this server only supports MUSIG2 (taproot htlcs)", version)
	}
	return nil
}

func (s *server) checkAmount(amt btcutil.Amount) error {
	if amt < btcutil.Amount(s.cfg.minSwapAmt) {
		return status.Errorf(codes.InvalidArgument, "swap amount too low, min %v", s.cfg.minSwapAmt)
	}
	if amt > btcutil.Amount(s.cfg.maxSwapAmt) {
		return status.Errorf(codes.InvalidArgument, "swap amount too high, max %v", s.cfg.maxSwapAmt)
	}
	return nil
}

func (s *server) LoopOutTerms(_ context.Context, req *swapserverrpc.ServerLoopOutTermsRequest) (*swapserverrpc.ServerLoopOutTerms, error) {
	log.Infof("LoopOutTerms user_agent=%q protocol=%v", req.UserAgent, req.ProtocolVersion)
	return &swapserverrpc.ServerLoopOutTerms{
		MinSwapAmount: uint64(s.cfg.minSwapAmt),
		MaxSwapAmount: uint64(s.cfg.maxSwapAmt),
		MinCltvDelta:  int32(s.cfg.minCltvDelta),
		MaxCltvDelta:  int32(s.cfg.maxCltvDelta),
	}, nil
}

func (s *server) LoopOutQuote(_ context.Context, req *swapserverrpc.ServerLoopOutQuoteRequest) (*swapserverrpc.ServerLoopOutQuote, error) {
	log.Infof("LoopOutQuote amt=%v expiry=%v user_agent=%q", req.Amt, req.Expiry, req.UserAgent)
	if err := s.checkLoopOutExpiry(req.Expiry); err != nil {
		return nil, err
	}
	amt := btcutil.Amount(req.Amt)
	if amt == 0 {
		amt = btcutil.Amount(s.cfg.maxSwapAmt)
	}
	return &swapserverrpc.ServerLoopOutQuote{
		SwapPaymentDest: fmt.Sprintf("%x", s.lnd.NodePubkey[:]),
		SwapFee:         int64(s.swapFee(amt)),
		PrepayAmt:       uint64(s.cfg.prepayAmt),
	}, nil
}

func (s *server) checkLoopOutExpiry(expiry int32) error {
	delta := expiry - s.currentHeight()
	if delta < int32(s.cfg.minCltvDelta) {
		return status.Errorf(codes.InvalidArgument, "cltv delta below minimum: %v", delta)
	}
	if delta > int32(s.cfg.maxCltvDelta) {
		return status.Errorf(codes.InvalidArgument, "cltv delta above maximum: %v", delta)
	}
	return nil
}

func (s *server) LoopInTerms(_ context.Context, req *swapserverrpc.ServerLoopInTermsRequest) (*swapserverrpc.ServerLoopInTerms, error) {
	log.Infof("LoopInTerms user_agent=%q protocol=%v", req.UserAgent, req.ProtocolVersion)
	return &swapserverrpc.ServerLoopInTerms{
		MinSwapAmount: uint64(s.cfg.minSwapAmt),
		MaxSwapAmount: uint64(s.cfg.maxSwapAmt),
	}, nil
}

func (s *server) LoopInQuote(_ context.Context, req *swapserverrpc.ServerLoopInQuoteRequest) (*swapserverrpc.ServerLoopInQuoteResponse, error) {
	log.Infof("LoopInQuote amt=%v pubkey=%x user_agent=%q", req.Amt, req.Pubkey, req.UserAgent)
	amt := btcutil.Amount(req.Amt)
	if amt == 0 {
		amt = btcutil.Amount(s.cfg.maxSwapAmt)
	}
	return &swapserverrpc.ServerLoopInQuoteResponse{
		SwapFee:   int64(s.swapFee(amt)),
		CltvDelta: int32(s.cfg.loopInDelta),
	}, nil
}

// Probe is used by clients to let us check that we can route to them before quoting a loop in: we don't need it.
func (s *server) Probe(_ context.Context, req *swapserverrpc.ServerProbeRequest) (*swapserverrpc.ServerProbeResponse, error) {
	log.Infof("Probe amt=%v target=%x", req.Amt, req.Target)
	return &swapserverrpc.ServerProbeResponse{}, nil
}

func (s *server) RecommendRoutingPlugin(context.Context, *swapserverrpc.RecommendRoutingPluginReq) (*swapserverrpc.RecommendRoutingPluginRes, error) {
	return &swapserverrpc.RecommendRoutingPluginRes{Plugin: swapserverrpc.RoutingPlugin_NONE}, nil
}

func (s *server) ReportRoutingResult(context.Context, *swapserverrpc.ReportRoutingResultReq) (*swapserverrpc.ReportRoutingResultRes, error) {
	return &swapserverrpc.ReportRoutingResultRes{}, nil
}

// FetchL402 only exists so that aperture (which is in front of us) asks for an L402 token.
func (s *server) FetchL402(context.Context, *swapserverrpc.FetchL402Request) (*swapserverrpc.FetchL402Response, error) {
	return &swapserverrpc.FetchL402Response{}, nil
}

// SubscribeNotifications is used by loopd for reservations and static addresses, which we don't support: we keep the
// stream open without sending anything.
func (s *server) SubscribeNotifications(_ *swapserverrpc.SubscribeNotificationsRequest, stream swapserverrpc.SwapServer_SubscribeNotificationsServer) error {
	select {
	case <-stream.Context().Done():
	case <-s.ctx.Done():
	}
	return nil
}

type updateSender func(state swapserverrpc.ServerSwapState) error

func (s *server) streamUpdates(ctx context.Context, st *swapStatus, send updateSender) error {
	state, ch, unsubscribe := st.subscribe()
	defer unsubscribe()
	if err := send(state); err != nil {
		return err
	}
	for !isFinal(state) {
		select {
		case state = <-ch:
			if err := send(state); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		case <-s.ctx.Done():
			return nil
		}
	}
	return nil
}

func (s *server) SubscribeLoopOutUpdates(req *swapserverrpc.SubscribeUpdatesRequest, stream swapserverrpc.SwapServer_SubscribeLoopOutUpdatesServer) error {
	hash, err := lntypes.MakeHash(req.SwapHash)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	s.mu.Lock()
	swp, ok := s.loopOuts[hash]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "unknown swap %v", hash)
	}
	return s.streamUpdates(stream.Context(), &swp.status, func(state swapserverrpc.ServerSwapState) error {
		return stream.Send(&swapserverrpc.SubscribeLoopOutUpdatesResponse{TimestampNs: time.Now().UnixNano(), State: state})
	})
}

func (s *server) SubscribeLoopInUpdates(req *swapserverrpc.SubscribeUpdatesRequest, stream swapserverrpc.SwapServer_SubscribeLoopInUpdatesServer) error {
	hash, err := lntypes.MakeHash(req.SwapHash)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	s.mu.Lock()
	swp, ok := s.loopIns[hash]
	s.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "unknown swap %v", hash)
	}
	return s.streamUpdates(stream.Context(), &swp.status, func(state swapserverrpc.ServerSwapState) error {
		return stream.Send(&swapserverrpc.SubscribeLoopInUpdatesResponse{TimestampNs: time.Now().UnixNano(), State: state})
	})
}

func hashOf(preimage lntypes.Preimage) lntypes.Hash {
	return lntypes.Hash(sha256.Sum256(preimage[:]))
}

// htlcV3 builds the swap htlc exactly like loop clients do (utils.GetHtlc with protocol version >= MUSIG2).
func (s *server) htlcV3(cltvExpiry int32, senderInternal, receiverInternal, sender, receiver [33]byte, hash lntypes.Hash) (*swap.Htlc, error) {
	return swap.NewHtlcV3(musig2Version, cltvExpiry, senderInternal, receiverInternal, sender, receiver, hash, s.chainParams)
}
