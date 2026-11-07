// Command regtestloopserver is a minimal Lightning Loop swap server for regtest, used to test loop clients (loopd,
// eclair-loop) now that the official regtest loopserver image isn't published anymore.
//
// It implements loop out and loop in with v3 (taproot) htlcs and MuSig2 cooperative sweeps, reusing loop's own swap
// package for scripts and lnd's MuSig2 implementation, and drives an lnd node through lndclient. Swaps are only kept
// in memory. L402 is handled by aperture in front of this server.
//
// This is a test tool: it does not try to be safe or efficient, and it only enforces what clients rely on.
package main

import (
	"context"
	"flag"
	"fmt"
	stdlog "log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lightninglabs/lndclient"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightningnetwork/lnd/cert"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type config struct {
	listen        string
	tlsDir        string
	noTLS         bool
	lndHost       string
	lndDir        string
	network       string
	minSwapAmt    int64
	maxSwapAmt    int64
	minCltvDelta  int
	maxCltvDelta  int
	loopInDelta   int
	swapFeeBase   int64
	swapFeePPM    int64
	prepayAmt     int64
	feeRate       chainfee.SatPerKWeight
	loopInConfs   int
	strictProbe   bool
	paymentTimout time.Duration
	noCoopSweep   bool
	cltvExtra     int
}

// logger is a minimal leveled logger on top of the standard library.
type logger struct{}

func (logger) Debugf(format string, args ...any) { stdlog.Printf("[DBG] "+format, args...) }
func (logger) Infof(format string, args ...any)  { stdlog.Printf("[INF] "+format, args...) }
func (logger) Warnf(format string, args ...any)  { stdlog.Printf("[WRN] "+format, args...) }
func (logger) Errorf(format string, args ...any) { stdlog.Printf("[ERR] "+format, args...) }

var log logger

func main() {
	cfg := config{}
	var feeRateSatVb int64
	flag.StringVar(&cfg.listen, "listen", "0.0.0.0:11009", "grpc listen address")
	flag.StringVar(&cfg.tlsDir, "tlsdir", "/root/loopserver", "directory where tls.cert/tls.key are created")
	flag.BoolVar(&cfg.noTLS, "notls", false, "serve plaintext grpc")
	flag.StringVar(&cfg.lndHost, "lnd.host", "lndserver:10009", "lnd grpc address")
	flag.StringVar(&cfg.lndDir, "lnd.dir", "/root/.lnd", "lnd directory (tls.cert and macaroons)")
	flag.StringVar(&cfg.network, "network", "regtest", "bitcoin network")
	flag.Int64Var(&cfg.minSwapAmt, "minamt", 250_000, "min swap amount (sat)")
	flag.Int64Var(&cfg.maxSwapAmt, "maxamt", 5_000_000, "max swap amount (sat)")
	flag.IntVar(&cfg.minCltvDelta, "mincltvdelta", 100, "min loop out htlc cltv delta")
	flag.IntVar(&cfg.maxCltvDelta, "maxcltvdelta", 400, "max loop out htlc cltv delta")
	flag.IntVar(&cfg.loopInDelta, "loopincltvdelta", 1000, "loop in htlc cltv delta")
	flag.Int64Var(&cfg.swapFeeBase, "swapfeebase", 50, "swap fee base (sat)")
	flag.Int64Var(&cfg.swapFeePPM, "swapfeeppm", 1000, "swap fee rate (ppm)")
	flag.Int64Var(&cfg.prepayAmt, "prepayamt", 30_000, "loop out prepay amount (sat)")
	flag.Int64Var(&feeRateSatVb, "feerate", 10, "on-chain fee rate (sat/vbyte)")
	flag.IntVar(&cfg.loopInConfs, "loopinconfs", 1, "confirmations required on loop in htlcs")
	flag.BoolVar(&cfg.strictProbe, "strictprobe", false, "fail loop in initiation when the probe payment doesn't fail with incorrect_or_unknown_payment_details")
	flag.IntVar(&cfg.cltvExtra, "loopoutcltvextra", 30, "loop out swap invoices' final cltv delta is the htlc delta plus this (a large value makes them unpayable by clients)")
	flag.BoolVar(&cfg.noCoopSweep, "nocoopsweep", false, "refuse to co-sign loop out sweeps (clients must use the script path)")
	flag.DurationVar(&cfg.paymentTimout, "paymenttimeout", time.Minute, "timeout of off-chain payments")
	flag.Parse()
	cfg.feeRate = chainfee.SatPerKVByte(feeRateSatVb * 1000).FeePerKWeight()

	stdlog.SetOutput(os.Stdout)
	stdlog.SetFlags(stdlog.LstdFlags | stdlog.Lmicroseconds)

	if err := run(cfg); err != nil {
		log.Errorf("fatal: %v", err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The tls cert is created first: aperture needs it to start.
	var opts []grpc.ServerOption
	if !cfg.noTLS {
		creds, err := loadOrCreateTLS(cfg.tlsDir)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// lnd may still be starting up (or waiting for blocks), retry until all its sub-servers answer.
	var lnd *lndclient.GrpcLndServices
	var srv *server
	for {
		var err error
		lnd, err = lndclient.NewLndServices(&lndclient.LndServicesConfig{
			LndAddress:  cfg.lndHost,
			Network:     lndclient.Network(cfg.network),
			MacaroonDir: filepath.Join(cfg.lndDir, "data", "chain", "bitcoin", cfg.network),
			TLSPath:     filepath.Join(cfg.lndDir, "tls.cert"),
			CallerCtx:   ctx,
		})
		if err == nil {
			srv, err = newServer(ctx, cfg, &lnd.LndServices)
			if err == nil {
				break
			}
			lnd.Close()
		}
		log.Infof("waiting for lnd: %v", err)
		time.Sleep(5 * time.Second)
	}
	defer lnd.Close()
	log.Infof("connected to lnd %x", lnd.NodePubkey[:])

	grpcServer := grpc.NewServer(opts...)
	swapserverrpc.RegisterSwapServerServer(grpcServer, srv)

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return err
	}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		grpcServer.GracefulStop()
	}()
	log.Infof("swap server listening on %v (tls=%v)", cfg.listen, !cfg.noTLS)
	return grpcServer.Serve(lis)
}

func loadOrCreateTLS(dir string) (credentials.TransportCredentials, error) {
	certFile := filepath.Join(dir, "tls.cert")
	keyFile := filepath.Join(dir, "tls.key")
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		certBytes, keyBytes, err := cert.GenCertPair(
			"regtest loopserver", []string{"loopserver", "localhost"}, nil, false, 14*30*24*time.Hour,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to generate cert pair: %w", err)
		}
		if err := cert.WriteCertPair(certFile, keyFile, certBytes, keyBytes); err != nil {
			return nil, err
		}
		_ = os.Chmod(certFile, 0644)
	}
	return credentials.NewServerTLSFromFile(certFile, keyFile)
}
