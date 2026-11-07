package main

import (
	"bytes"
	"context"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr/musig2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/psbt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightninglabs/loop/swap"
	"github.com/lightninglabs/loop/swapserverrpc"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/lntypes"
)

// musig2Version is the MuSig2 variant used by loop for protocol version >= MUSIG2.
const musig2Version = input.MuSig2Version100RC2

func pubKey33(k *btcec.PublicKey) [33]byte {
	var b [33]byte
	copy(b[:], k.SerializeCompressed())
	return b
}

// sweepWeight is a generous estimate of the weight of a 1-input 1-output taproot script-path sweep.
const sweepWeight = lntypes.WeightUnit(800)

// sweepScriptPath spends an htlc output through one of its tapscript leaves to a fresh wallet address.
//
// witnessFn builds the witness from our signature.
func (s *server) sweepScriptPath(ctx context.Context, label string, htlc *swap.Htlc, outpoint wire.OutPoint, value btcutil.Amount,
	leafScript []byte, key *btcec.PrivateKey, sequence uint32, lockTime uint32,
	witnessFn func(sig []byte) (wire.TxWitness, error)) (*wire.MsgTx, error) {

	addr, err := s.lnd.WalletKit.NextAddr(ctx, "", 0, false)
	if err != nil {
		return nil, fmt.Errorf("next addr: %w", err)
	}
	pkScript, err := txscript.PayToAddrScript(addr)
	if err != nil {
		return nil, err
	}
	fee := s.cfg.feeRate.FeeForWeight(sweepWeight)
	if value-fee <= 0 {
		return nil, fmt.Errorf("htlc value %v too low to pay fee %v", value, fee)
	}

	tx := wire.NewMsgTx(2)
	tx.LockTime = lockTime
	tx.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint, Sequence: sequence})
	tx.AddTxOut(&wire.TxOut{Value: int64(value - fee), PkScript: pkScript})

	prevFetcher := txscript.NewCannedPrevOutputFetcher(htlc.PkScript, int64(value))
	sigHashes := txscript.NewTxSigHashes(tx, prevFetcher)
	sig, err := txscript.RawTxInTapscriptSignature(
		tx, sigHashes, 0, int64(value), htlc.PkScript, txscript.NewBaseTapLeaf(leafScript), txscript.SigHashDefault, key,
	)
	if err != nil {
		return nil, fmt.Errorf("sign sweep: %w", err)
	}
	witness, err := witnessFn(sig)
	if err != nil {
		return nil, err
	}
	tx.TxIn[0].Witness = witness

	if err := s.lnd.WalletKit.PublishTransaction(ctx, tx, label); err != nil {
		return nil, fmt.Errorf("publish %v: %w", label, err)
	}
	return tx, nil
}

// muSig2PartialSign co-signs the key-path spend of our htlc in a (possibly batched) sweep transaction built by the
// client, like loop's server does for MuSig2SignSweep. Signers are always ordered sender, receiver (keys are sorted
// anyway) and the aggregate key is tweaked with the htlc script tree root.
func muSig2PartialSign(htlc *swap.Htlc, outpoint wire.OutPoint, senderInternal, receiverInternal [33]byte, ourKey *btcec.PrivateKey,
	req *swapserverrpc.MuSig2SignSweepReq) ([]byte, []byte, error) {

	htlcScript, ok := htlc.HtlcScript.(*swap.HtlcScriptV3)
	if !ok {
		return nil, nil, fmt.Errorf("not a v3 htlc")
	}

	packet, err := psbt.NewFromRawBytes(bytes.NewReader(req.SweepTxPsbt), false)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid psbt: %w", err)
	}
	tx := packet.UnsignedTx

	prevOuts := make(map[wire.OutPoint]*wire.TxOut)
	for _, p := range req.PrevoutInfo {
		hash, err := chainhash.NewHash(p.TxidBytes)
		if err != nil {
			return nil, nil, err
		}
		prevOuts[wire.OutPoint{Hash: *hash, Index: p.OutputIndex}] = wire.NewTxOut(int64(p.Value), p.PkScript)
	}
	inputIndex := -1
	for i, in := range tx.TxIn {
		if _, ok := prevOuts[in.PreviousOutPoint]; !ok {
			return nil, nil, fmt.Errorf("missing prevout info for input %v", in.PreviousOutPoint)
		}
		// Like loop's server, we require the psbt to include the spent outputs (loopd always sets them).
		witnessUtxo := packet.Inputs[i].WitnessUtxo
		if witnessUtxo == nil || witnessUtxo.Value != prevOuts[in.PreviousOutPoint].Value ||
			!bytes.Equal(witnessUtxo.PkScript, prevOuts[in.PreviousOutPoint].PkScript) {

			return nil, nil, fmt.Errorf("invalid sweep: missing or invalid witness utxo for input %v", in.PreviousOutPoint)
		}
		if in.PreviousOutPoint == outpoint {
			inputIndex = i
		}
	}
	if inputIndex < 0 {
		return nil, nil, fmt.Errorf("sweep doesn't spend htlc %v", outpoint)
	}
	if !bytes.Equal(prevOuts[outpoint].PkScript, htlc.PkScript) {
		return nil, nil, fmt.Errorf("invalid htlc prevout script")
	}

	prevFetcher := txscript.NewMultiPrevOutFetcher(prevOuts)
	sigHash, err := txscript.CalcTaprootSignatureHash(
		txscript.NewTxSigHashes(tx, prevFetcher), txscript.SigHashDefault, tx, inputIndex, prevFetcher,
	)
	if err != nil {
		return nil, nil, err
	}
	var digest [32]byte
	copy(digest[:], sigHash)

	signers, err := input.MuSig2ParsePubKeys(musig2Version, [][]byte{senderInternal[:], receiverInternal[:]})
	if err != nil {
		return nil, nil, err
	}
	_, session, err := input.MuSig2CreateContext(
		musig2Version, ourKey, signers, &input.MuSig2Tweaks{TaprootTweak: htlcScript.RootHash[:]}, nil,
	)
	if err != nil {
		return nil, nil, err
	}
	if len(req.Nonce) != musig2.PubNonceSize {
		return nil, nil, fmt.Errorf("invalid nonce length %v", len(req.Nonce))
	}
	var clientNonce [musig2.PubNonceSize]byte
	copy(clientNonce[:], req.Nonce)
	if _, err := session.RegisterPubNonce(clientNonce); err != nil {
		return nil, nil, err
	}
	// The session forgets its nonces once it has signed.
	nonce := session.PublicNonce()
	partialSig, err := input.MuSig2Sign(session, digest, true)
	if err != nil {
		return nil, nil, err
	}
	sigBytes, err := input.SerializePartialSignature(partialSig)
	if err != nil {
		return nil, nil, err
	}
	return nonce[:], sigBytes[:], nil
}

// fundHtlc publishes a transaction paying value to the htlc from lnd's wallet.
func (s *server) fundHtlc(ctx context.Context, htlc *swap.Htlc, value btcutil.Amount, label string) (*wire.MsgTx, wire.OutPoint, error) {
	tx, err := s.lnd.WalletKit.SendOutputs(ctx, []*wire.TxOut{wire.NewTxOut(int64(value), htlc.PkScript)}, s.cfg.feeRate, label)
	if err != nil {
		return nil, wire.OutPoint{}, err
	}
	for i, out := range tx.TxOut {
		if bytes.Equal(out.PkScript, htlc.PkScript) {
			return tx, wire.OutPoint{Hash: tx.TxHash(), Index: uint32(i)}, nil
		}
	}
	return nil, wire.OutPoint{}, fmt.Errorf("htlc output not found in %v", tx.TxHash())
}

// keyPathSweepWeight is a generous estimate of the weight of a 1-input 1-output taproot key-path sweep.
const keyPathSweepWeight = lntypes.WeightUnit(500)

// sweepKeyPath spends an htlc output through its MuSig2 key path, when we hold both internal private keys (the client
// gave us its key after we paid its loop in invoice).
func (s *server) sweepKeyPath(ctx context.Context, label string, htlc *swap.Htlc, outpoint wire.OutPoint, value btcutil.Amount,
	senderInternal, receiverInternal [33]byte, keys []*btcec.PrivateKey) (*wire.MsgTx, error) {

	htlcScript, ok := htlc.HtlcScript.(*swap.HtlcScriptV3)
	if !ok {
		return nil, fmt.Errorf("not a v3 htlc")
	}
	addr, err := s.lnd.WalletKit.NextAddr(ctx, "", 0, false)
	if err != nil {
		return nil, fmt.Errorf("next addr: %w", err)
	}
	pkScript, err := txscript.PayToAddrScript(addr)
	if err != nil {
		return nil, err
	}
	fee := s.cfg.feeRate.FeeForWeight(keyPathSweepWeight)
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(&wire.TxIn{PreviousOutPoint: outpoint, Sequence: wire.MaxTxInSequenceNum - 2})
	tx.AddTxOut(&wire.TxOut{Value: int64(value - fee), PkScript: pkScript})

	prevFetcher := txscript.NewCannedPrevOutputFetcher(htlc.PkScript, int64(value))
	sigHash, err := txscript.CalcTaprootSignatureHash(txscript.NewTxSigHashes(tx, prevFetcher), txscript.SigHashDefault, tx, 0, prevFetcher)
	if err != nil {
		return nil, err
	}
	var digest [32]byte
	copy(digest[:], sigHash)

	signers, err := input.MuSig2ParsePubKeys(musig2Version, [][]byte{senderInternal[:], receiverInternal[:]})
	if err != nil {
		return nil, err
	}
	sessions := make([]input.MuSig2Session, len(keys))
	for i, k := range keys {
		if _, sessions[i], err = input.MuSig2CreateContext(musig2Version, k, signers, &input.MuSig2Tweaks{TaprootTweak: htlcScript.RootHash[:]}, nil); err != nil {
			return nil, err
		}
	}
	nonces := [][musig2.PubNonceSize]byte{sessions[0].PublicNonce(), sessions[1].PublicNonce()}
	if _, err := sessions[0].RegisterPubNonce(nonces[1]); err != nil {
		return nil, err
	}
	if _, err := sessions[1].RegisterPubNonce(nonces[0]); err != nil {
		return nil, err
	}
	partialSigs := make([]*musig2.PartialSignature, len(sessions))
	for i, session := range sessions {
		if partialSigs[i], err = input.MuSig2Sign(session, digest, true); err != nil {
			return nil, err
		}
	}
	if _, err := input.MuSig2CombineSig(sessions[0], partialSigs[1]); err != nil {
		return nil, err
	}
	tx.TxIn[0].Witness = wire.TxWitness{sessions[0].FinalSig().Serialize()}

	if err := s.lnd.WalletKit.PublishTransaction(ctx, tx, label); err != nil {
		return nil, fmt.Errorf("publish %v: %w", label, err)
	}
	return tx, nil
}
