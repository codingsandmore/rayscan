package onchain

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/codingsandmore/rayscan/connection"
	"github.com/codingsandmore/rayscan/onchain/raydium"
	"github.com/codingsandmore/rayscan/onchain/serum"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	log "github.com/sirupsen/logrus"
)

var Max_Transaction_Version uint64 = 1
var Rewards bool = false

type TxCandidate struct {
	Signature  solana.Signature
	clientName string
	Metadata   *json.RawMessage
}

type TxAnalyzer struct {
	rpcPool *connection.RPCPool

	txCandidateC chan TxCandidate
	doneC        chan struct{}
	infoPublishC chan<- Info

	gotCandidates map[solana.Signature]struct{}
}

func NewTxAnalyzer(rpcPool *connection.RPCPool) *TxAnalyzer {
	return &TxAnalyzer{
		rpcPool:       rpcPool,
		txCandidateC:  make(chan TxCandidate, 32),
		doneC:         make(chan struct{}),
		gotCandidates: make(map[solana.Signature]struct{}),
	}
}

func (a *TxAnalyzer) Channel() chan<- TxCandidate {
	return a.txCandidateC
}

func (a *TxAnalyzer) Start(infoPublishC chan<- Info) {
	log.Debugf("TxAnalyzer: starting...")

	go func() {
		defer close(a.doneC)

		for txCandidate := range a.txCandidateC {
			if _, ok := a.gotCandidates[txCandidate.Signature]; ok {
				continue
			}

			a.gotCandidates[txCandidate.Signature] = struct{}{}

			go a.analyze(txCandidate, infoPublishC)
		}
	}()
}

func (a *TxAnalyzer) analyze(txCandidate TxCandidate, infoPublishC chan<- Info) {
	// Send rpc for full tx details
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	log.Debugf("TxAnalyzer: analyzing tx candidate: %s", txCandidate.Signature.String())
	begin := time.Now()
	rpcTx, tx, err := a.getConfirmedTransaction(ctx, txCandidate)
	if err != nil {
		log.Infof("TxAnalyzer: error getting transaction (tx: %s): %s", txCandidate.Signature, err)
		return
	} else {
		log.Debugf("received new tx: %s", txCandidate.Signature.String())
	}
	end := time.Now()
	duration := end.Sub(begin)
	log.Debugf("TxAnalyzer: tx: %s analyzed in %fs", txCandidate.Signature.String(), duration.Seconds())

	// Known for sure that empty metadata is InitializeMarket instruction.
	if txCandidate.Metadata == nil {
		if err := a.analyzeInitMarket(rpcTx, tx, txCandidate, infoPublishC); err != nil {
			log.Debugf("TxAnalyzer: error analyzing init market (tx: %s): %s", txCandidate.Signature, err)
		}
		return
	}

	if err := a.analyzeAddLiquidity(rpcTx, tx, txCandidate, infoPublishC); err != nil {
		log.Errorf("TxAnalyzer: error analyzing add liquidity (tx: %s): %s", txCandidate.Signature, err)
	}
	end = time.Now()
	duration = end.Sub(begin)

	if duration.Seconds() > 2 {
		log.Warnf("TxAnalyzer: finished handling of tx: %s in %fs", txCandidate.Signature.String(), duration.Seconds())
	}
}

func (a *TxAnalyzer) getConfirmedTransaction(ctx context.Context, txCandidate TxCandidate) (*rpc.GetTransactionResult, *solana.Transaction, error) {
	rpcClient := a.rpcPool.NamedConnection(txCandidate.clientName).RPCClient

	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			begin := time.Now()
			log.Debugf("load transaction data for %s", txCandidate.Signature.String())
			rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
			rpcTx, err := rpcClient.GetTransaction(rctx, txCandidate.Signature, &rpc.GetTransactionOpts{
				MaxSupportedTransactionVersion: &Max_Transaction_Version,
				Commitment:                     rpc.CommitmentConfirmed,
			})
			end := time.Now()
			duration := end.Sub(begin)
			if duration.Seconds() > 2 {
				log.Warnf("finished loading transaction data in %fs for %s", duration.Seconds(), txCandidate.Signature.String())
			}
			rcancel()

			if err != nil && err.Error() == "not found" {
				log.Debugf("error observed on RPC client %s: %s. This was ignored", rpcClient, err)
				continue
			}
			if err != nil {
				log.Warnf("error observed on RPC client %s: %s", rpcClient, err)
				begin = time.Now()
				rpcClient = a.rpcPool.Client() // Try with another client.
				end = time.Now()
				duration = end.Sub(begin)
				log.Debugf("switched to new rpc client in %fs due to error %s", duration.Seconds(), err)
				continue
			}

			if rpcTx.Meta.Err != nil {
				return nil, nil, fmt.Errorf("Transaction failed: %v", rpcTx.Meta.Err)
			}

			tx, err := rpcTx.Transaction.GetTransaction()
			if err != nil {
				return nil, nil, fmt.Errorf("Couldnt get transaction: %v", err)
			}

			return rpcTx, tx, nil
		}
	}
}

func (a *TxAnalyzer) analyzeInitMarket(rpcTx *rpc.GetTransactionResult, tx *solana.Transaction, txCandidate TxCandidate, infoPublishC chan<- Info) error {
	minfo, err := serum.MarketInfoFromTransaction(rpcTx, tx)
	if err != nil {
		return fmt.Errorf("error getting market info: %w", err)
	}

	infoPublishC <- &minfo

	ainfo, err := raydium.DeriveAmmInfoFromMarket(minfo)
	if err != nil {
		return fmt.Errorf("error deriving amm info from market: %w", err)
	}

	infoPublishC <- ainfo

	tinfo, err := a.TokenInfoFromMarket(minfo)
	if err != nil {
		return fmt.Errorf("error getting token info from market: %w", err)
	}

	infoPublishC <- &tinfo

	return nil
}

func (a *TxAnalyzer) TokenInfoFromMarket(market serum.MarketInfo) (TokenInfo, error) {
	Limit := 100
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sigs, err := a.rpcPool.Client().GetSignaturesForAddressWithOpts(ctx, market.TokenAddress(),
		&rpc.GetSignaturesForAddressOpts{
			Limit: &Limit,
		},
	)
	if err != nil || len(sigs) == 0 {
		return TokenInfo{}, err
	}

	lastSig := sigs[len(sigs)-1]
	if lastSig.BlockTime == nil {
		return TokenInfo{}, fmt.Errorf("no block time in signature")
	}

	var createTime time.Time
	if lastSig.BlockTime != nil {
		createTime = lastSig.BlockTime.Time()
	}

	rpcTokenSupply, err := a.rpcPool.Client().GetTokenSupply(ctx, market.TokenAddress(), rpc.CommitmentFinalized)
	if err != nil {
		return TokenInfo{}, err
	}

	if rpcTokenSupply.Value == nil {
		return TokenInfo{}, fmt.Errorf("no token supply")
	}

	uval, err := strconv.ParseUint(rpcTokenSupply.Value.Amount, 10, 64)
	if err != nil {
		return TokenInfo{}, err
	}

	tinfo := TokenInfo{
		Address:              market.TokenAddress(),
		TxID:                 lastSig.Signature,
		TxTime:               createTime,
		TimeToSerumMarket:    market.TxTime.Sub(createTime),
		TxCountToSerumMarket: uint64(len(sigs)),
		TotalSupply:          uval,
		Decimals:             rpcTokenSupply.Value.Decimals,
	}

	return tinfo, nil
}

func (a *TxAnalyzer) analyzeAddLiquidity(rpcTx *rpc.GetTransactionResult, tx *solana.Transaction, txCandidate TxCandidate, infoPublishC chan<- Info) error {
	// raydium.AmmInfo instruction
	ainfo, err := raydium.AmmInfoFromTransaction(rpcTx, tx, txCandidate.Metadata)
	if err != nil {
		return fmt.Errorf("error getting amm info: %w", err)
	}

	infoPublishC <- &ainfo

	return nil
}

func (a *TxAnalyzer) Stop(ctx context.Context) error {
	close(a.txCandidateC)

	select {
	case <-a.doneC:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
