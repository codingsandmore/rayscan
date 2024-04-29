package onchain

import (
	"fmt"
	"sync"
	"time"

	"github.com/codingsandmore/rayscan/onchain/raydium"
	"github.com/codingsandmore/rayscan/onchain/serum"
	"github.com/gagliardetto/solana-go"
)

type TokenInfo struct {
	TimeToSerumMarket    time.Duration
	TxCountToSerumMarket uint64
	TotalSupply          uint64
	Decimals             uint8
	TxID                 solana.Signature
	TxTime               time.Time
	Address              solana.PublicKey
}

func (t *TokenInfo) String() string {
	return fmt.Sprintf("Token("+
		"supply: %d"+
		"decimals: %d"+
		"address: %s"+
		")", t.TotalSupply, t.Decimals, t.Address)
}
func (t *TokenInfo) TokenAddress() solana.PublicKey {
	return t.Address
}

func (t *TokenInfo) Ready() bool {
	return t.TimeToSerumMarket != 0
}

type PairInfo struct {
	// MarketInfo is the info about the market.
	MarketInfo serum.MarketInfo

	// TokenInfo is the metadata of token related to serum market info.
	TokenInfo TokenInfo

	// raydium.AmmInfo is the info about the AMM.
	AmmInfo           raydium.AmmInfo
	CalculatedAmmInfo raydium.AmmInfo

	// PairInfo metadata.
	Readiness time.Time // Timestamp of when the first swap is ready to be executed.

	mu sync.RWMutex
}

func (p *PairInfo) String() string {
	return fmt.Sprintf("Pair("+
		" token: %s"+
		" amm %s"+
		" calculated amm: %s"+
		" readiness: %s"+
		")", p.TokenInfo.String(), p.AmmInfo.String(), p.CalculatedAmmInfo.String(), p.Readiness)
}
func (p *PairInfo) GetCurrentAmmLiveInfo() raydium.AmmLiveInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.AmmInfo.CurrentLiveInfo
}

func (p *PairInfo) SetCurrentAmmLiveInfo(ammLiveInfo raydium.AmmLiveInfo) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.AmmInfo.CurrentLiveInfo = ammLiveInfo
}
func (p *PairInfo) TokenAddress() solana.PublicKey {
	return p.MarketInfo.TokenAddress()
}

func (p *PairInfo) Ready() bool {
	return p.MarketInfo.Ready() && p.AmmInfo.Ready() && p.TokenInfo.Ready()
}
