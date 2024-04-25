package connection

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"

	"github.com/codingsandmore/rayscan/config"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/jsonrpc"
)

type Connection struct {
	ConnectionInfo config.RPCNode
	RPCClient      *rpc.Client
	CooldownUntil  time.Time
}

type RPCPool struct {
	Connections []*Connection
	CurrentIdx  int
	mu          sync.RWMutex
}

func (r *RPCPool) Size() int {
	return len(r.Connections)
}

func (r *RPCPool) BaseConnection() Connection {
	for _, c := range r.Connections {
		if c.ConnectionInfo.RPCEndpoint == rpc.MainNetBeta_RPC {
			return *c
		}
	}

	panic("No base connection found!")
}

func (r *RPCPool) NamedConnection(name string) Connection {
	for _, c := range r.Connections {
		if c.ConnectionInfo.Name == name {
			return *c
		}
	}

	panic(fmt.Sprintf("No connection with name %s found!", name))
}

func (r *RPCPool) Client() *rpc.Client {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldIdx := r.CurrentIdx

	for i := 0; r.Connections[oldIdx].CooldownUntil.After(time.Now()); i++ {
		oldIdx = (oldIdx + 1) % len(r.Connections)
		if i == len(r.Connections) {
			log.Debugf("All connections are on cooldown, waiting for cooldown to end... Consider adding new RPC providers\n")
			time.Sleep(50 * time.Millisecond)
			i = 0
		}
	}

	r.CurrentIdx = (oldIdx + 1) % len(r.Connections)
	return r.Connections[oldIdx].RPCClient
}

func (r *RPCPool) Close() {
	for _, c := range r.Connections {
		c.RPCClient.Close()
	}

	r.Connections = nil
}

type ConnectionInfo struct {
	Name    string
	RPCNode string
	WSNode  string
}

type BuildRPCClient func(url string) *rpc.Client

func NewRPCClientPool(nodes map[string]config.RPCNode, init BuildRPCClient) (*RPCPool, error) {
	var rpcPool RPCPool

	initialLen := len(nodes)
	log.Debugf("Checking connection list...\n")

	for k, v := range nodes {
		var rpcClient *rpc.Client

		if init == nil {
			log.Warnf("using default init for client creation, you really want to use an init function!!!")
			rpcClient = rpc.New(v.RPCEndpoint)
		} else {
			rpcClient = init(v.RPCEndpoint)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2000*time.Millisecond)
		health, err := rpcClient.GetHealth(ctx)
		cancel()

		if err != nil || health != "ok" {
			reason := err.Error()
			code := -1
			var asErr *jsonrpc.RPCError
			if errors.As(err, &asErr) {
				reason = err.(*jsonrpc.RPCError).Message
				code = err.(*jsonrpc.RPCError).Code
			}

			log.Debugf("Remove unhealthy connection: %s (reason: %s, code: %d)\n", v.Name, reason, code)
			rpcClient.Close()
			continue
		}

		v.Name = k
		log.Debugf("Connection %s is healthy\n", v.Name)

		rpcPool.Connections = append(rpcPool.Connections, &Connection{
			ConnectionInfo: v,
			RPCClient:      rpcClient,
		})
	}

	if len(rpcPool.Connections) == 0 {
		return nil, fmt.Errorf("No healthy connections found!")
	}

	healthyConnectionNames := make([]string, len(rpcPool.Connections))
	for i, c := range rpcPool.Connections {
		healthyConnectionNames[i] = c.ConnectionInfo.Name
	}

	log.Debugf("Connection list checked! %d/%d connections are ok [%s]\n", len(rpcPool.Connections), initialLen, strings.Join(healthyConnectionNames, ", "))
	return &rpcPool, nil
}
