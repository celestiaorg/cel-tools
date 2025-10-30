package main

import (
	"context"
	"errors"
	"fmt"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/spf13/afero"
	"github.com/yandex/pandora/cli"
	"github.com/yandex/pandora/core"
	coreimport "github.com/yandex/pandora/core/import"
	"github.com/yandex/pandora/core/register"
	"go.uber.org/zap"

	"github.com/renaynay/cel-gun/cel-gun/registry"
)

type Gun struct {
	GunDeps core.GunDeps
	aggr    core.Aggregator

	conf GunConfig

	counter atomic.Int64

	hosts  []host.Host
	target peer.ID
}

type GunConfig struct {
	ProtocolID protocol.ID
	Target     multiaddr.Multiaddr
	Message    registry.Message
	MutRate    registry.MutationRate
	Parallel   int
}

func NewGun(conf GunConfig) *Gun {
	resourceMgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if err != nil {
		panic(err)
	}

	hosts := make([]host.Host, conf.Parallel)
	for i := 0; i < conf.Parallel; i++ {
		h, err := libp2p.New(libp2p.ResourceManager(resourceMgr))
		if err != nil {
			panic(err)
		}
		hosts[i] = h
	}

	return &Gun{
		conf:  conf,
		hosts: hosts,
	}
}

func (g *Gun) Bind(aggr core.Aggregator, deps core.GunDeps) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	info, err := peer.AddrInfoFromP2pAddr(g.conf.Target)
	if err != nil {
		panic(err)
	}
	g.target = info.ID

	for _, h := range g.hosts {
		err = h.Connect(ctx, *info)
		if err != nil {
			panic(err)
		}
	}

	g.aggr = aggr
	g.GunDeps = deps
	return nil
}

func (g *Gun) Shoot(ammo core.Ammo) {
	customAmmo := ammo.(*Ammo)

	g.counter.Add(1)

	randIdx := rand.Intn(g.conf.Parallel)
	g.shoot(context.Background(), customAmmo, g.hosts[randIdx])

	fmt.Println("-------------------------------------------------SHOT ROUND-------------------------------------------------")

	if g.conf.MutRate == registry.PerShot {
		mm, ok := g.conf.Message.(registry.MutableMessage)
		if !ok {
			panic("message is not mutable but mutation rate is set")
		}
		err := mm.Mutate()
		if err != nil {
			panic("failed to mutate message: " + err.Error())
		}

		g.conf.Message = mm
		fmt.Println("-------MUTATED MESSAGE: new height: ", g.conf.Message.StartHeight(), "--------------")
	}
}

func (g *Gun) shoot(ctx context.Context, _ *Ammo, h host.Host) {
	var err error
	defer func() {
		if err != nil {
			g.aggr.Report(Report{
				Fail:    true,
				HostPID: h.ID().String(),
			})
			fmt.Println("Failed to get response from host: ", h.ID().String(), " with error: ", err.Error())
			return
		}
	}()

	fmt.Println("Shooting from host:   ", h.ID().String())

	stream, err := h.NewStream(ctx, g.target, g.conf.ProtocolID)
	if err != nil {
		if errors.Is(errors.Unwrap(err), network.ErrResourceLimitExceeded) {
			return
		}
		panic(err)
	}

	totalBytes, latency, err := Send(ctx, stream, g.conf.Message.Handler())
	if err != nil {
		return
	}

	g.aggr.Report(Report{
		Fail:              false,
		PayloadSize:       uint64(totalBytes),
		TotalDownloadTime: latency,
		DownloadSpeed:     float64(totalBytes) / latency,
		HostPID:           h.ID().String(),
	})

	fmt.Println("Successfully got a response:  ", totalBytes, "      in ", latency, " ms    from host:   ", h.ID().String())
}

type Ammo struct{}

type Report struct {
	Fail              bool    `json:"fail"`
	PayloadSize       uint64  `json:"payload_size"`
	TotalDownloadTime float64 `json:"total_download_time_ms"`
	DownloadSpeed     float64 `json:"download_speed"` // bytes per second
	HostPID           string  `json:"host_pid"`
}

func main() {
	networkID := os.Getenv("GUN_NETWORK")
	targetMultiAddr := os.Getenv("GUN_TARGET")
	messageType := os.Getenv("GUN_MESSAGE_TYPE")
	filePath := os.Getenv("GUN_FILEPATH")

	if networkID == "" || targetMultiAddr == "" || messageType == "" || filePath == "" {
		panic("make sure to provide all the necessary env variables: " +
			"GUN_NETWORK, GUN_TARGET, GUN_MESSAGE_TYPE, GUN_FILEPATH")
	}

	addr, err := multiaddr.NewMultiaddr(targetMultiAddr)
	if err != nil {
		panic(err)
	}
	id, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		panic(err)
	}

	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		panic(fmt.Errorf("failed to read file %s: %w", filePath, err))
	}

	message, err := registry.LoadMessageFromJSON(messageType, jsonData)
	if err != nil {
		panic(fmt.Errorf("failed to load message: %w", err))
	}

	protocolID := message.ProtocolString(networkID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err = message.Preload(ctx, networkID, id.ID)
	if err != nil {
		panic(fmt.Errorf("failed to preload message: %w", err))
	}
	// Standard imports
	fs := afero.NewOsFs()
	coreimport.Import(fs)

	// Custom imports. Integrate your custom types into configuration system.
	coreimport.RegisterCustomJSONProvider("custom_provider", func() core.Ammo { return &Ammo{} })

	register.Gun("cel_gun", NewGun, func() GunConfig {
		var mutRate registry.MutationRate
		if mm, ok := message.(registry.MutableMessage); ok {
			mutRate = mm.Rate()
		}

		return GunConfig{
			ProtocolID: protocol.ID(protocolID),
			Target:     addr,
			Message:    message,
			MutRate:    mutRate,
		}
	})

	cli.Run()

	// suppress logs from pandora as they are not useful to me right now
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel) // discard all INFO  logs from pandora
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(logger)
}

func Send(ctx context.Context, stream network.Stream, handler registry.MessageHandler) (int64, float64, error) {
	totalBytes, latency, err := handler(ctx, stream)
	if err != nil {
		if strings.Contains(err.Error(), "stream reset") || strings.Contains(err.Error(), "stream closed") {
			stream = nil
		}
		fmt.Println("Failed to handle message: ", err.Error())
	}
	return totalBytes, latency, err
}
