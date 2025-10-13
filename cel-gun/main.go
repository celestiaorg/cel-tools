package main

import (
	"context"
	"errors"
	"fmt"
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
)

type Gun struct {
	GunDeps core.GunDeps
	aggr    core.Aggregator

	conf GunConfig

	counter atomic.Int64

	hosts  []host.Host
	target peer.ID
}

// MutationRate defines how often to mutate the message
type MutationRate int

// TODO @renaynay: eventually add more mutation strategies here
const (
	None MutationRate = iota
	PerShot
)

type GunConfig struct {
	ProtocolID protocol.ID
	Target     multiaddr.Multiaddr
	Message    Message
	MutRate    MutationRate
	Parallel   int
}

func NewGun(conf GunConfig) *Gun {
	hosts := make([]host.Host, conf.Parallel)
	for i := 0; i < conf.Parallel; i++ {
		h, err := libp2p.New()
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

	if g.conf.MutRate == PerShot {
		mm, ok := g.conf.Message.(MutableMessage)
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

	bin, err := g.conf.Message.MarshalRequest()
	if err != nil {
		panic(err)
	}

	fmt.Println("Shooting from host:   ", h.ID().String())

	stream, err := h.NewStream(ctx, g.target, g.conf.ProtocolID)
	if err != nil {
		if errors.Is(errors.Unwrap(err), network.ErrResourceLimitExceeded) {
			return
		}
		panic(err)
	}

	err = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		fmt.Println("set read deadline err: ", err.Error())
	}

	_, err = stream.Write(bin)
	if err != nil {
		if strings.Contains(err.Error(), "stream reset") || strings.Contains((err.Error()), "stream closed") {
			stream = nil
		}
		return
	}
	stream.CloseWrite()

	err = stream.SetReadDeadline(time.Now().Add(time.Minute))
	if err != nil {
		fmt.Println("set read deadline err: ", err.Error())
	}

	startTime := time.Now()

	err = g.conf.Message.ReadResponse(stream)
	if err != nil {
		if strings.Contains(err.Error(), "stream reset") || strings.Contains((err.Error()), "stream closed") {
			stream = nil
		}
		fmt.Println("ERR reading message from stream: ", err.Error())
		return
	}
	stream.CloseRead()

	endTime := time.Since(startTime)
	latencyMilliseconds := float64(endTime.Milliseconds())
	responseBytes := g.conf.Message.GetResponseSize()

	speed := float64(responseBytes) / latencyMilliseconds // bytes per ms

	g.aggr.Report(Report{
		Fail:              false,
		PayloadSize:       responseBytes,
		TotalDownloadTime: latencyMilliseconds,
		DownloadSpeed:     speed,
		HostPID:           h.ID().String(),
	})

	fmt.Println("Successfully got a response:  ", responseBytes, "      in ", latencyMilliseconds, " ms    from host:   ", h.ID().String())
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

	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		panic(fmt.Errorf("failed to read file %s: %w", filePath, err))
	}

	message, err := LoadMessageFromJSON(messageType, jsonData)
	if err != nil {
		panic(fmt.Errorf("failed to load message: %w", err))
	}

	protocolID := message.ProtocolString(networkID)

	// Standard imports
	fs := afero.NewOsFs()
	coreimport.Import(fs)

	// Custom imports. Integrate your custom types into configuration system.
	coreimport.RegisterCustomJSONProvider("custom_provider", func() core.Ammo { return &Ammo{} })

	register.Gun("cel_gun", NewGun, func() GunConfig {
		addr, err := multiaddr.NewMultiaddr(targetMultiAddr)
		if err != nil {
			panic(err)
		}

		var mutRate MutationRate
		if mm, ok := message.(MutableMessage); ok {
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
