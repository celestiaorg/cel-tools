package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/yandex/pandora/cli"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	shrexpb "github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/pb"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
	libshare "github.com/celestiaorg/go-square/v2/share"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/spf13/afero"
	"github.com/yandex/pandora/core"
	coreimport "github.com/yandex/pandora/core/import"
	"github.com/yandex/pandora/core/register"
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
	ProtocolID  protocol.ID
	Target      multiaddr.Multiaddr
	Namespace   libshare.Namespace
	StartHeight uint64
	Parallel    int
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
	fmt.Println("-------------------------------------------------decrementing height...-------------------------------------------------")

	g.conf.StartHeight--
}

func (g *Gun) shoot(ctx context.Context, _ *Ammo, h host.Host) {
	var err error
	defer func() {
		if err != nil {
			g.aggr.Report(Report{
				Fail:    true,
				HostPID: h.ID().String(),
			})
			fmt.Println("Failed to get namespace data from host: ", h.ID().String(), " with error: ", err.Error())
			return
		}
	}()

	ndReq := shwap.NamespaceDataID{
		EdsID: shwap.EdsID{
			Height: g.conf.StartHeight,
		},
		DataNamespace: g.conf.Namespace,
	}

	bin, err := ndReq.MarshalBinary()
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

	var resp shrexpb.Response
	_, err = serde.Read(stream, &resp)
	if err != nil {
		if strings.Contains(err.Error(), "stream reset") || strings.Contains((err.Error()), "stream closed") {
			stream = nil
		}
		fmt.Println("ERR reading shrexpb resp status from stream : ", err.Error())
		return
	}

	nd := new(shwap.NamespaceData)
	_, err = nd.ReadFrom(stream)
	if err != nil {
		if strings.Contains(err.Error(), "stream reset") || strings.Contains((err.Error()), "stream closed") {
			stream = nil
		}
		fmt.Println("ERR reading nd from stream: ", err.Error())
		return
	}
	stream.CloseRead()

	endTime := time.Since(startTime)
	latencyMilliseconds := float64(endTime.Milliseconds())
	responseBytes := int64(len(nd.Flatten())) * 512

	speed := float64(responseBytes) / latencyMilliseconds // bytes per ms

	g.aggr.Report(Report{
		Fail:              false,
		PayloadSize:       responseBytes,
		TotalDownloadTime: latencyMilliseconds,
		DownloadSpeed:     speed,
		HostPID:           h.ID().String(),
	})

	fmt.Println("Successfully got namespace data response:  ", responseBytes, "      in ", latencyMilliseconds, " ms    from host:   ", h.ID().String())
}

type Ammo struct{}

type Report struct {
	Fail              bool    `json:"fail"`
	PayloadSize       int64   `json:"payload_size"`
	TotalDownloadTime float64 `json:"total_download_time_ms"`
	DownloadSpeed     float64 `json:"download_speed"` // bytes per second
	HostPID           string  `json:"host_pid"`
}

func main() {
	networkID := os.Getenv("GUN_NETWORK")
	targetMultiAddr := os.Getenv("GUN_TARGET")
	targetNS := os.Getenv("GUN_NAMESPACE")
	height := os.Getenv("GUN_HEIGHT")

	protocolID := networkID + shrex.ProtocolString + shwap.NamespaceDataName
	ns := parseNS(targetNS)
	startHeight, err := strconv.ParseUint(height, 10, 64)
	if err != nil {
		panic(err)
	}

	// Standard imports
	fs := afero.NewOsFs()
	coreimport.Import(fs)

	// Custom imports. Integrate your custom types into configuration system.
	coreimport.RegisterCustomJSONProvider("custom_provider", func() core.Ammo { return &Ammo{} })

	register.Gun("nd_gun", NewGun, func() GunConfig {
		addr, err := multiaddr.NewMultiaddr(targetMultiAddr)
		if err != nil {
			panic(err)
		}

		return GunConfig{
			ProtocolID:  protocol.ID(protocolID),
			Target:      addr,
			Namespace:   ns,
			StartHeight: startHeight,
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

func parseNS(unparsed string) libshare.Namespace {
	decodedNS, err := hex.DecodeString(unparsed)
	if err != nil {
		panic(err)
	}
	ns, err := libshare.NewNamespace(0, decodedNS)
	if err != nil {
		panic(err)
	}
	return ns
}
