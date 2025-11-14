package registry

import (
	"context"
	"encoding/json"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/yandex/pandora/core"

	"io"
	"time"

	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NamespaceDataMessage implements the Message interface for namespace data requests
type NamespaceDataMessage struct {
	request *shwap.NamespaceDataID

	responseData *shwap.NamespaceData
}

func (n *NamespaceDataMessage) Mutate() error {
	newEDSID, err := shwap.NewEdsID(n.request.Height() - 10)
	if err != nil {
		return err
	}
	ns := n.request.DataNamespace
	n.request = &shwap.NamespaceDataID{
		EdsID:         newEDSID,
		DataNamespace: ns,
	}
	return nil
}

func (n *NamespaceDataMessage) Rate() MutationRate { return PerShot }

func (n *NamespaceDataMessage) ProtocolString(network string) string {
	return string(shrex.ProtocolID(network, n.request.Name()))
}

func (n *NamespaceDataMessage) StartHeight() uint64 {
	return n.request.Height()
}

func (n *NamespaceDataMessage) UnmarshalRequest(data []byte) error {
	ndid := new(shwap.NamespaceDataID)
	err := json.Unmarshal(data, ndid)
	if err != nil {
		return err
	}

	n.request = ndid
	return nil
}

func (n *NamespaceDataMessage) MarshalRequest() ([]byte, error) {
	return n.request.MarshalBinary()
}

func (n *NamespaceDataMessage) ReadFrom(reader io.Reader) (int64, error) {
	return n.responseData.ReadFrom(reader)
}

func (n *NamespaceDataMessage) WriteTo(writer io.Writer) (int64, error) {
	return n.request.WriteTo(writer)
}

func (n *NamespaceDataMessage) GetResponseSize() uint64 {
	if n.responseData == nil {
		return 0
	}
	return uint64(len(n.responseData.Flatten())) * 512
}

func (n *NamespaceDataMessage) Preload(context.Context, string, peer.AddrInfo) error {
	return nil
}

func (n *NamespaceDataMessage) Send(ctx context.Context, host host.Host, target peer.ID, networkID string, _ core.Aggregator) (int64, float64, error) {
	params := shrex.DefaultClientParameters()
	params.WithNetworkID(networkID)
	client, err := shrex.NewClient(params, host)
	if err != nil {
		return 0, 0, err
	}
	err = client.WithMetrics()
	if err != nil {
		return 0, 0, err
	}
	start := time.Now()
	length, err := client.Get(ctx, n.request, n.responseData, target)
	if err != nil {
		return 0, 0, err
	}

	end := time.Since(start)
	return length, float64(end), nil
}
