package registry

import (
	"encoding/json"
	"io"

	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
)

// NamespaceDataMessage implements the Message interface for namespace data requests
type NamespaceDataMessage struct {
	request *shwap.NamespaceDataID

	responseData *shwap.NamespaceData
}

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
	// TODO: read status first
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
