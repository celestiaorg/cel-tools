package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"io"
	"time"

	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	shrexpb "github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/pb"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// NamespaceDataMessage implements the Message interface for namespace data requests
type NamespaceDataMessage struct {
	request *shwap.NamespaceDataID

	responseData *shwap.NamespaceData
}

func (n *NamespaceDataMessage) Mutate() error {
	newEDSID, err := shwap.NewEdsID(n.request.Height() - 500)
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

func (n *NamespaceDataMessage) Handler() MessageHandler {
	return func(ctx context.Context, stream network.Stream) (int64, float64, error) {
		err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		_, err = n.request.WriteTo(stream)
		if err != nil {
			return 0, 0, err
		}
		_ = stream.CloseWrite()

		err = stream.SetReadDeadline(time.Now().Add(time.Minute))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		// wrap the entire read (of status + resp) with time
		// to record real latency of the request
		startTime := time.Now()

		var statusResp shrexpb.Response
		statusBytes, err := serde.Read(stream, &statusResp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, 0, fmt.Errorf("reading a response: %w", shrex.ErrRateLimited)
			}
			return 0, 0, fmt.Errorf("unexpected error during reading the status from stream: %w", err)
		}

		switch statusResp.Status {
		case shrexpb.Status_OK:
		case shrexpb.Status_NOT_FOUND:
			return 0, 0, shrex.ErrNotFound
		case shrexpb.Status_INTERNAL:
			return 0, 0, shrex.ErrInternalServer
		default:
			return 0, 0, shrex.ErrInvalidResponse
		}

		n.responseData = new(shwap.NamespaceData)

		respBytes, err := n.ReadFrom(stream)
		if err != nil {
			return 0, 0, fmt.Errorf("%w: %w", shrex.ErrInvalidResponse, err)
		}

		endTime := time.Since(startTime)
		totalBytes := int64(statusBytes) + respBytes

		_ = stream.CloseRead()

		return totalBytes, float64(endTime.Milliseconds()), nil
	}
}

func (n *NamespaceDataMessage) Preload(context.Context, string, peer.ID) error {
	return nil
}
