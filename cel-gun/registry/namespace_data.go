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
)

// NamespaceDataMessage implements the Message interface for namespace data requests
type NamespaceDataMessage struct {
	metricProvider

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

func (n *NamespaceDataMessage) Handler() MessageHandler {
	return func(ctx context.Context, stream network.Stream) error {
		err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		_, err = n.request.WriteTo(stream)
		if err != nil {
			return err
		}
		_ = stream.CloseWrite()

		err = stream.SetReadDeadline(time.Now().Add(time.Minute))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		var statusResp shrexpb.Response
		_, err = serde.Read(stream, &statusResp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("reading a response: %w", shrex.ErrRateLimited)
			}
			return fmt.Errorf("unexpected error during reading the status from stream: %w", err)
		}

		switch statusResp.Status {
		case shrexpb.Status_OK:
		case shrexpb.Status_NOT_FOUND:
			return shrex.ErrNotFound
		case shrexpb.Status_INTERNAL:
			return shrex.ErrInternalServer
		default:
			return shrex.ErrInvalidResponse
		}

		startTime := time.Now()
		totalBytes, err := n.ReadFrom(stream)
		if err != nil {
			return fmt.Errorf("%w: %w", shrex.ErrInvalidResponse, err)
		}

		endTime := time.Since(startTime)
		n.latency = float64(endTime.Milliseconds())
		n.totalBytes = totalBytes
		_ = stream.CloseRead()
		return nil
	}
}
