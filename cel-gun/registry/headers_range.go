package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/celestiaorg/celestia-node/header"
	p2ppb "github.com/celestiaorg/go-header/p2p/pb"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type HeaderRangeMessage struct {
	metricProvider

	request  *p2ppb.HeaderRequest
	response []*header.ExtendedHeader
	respSize uint64
}

func (h *HeaderRangeMessage) ProtocolString(networkID string) string {
	base := protocol.ID("header-ex/v0.0.3")
	return fmt.Sprintf("/%s/%s", networkID, base)
}

func (h *HeaderRangeMessage) StartHeight() uint64 {
	return h.request.GetOrigin()
}

func (h *HeaderRangeMessage) UnmarshalRequest(data []byte) error {
	req := &struct {
		Origin uint64 `json:"origin"`
		Amount uint64 `json:"amount"`
	}{}
	err := json.Unmarshal(data, req)
	if err != nil {
		return err
	}

	h.request = &p2ppb.HeaderRequest{
		Data:   &p2ppb.HeaderRequest_Origin{Origin: req.Origin},
		Amount: req.Amount,
	}
	return nil
}

func (h *HeaderRangeMessage) MarshalRequest() ([]byte, error) {
	buf := make([]byte, h.request.Size()+h.request.Size())
	_, err := serde.Marshal(h.request, buf)
	return buf, err
}

func (h *HeaderRangeMessage) GetResponseSize() uint64 {
	return h.respSize
}

func (h *HeaderRangeMessage) ReadFrom(reader io.Reader) (int64, error) {
	var (
		headers = make([]*p2ppb.HeaderResponse, 0)
		err     error
	)

	var totalRespLn int
	for i := uint64(0); i < h.request.Amount; i++ {
		resp := new(p2ppb.HeaderResponse)
		respLn, readErr := serde.Read(reader, resp)
		if readErr != nil {
			err = readErr
			break
		}

		totalRespLn += respLn
		headers = append(headers, resp)
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}

	libheaders := make([]*header.ExtendedHeader, len(headers))
	totalRespSize := 0

	for i, h := range headers {
		hdr := &header.ExtendedHeader{}

		totalRespSize += len(h.Body)

		err = hdr.UnmarshalBinary(h.Body)
		if err != nil {
			return 0, err
		}

		libheaders[i] = hdr
	}

	h.response = libheaders
	h.respSize = uint64(totalRespSize)
	return int64(totalRespLn), nil
}

func (h *HeaderRangeMessage) WriteTo(writer io.Writer) (int64, error) {
	n, err := serde.Write(writer, h.request)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

// Mutate will decrement the header range request
func (h *HeaderRangeMessage) Mutate() error {
	origin := h.request.GetOrigin()
	amount := h.request.GetAmount()

	if origin <= 1 {
		// we've reached the end, cannot mutate further
		return fmt.Errorf("mutate: header range fully requested")
	}

	var (
		newAmount uint64
		newOrigin uint64
	)

	// Calculate the new origin by moving back by 'amount'
	if origin <= amount {
		// We're close to genesis, so we can't move back a full 'amount'
		// Request from genesis (1) up to just before the current origin
		newOrigin = 1
		newAmount = origin - 1
	} else {
		// Move back by 'amount'
		newOrigin = origin - amount
		newAmount = amount
	}

	h.request = &p2ppb.HeaderRequest{
		Data:   &p2ppb.HeaderRequest_Origin{Origin: newOrigin},
		Amount: newAmount,
	}

	return nil
}

func (h *HeaderRangeMessage) Rate() MutationRate { return PerShot }

func (h *HeaderRangeMessage) Handler() MessageHandler {
	return func(ctx context.Context, stream network.Stream) error {
		err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		_, err = h.WriteTo(stream)
		if err != nil {
			return err
		}
		_ = stream.CloseWrite()

		err = stream.SetReadDeadline(time.Now().Add(time.Minute))
		if err != nil {
			fmt.Println("set read deadline err: ", err.Error())
		}

		startTime := time.Now()

		_, err = h.ReadFrom(stream)
		if err != nil {
			fmt.Println("ERR reading message from stream: ", err.Error())
			return err
		}
		_ = stream.CloseRead()

		endTime := time.Since(startTime)
		h.latency = float64(endTime.Milliseconds())
		h.totalBytes = int64(h.GetResponseSize())
		return nil
	}
}
