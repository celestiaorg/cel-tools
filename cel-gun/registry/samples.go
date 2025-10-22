package registry

import (
	"context"
	"errors"
	"fmt"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/availability/light"
	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	shrexpb "github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/pb"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"io"
	"time"
)

// SampleMessage contains both shwap.SampleID and shwap.Sample to request sample.
type SampleMessage struct {
	request *shwap.SampleID

	response *shwap.Sample
}

func NewSampleMessage(height uint64, squareSize int, coords shwap.SampleCoords) *SampleMessage {
	req, err := shwap.NewSampleID(height, coords, squareSize)
	if err != nil {
		panic(err)
	}
	return &SampleMessage{
		request: &req,
	}
}

// SamplesRange is a collection of sample messages at the particular height
type SamplesRange struct {
	sampleMessages []*SampleMessage
}

func NewSamplesRange(height uint64, squareSize int) *SamplesRange {
	samplingResult := light.NewSamplingResult(squareSize, int(light.DefaultSampleAmount))
	messages := make([]*SampleMessage, len(samplingResult.Remaining))
	for i := range samplingResult.Remaining {
		messages[i] = NewSampleMessage(height, squareSize, samplingResult.Remaining[i])
	}
	return &SamplesRange{
		sampleMessages: messages,
	}
}

// SampleRanges is a collection of SamplesRange for multiple heights.
type SampleRanges struct {
	rngMessage *HeaderRangeMessage

	ranges []*SamplesRange

	// last handled index from `ranges`
	rangeIndex int
}

// ProtocolString returns the protocol string for this message type
func (sr *SampleRanges) ProtocolString(network string) string {
	return string(shrex.ProtocolID(network, shwap.SampleID{}.Name()))
}

// StartHeight returns the height for this message
func (sr *SampleRanges) StartHeight() uint64 {
	return sr.rngMessage.response[sr.rangeIndex].Height()
}

// UnmarshalRequest deserializes the request from bytes
// Has to be used along with `Preload`
func (sr *SampleRanges) UnmarshalRequest(data []byte) error {
	rng := new(HeaderRangeMessage)
	err := rng.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	sr.rngMessage = rng
	return nil
}

// Preload creates a default host and requests a range of headers from the specified peer. These headers
// will be used to build sample ranges.
func (sr *SampleRanges) Preload(ctx context.Context, network string, peer peer.ID) error {
	protocolString := sr.rngMessage.ProtocolString(network)
	host, err := libp2p.New()
	if err != nil {
		return err
	}
	stream, err := host.NewStream(ctx, peer, protocol.ID(protocolString))
	if err != nil {
		return err
	}
	defer stream.Close()

	_, err = sr.rngMessage.WriteTo(stream)
	if err != nil {
		return err
	}

	_, err = sr.rngMessage.ReadFrom(stream)
	if err != nil {
		return err
	}

	if len(sr.rngMessage.response) != int(sr.rngMessage.request.GetAmount()) {
		return fmt.Errorf("invalid number of headers received, expected %d, got %d",
			sr.rngMessage.request.GetAmount(), len(sr.rngMessage.response),
		)
	}
	result := skipEmptyHeaders(sr.rngMessage.response)
	if len(result) == 0 {
		return fmt.Errorf("headers range contained empty headers")
	}
	sr.rngMessage.response = result

	sr.ranges = make([]*SamplesRange, len(sr.rngMessage.response))
	for i, message := range sr.rngMessage.response {
		sr.ranges[i] = NewSamplesRange(message.Height(), len(message.DAH.RowRoots))
	}
	return nil
}

// MarshalRequest returns the serialized request bytes
func (sr *SampleRanges) MarshalRequest() ([]byte, error) {
	panic("not implemented")

}

// GetResponseSize returns the size of the response for metrics
func (sr *SampleRanges) GetResponseSize() uint64 {
	panic("not implemented")
}

// Handler returns function that shoots a range of samples for the specified height. It
// returns total latency and bytes read for a particular range.
func (sr *SampleRanges) Handler() MessageHandler {
	return func(ctx context.Context, stream network.Stream) (int64, float64, error) {
		sampleRng := sr.ranges[sr.rangeIndex]
		var (
			totalBytes   int64
			totalLatency float64
		)
		for i, message := range sampleRng.sampleMessages {
			err := stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				fmt.Println("set read deadline err: ", err.Error())
			}

			_, err = message.request.WriteTo(stream)
			if err != nil {
				_ = stream.Close()
				return 0, 0, err
			}

			err = stream.SetReadDeadline(time.Now().Add(time.Minute))
			if err != nil {
				fmt.Println("set read deadline err: ", err.Error())
			}

			var statusResp shrexpb.Response
			_, err = serde.Read(stream, &statusResp)
			if err != nil {
				_ = stream.Close()
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

			startTime := time.Now()
			length, err := message.request.ReadFrom(stream)
			if err != nil {
				return 0, 0, fmt.Errorf("%w: %w", shrex.ErrInvalidResponse, err)
			}

			endTime := time.Since(startTime)
			totalLatency += float64(endTime.Milliseconds())
			totalBytes += length
			sampleRng.sampleMessages[i] = message
		}

		sr.ranges[sr.rangeIndex] = sampleRng
		_ = stream.Close()
		return totalBytes, totalLatency, nil
	}

}

func (sr *SampleRanges) Rate() MutationRate { return PerShot }

// Mutate updates `rangeIndex` allowing to request new range on the nex iteration.
// Fails if `rangeIndex` will be out of range(>= amount of ranges)
func (sr *SampleRanges) Mutate() error {
	sr.rangeIndex++
	if sr.rangeIndex >= len(sr.ranges) {
		return fmt.Errorf("samples ranges exceeds maximum number of samples ranges")
	}
	return nil
}

func skipEmptyHeaders(hdrs []*header.ExtendedHeader) []*header.ExtendedHeader {
	result := make([]*header.ExtendedHeader, 0)
	for _, hdr := range hdrs {
		if !share.DataHash(hdr.DataHash).IsEmptyEDS() {
			result = append(result, hdr)
		}
	}
	return result
}
