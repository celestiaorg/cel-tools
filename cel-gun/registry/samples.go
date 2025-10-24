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
	"github.com/libp2p/go-libp2p/core/host"
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
func (sr *SampleRanges) Preload(ctx context.Context, network string, peer peer.AddrInfo) error {
	protocolString := sr.rngMessage.ProtocolString(network)
	host, err := libp2p.New()
	if err != nil {
		return err
	}

	err = host.Connect(ctx, peer)
	if err != nil {
		return err
	}

	stream, err := host.NewStream(ctx, peer.ID, protocol.ID(protocolString))
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

// Send shoots a range of samples for the specified height.
func (sr *SampleRanges) Send(ctx context.Context, host host.Host, target peer.ID, protocol protocol.ID) (int64, float64, error) {
	sampleRng := sr.ranges[sr.rangeIndex]
	bytesRead := make([]int64, len(sampleRng.sampleMessages))
	timeToRead := make([]time.Duration, len(sampleRng.sampleMessages))
	errCh := make(chan error)
	doneCh := make(chan struct{}, len(sampleRng.sampleMessages))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, message := range sampleRng.sampleMessages {
		go func(i int, message *SampleMessage) {
			stream, err := host.NewStream(ctx, target, protocol)
			if err != nil {
				fmt.Println(err)
				errCh <- err
			}
			defer stream.Close()
			err = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				fmt.Println("set read deadline err: ", err.Error())
			}

			_, err = message.request.WriteTo(stream)
			if err != nil {
				fmt.Println("err Sending a request ", err)
			}
			err = stream.SetReadDeadline(time.Now().Add(time.Minute))
			if err != nil {
				fmt.Println("set read deadline err: ", err.Error())
			}
			var statusResp shrexpb.Response
			statusTime := time.Now()
			_, err = serde.Read(stream, &statusResp)
			if err != nil {
				if errors.Is(err, io.EOF) {
					errCh <- fmt.Errorf("reading a response: %w", shrex.ErrRateLimited)
					return
				}
				errCh <- fmt.Errorf("unexpected error during reading the status from stream: %w", err)
				return
			}

			switch statusResp.Status {
			case shrexpb.Status_OK:
			case shrexpb.Status_NOT_FOUND:
				errCh <- shrex.ErrNotFound
				return
			case shrexpb.Status_INTERNAL:
				errCh <- shrex.ErrInternalServer
				return
			default:
				errCh <- shrex.ErrInvalidResponse
				return
			}

			message.response = new(shwap.Sample)
			length, err := message.response.ReadFrom(stream)
			if err != nil {
				errCh <- fmt.Errorf("%w: %w", shrex.ErrInvalidResponse, err)
				return
			}
			stream.Close()

			bytesRead[i] = length
			timeToRead[i] = time.Since(statusTime)
			sampleRng.sampleMessages[i] = message
			doneCh <- struct{}{}
		}(i, message)
	}

	for range sampleRng.sampleMessages {
		select {
		case err := <-errCh:
			return 0, 0, err
		case <-ctx.Done():
			return 0, 0, errors.New("context canceled")
		case <-doneCh:
		}
	}
	sr.ranges[sr.rangeIndex] = sampleRng
	totaBytesRead := int64(0)
	totalLatency := time.Duration(0)
	for i, read := range bytesRead {
		totaBytesRead += read
		totalLatency += timeToRead[i]
	}
	latency := totalLatency.Milliseconds() / int64(len(sampleRng.sampleMessages))
	return totaBytesRead, float64(latency), nil
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
