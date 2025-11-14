// Handling samples requests:
// 1. Use headers_request.json to preload headers and build header ranges
// 2. These headers MUST NOT be empty
// 3. Since libp2p limits concurrent connections from the same IP address to 8,
//    the resource manager on the server side should be set to NullResourceManager
//    to allow load testing with multiple workers from a single IP
// 4. `times` field in load.yaml is responsible for spawning workers

package registry

import (
	"context"
	"errors"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/availability/light"
	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	p2ppb "github.com/celestiaorg/go-header/p2p/pb"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/yandex/pandora/core"
	"golang.org/x/sync/errgroup"
	"sync"
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
	Origin uint64

	ranges []*SamplesRange

	rangeMk sync.Mutex
	// last handled index from `ranges`
	rangeIndex int

	height uint64
}

// ProtocolString returns the protocol string for this message type
func (sr *SampleRanges) ProtocolString(network string) string {
	return string(shrex.ProtocolID(network, shwap.SampleID{}.Name()))
}

// StartHeight returns the height for this message
func (sr *SampleRanges) StartHeight() uint64 {
	sr.rangeMk.Lock()
	defer sr.rangeMk.Unlock()
	return sr.ranges[sr.rangeIndex].sampleMessages[0].request.Height()
}

// UnmarshalRequest deserializes the request from bytes
// Has to be used along with `Preload`
func (sr *SampleRanges) UnmarshalRequest(data []byte) error {
	rng := new(HeaderRangeMessage)
	err := rng.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	sr.Origin = rng.request.GetOrigin()
	sr.height = rng.request.GetOrigin()
	return nil
}

// Preload creates a default host and requests a range of headers from the specified peer. These headers
// will be used to build sample ranges.
func (sr *SampleRanges) Preload(ctx context.Context, network string, peer peer.AddrInfo) error {
	amountPerReq := uint64(64)

	hr := &HeaderRangeMessage{
		request: &p2ppb.HeaderRequest{
			Data:   &p2ppb.HeaderRequest_Origin{Origin: sr.Origin},
			Amount: amountPerReq,
		},
	}
	protocolString := hr.ProtocolString(network)

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

	recievedHeadersCount, targetedHeaders := 0, 20

	receivedHeaders := make([]*header.ExtendedHeader, 0, targetedHeaders)
	for recievedHeadersCount < targetedHeaders {
		_, err := hr.WriteTo(stream)
		if err != nil {
			return err
		}
		_, err = hr.ReadFrom(stream)
		if err != nil {
			return err
		}

		hdrs := skipEmptyHeaders(hr.response)
		receivedHeaders = append(receivedHeaders, hdrs...)
		recievedHeadersCount += len(hdrs)
		if recievedHeadersCount == targetedHeaders {
			break
		}

		remainingHeaders := uint64(targetedHeaders - recievedHeadersCount)
		if remainingHeaders > amountPerReq {
			remainingHeaders = amountPerReq
		}

		lastHeight := hr.request.GetOrigin()
		if len(receivedHeaders) > 0 {
			lastHeight = receivedHeaders[len(receivedHeaders)-1].Height()
		}
		hr = &HeaderRangeMessage{
			request: &p2ppb.HeaderRequest{
				Data:   &p2ppb.HeaderRequest_Origin{Origin: lastHeight + 1},
				Amount: remainingHeaders,
			},
		}
	}

	sr.ranges = make([]*SamplesRange, len(receivedHeaders))
	for i, hdr := range receivedHeaders {
		go func(i int, h *header.ExtendedHeader) {
			sr.ranges[i] = NewSamplesRange(h.Height(), len(h.DAH.RowRoots))
		}(i, hdr)

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
func (sr *SampleRanges) Send(ctx context.Context, host host.Host, target peer.ID, networkID string, aggregator core.Aggregator) (int64, float64, error) {
	sr.rangeMk.Lock()
	rng := sr.ranges[sr.rangeIndex]
	sr.rangeMk.Unlock()

	params := shrex.DefaultClientParameters()
	params.WithNetworkID(networkID)
	client, err := shrex.NewClient(params, host)
	if err != nil {
		return 0, 0, err
	}

	bytesRead := make([]int64, len(rng.sampleMessages))
	timeToRead := make([]time.Duration, len(rng.sampleMessages))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errGroup, ctx := errgroup.WithContext(ctx)

	wallClock := time.Now()

	for i, message := range rng.sampleMessages {
		errGroup.Go(func() error {
			resp := new(shwap.Sample)
			start := time.Now()
			length, err := client.Get(ctx, message.request, resp, target)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			end := time.Since(start)
			bytesRead[i] = length
			timeToRead[i] = end
			aggregator.Report(struct {
				Fail          bool    `json:"fail"`
				PayloadSize   uint64  `json:"sample_payload_size"`
				Height        uint64  `json:"height"`
				RowIndex      uint64  `json:"row_index"`
				ColIndex      uint64  `json:"col_index"`
				DownloadTime  float64 `json:"download_time_ms"`
				DownloadSpeed float64 `json:"download_speed_b_per_s"` // bytes per second
			}{
				Fail:          false,
				PayloadSize:   uint64(length),
				Height:        message.request.Height(),
				RowIndex:      uint64(message.request.RowIndex),
				ColIndex:      uint64(message.request.ShareIndex),
				DownloadTime:  float64(end.Milliseconds()),
				DownloadSpeed: float64(length/end.Milliseconds()) * 1000,
			},
			)
			return nil
		})
	}
	err = errGroup.Wait()
	if err != nil {
		return 0, 0, err
	}

	totalBytesRead := int64(0)
	totalLatency := time.Duration(0)
	for i, read := range bytesRead {
		totalBytesRead += read
		totalLatency += timeToRead[i]
	}

	// latency of the longest request
	wallClockEnd := time.Since(wallClock)

	return totalBytesRead, float64(wallClockEnd.Milliseconds()), nil
}

func (sr *SampleRanges) Rate() MutationRate { return PerShot }

// Mutate updates `rangeIndex` allowing to request new range on the nex iteration.
// Fails if `rangeIndex` will be out of range(>= amount of ranges)
func (sr *SampleRanges) Mutate() error {
	sr.rangeMk.Lock()
	defer sr.rangeMk.Unlock()
	sr.rangeIndex++
	if sr.rangeIndex >= len(sr.ranges) {
		sr.rangeIndex = 0
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
