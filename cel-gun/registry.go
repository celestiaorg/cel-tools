package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	"io"

	p2p_pb "github.com/celestiaorg/go-header/p2p/pb"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share/shwap"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex"
	shrexpb "github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/pb"
	"github.com/celestiaorg/go-libp2p-messenger/serde"
)

// Message interface defines the contract for all message types
type Message interface {
	// ProtocolString returns the protocol string for this message type
	ProtocolString(string) string
	// StartHeight returns the height for this message
	StartHeight() uint64
	// UnmarshalRequest deserializes the request from bytes
	UnmarshalRequest(data []byte) error
	// MarshalRequest returns the serialized request bytes
	MarshalRequest() ([]byte, error)
	// ReadResponse reads the response from the stream and deserializes it into
	// the expected type
	ReadResponse(stream network.Stream) error
	// GetResponseSize returns the size of the response for metrics
	GetResponseSize() uint64
}

// MessageFactory is a function that creates a new instance of a message type
type MessageFactory func() Message

// messageRegistry holds all registered message types
var messageRegistry = make(map[string]MessageFactory)

// RegisterMessage registers a new message type with the given name
func RegisterMessage(name string, factory MessageFactory) {
	messageRegistry[name] = factory
}

// CreateMessage creates a new message instance by name
func CreateMessage(name string) (Message, error) {
	factory, exists := messageRegistry[name]
	if !exists {
		return nil, fmt.Errorf("unknown message type: %s", name)
	}
	return factory(), nil
}

// LoadMessageFromJSON loads a message from a JSON file
func LoadMessageFromJSON(messageType string, jsonData []byte) (Message, error) {
	msg, err := CreateMessage(messageType)
	if err != nil {
		return nil, err
	}

	err = msg.UnmarshalRequest(jsonData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON for message type %s: %w", messageType, err)
	}

	return msg, nil
}

type HeaderRangeMessage struct {
	request  *p2p_pb.HeaderRequest
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

	h.request = &p2p_pb.HeaderRequest{
		Data:   &p2p_pb.HeaderRequest_Origin{Origin: req.Origin},
		Amount: req.Amount,
	}
	return nil
}

func (h *HeaderRangeMessage) MarshalRequest() ([]byte, error) {
	return h.request.Marshal()
}

func (h *HeaderRangeMessage) ReadResponse(stream network.Stream) error {
	var (
		headers = make([]*p2p_pb.HeaderResponse, 0)
		err     error
	)

	var totalRespLn int
	for i := uint64(0); i < h.request.Amount; i++ {
		resp := new(p2p_pb.HeaderResponse)
		respLn, readErr := serde.Read(stream, resp)
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
			return err
		}

		libheaders[i] = hdr
	}

	h.response = libheaders
	h.respSize = uint64(totalRespSize)
	return nil
}

func (h *HeaderRangeMessage) GetResponseSize() uint64 {
	return h.respSize
}

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

func (n *NamespaceDataMessage) ReadResponse(stream network.Stream) error {
	var resp shrexpb.Response
	_, err := serde.Read(stream, &resp)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	nd := new(shwap.NamespaceData)
	_, err = nd.ReadFrom(stream)
	if err != nil {
		return fmt.Errorf("failed to read namespace data: %w", err)
	}

	n.responseData = nd
	return nil
}

func (n *NamespaceDataMessage) GetResponseSize() uint64 {
	if n.responseData == nil {
		return 0
	}
	return uint64(len(n.responseData.Flatten())) * 512
}

type SampleMessage struct {
	request *shwap.SampleID

	responseData *shwap.Sample
}

func (n *SampleMessage) ProtocolString(network string) string {
	return string(shrex.ProtocolID(network, n.request.Name()))
}

func (n *SampleMessage) StartHeight() uint64 {
	return n.request.Height()
}

func (n *SampleMessage) UnmarshalRequest(data []byte) error {
	sampleId := new(shwap.SampleID)
	err := json.Unmarshal(data, sampleId)
	if err != nil {
		return err
	}

	n.request = sampleId
	return nil
}

func (n *SampleMessage) MarshalRequest() ([]byte, error) {
	return n.request.MarshalBinary()
}

func (n *SampleMessage) ReadResponse(stream network.Stream) error {
	var resp shrexpb.Response
	_, err := serde.Read(stream, &resp)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	sample := new(shwap.Sample)
	_, err = sample.ReadFrom(stream)
	if err != nil {
		return fmt.Errorf("failed to read namespace data: %w", err)
	}

	n.responseData = sample
	return nil
}

func (n *SampleMessage) GetResponseSize() uint64 {
	if n.responseData == nil {
		return 0
	}
	jsonData, err := n.responseData.MarshalJSON()
	if err != nil {
		panic(err)
	}

	return uint64(len(jsonData))
}

// init registers all built-in message types
func init() {
	RegisterMessage("namespace_data_request", func() Message {
		return &NamespaceDataMessage{}
	})
	RegisterMessage("header_range_request", func() Message {
		return &HeaderRangeMessage{}
	})

	RegisterMessage("sample_request", func() Message { return &SampleMessage{} })
}
