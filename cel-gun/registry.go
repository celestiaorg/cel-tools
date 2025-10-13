package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	"io"

	p2ppb "github.com/celestiaorg/go-header/p2p/pb"
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

// MutableMessage extends Message with mutation capabilities
type MutableMessage interface {
	Message

	Mutate() error
	Rate() MutationRate
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

func (h *HeaderRangeMessage) ReadResponse(stream network.Stream) error {
	var (
		headers = make([]*p2ppb.HeaderResponse, 0)
		err     error
	)

	var totalRespLn int
	for i := uint64(0); i < h.request.Amount; i++ {
		resp := new(p2ppb.HeaderResponse)
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

// init registers all built-in message types
func init() {
	RegisterMessage("namespace_data_request", func() Message {
		return &NamespaceDataMessage{}
	})
	RegisterMessage("header_range_request", func() Message {
		return &HeaderRangeMessage{}
	})
}
