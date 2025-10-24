package registry

import (
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// init registers all built-in message types
func init() {
	RegisterMessage("namespace_data_request", func() Message {
		return &NamespaceDataMessage{}
	})
	RegisterMessage("header_range_request", func() Message {
		return &HeaderRangeMessage{}
	})
	RegisterMessage("samples_request", func() Message { return &SampleRanges{} })
}

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
	// GetResponseSize returns the size of the response for metrics
	GetResponseSize() uint64

	// Preload performs any initialization or resource loading that may improve
	// performance or prepare the implementation for use. Calling this method
	// is optional.
	// Implementations without preload requirements should return nil.
	Preload(context.Context, string, peer.AddrInfo) error

	// Send sends the message to the specified peer under specific protocol ID.
	Send(context.Context, host.Host, peer.ID, protocol.ID) (int64, float64, error)
}

// MutationRate defines how often to mutate the message
type MutationRate int

// TODO @renaynay: eventually add more mutation strategies here
const (
	None MutationRate = iota
	PerShot
)

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
