package main

import (
	"bytes"
	"encoding/base64"
	"github.com/celestiaorg/celestia-node/share/shwap"
	p2p_pb "github.com/celestiaorg/go-header/p2p/pb"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistry_HeaderRange(t *testing.T) {
	jsonData, err := os.ReadFile("header_req.json")
	require.NoError(t, err)

	msg, err := LoadMessageFromJSON("header_range_request", jsonData)
	require.NoError(t, err)

	hr, ok := msg.(*HeaderRangeMessage)
	if !ok {
		t.Fatal("message is not of type HeaderRangeMessage")
	}

	require.Equal(t, uint64(12345), hr.request.GetOrigin())
	require.Equal(t, uint64(10), hr.request.GetAmount())

	bin, err := msg.MarshalRequest()
	require.NoError(t, err)

	expected := &p2p_pb.HeaderRequest{}
	err = expected.Unmarshal(bin)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), expected.GetOrigin())
	require.Equal(t, uint64(10), expected.GetAmount())
}

func TestRegistry_NamespaceData(t *testing.T) {
	jsonData, err := os.ReadFile("nd_req.json")
	require.NoError(t, err)

	msg, err := LoadMessageFromJSON("namespace_data_request", jsonData)
	require.NoError(t, err)

	nd, ok := msg.(*NamespaceDataMessage)
	if !ok {
		t.Fatal("message is not of type NamespaceDataMessage")
	}

	expected, err := base64.StdEncoding.DecodeString("AAAAAAAAAAAAAAAAAAAAAAAAAHNvdi1taW5pLWg=")
	require.NoError(t, err)
	require.True(t, bytes.Equal(expected, nd.request.DataNamespace.Bytes()))
	require.Equal(t, uint64(1000000), nd.request.Height)

	bin, err := msg.MarshalRequest()
	require.NoError(t, err)

	ndid, err := shwap.NamespaceDataIDFromBinary(bin)
	require.NoError(t, err)
	require.Equal(t, nd.request.Height, ndid.Height)
	require.True(t, bytes.Equal(nd.request.DataNamespace.Bytes(), ndid.DataNamespace.Bytes()))
}
