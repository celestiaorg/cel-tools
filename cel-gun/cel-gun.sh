#!/bin/bash

# Check if the correct number of arguments were provided
if [ $# -ne 4 ]; then
    echo "Usage: $0 <GUN_NETWORK> <GUN_TARGET> <GUN_NAMESPACE> <GUN_HEIGHT>"
    echo "Example: $0 mocha /ip4/1.2.3.4/tcp/2121/p2p/12D3K... deadbeef 12345"
    exit 1
fi

# Build the binary
echo "Building cel-gun binary..."
go build -o cel-gun

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Build successful!"

# Set environment variables from command line arguments
export GUN_NETWORK="$1"
export GUN_TARGET="$2"
export GUN_NAMESPACE="$3"
export GUN_HEIGHT="$4"

echo "Environment variables set:"
echo "  GUN_NETWORK=$GUN_NETWORK"
echo "  GUN_TARGET=$GUN_TARGET"
echo "  GUN_NAMESPACE=$GUN_NAMESPACE"
echo "  GUN_HEIGHT=$GUN_HEIGHT"

# Run the executable
echo "Running cel-gun..."
./cel-gun

