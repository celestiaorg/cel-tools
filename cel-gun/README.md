# cel-gun

cel-gun is a testing utility meant to be used to load test a given celestia protocol.

## Usage

```bash
sh cel-gun.sh  <network ID> <multiaddr target> <message type> <filepath to message> 
# example sh cel-gun.sh mocha-4 /dnsaddr/mocha-boot.pops.one/p2p/12D3KooWDzNyDSvTBdKQAmnsUdAyQCQWwM3ReXTmPaaf6LzfNwR namespace_data_request ./registry/testdata/nd_req.json
```

### Network ID

Enter the exact network ID of the target node (e.g. `mocha` would have to be entered as `mocha-4`)

### Multiaddr target

The multiaddr of the node that is the target of the load test.

### Message type

Current supported message types:

* `namespace_data_request`
* `header_range_request`

With more types to be added soon

#### Optional: MutableMessage support

If you need to modify the message between submissions, implement the `MutableMessage` interface for your message type.
The client will detect the interface implementation and invoke `msg.Mutate()` between each submission. This enables
testing of cache behavior versus persistent storage by varying request parameters.

### Filepath to message

Path to a file containing a JSON message of the given type with the
desired request. See `nd_req.json` for an example of a namespace data request.

## TODO

- [x] run against resource unlimited node to completion
- [x] make it extensible to any celestia protocol + any message type
- [ ] allow more than one message to be sent (randomised messages w/ some bogus requests)
