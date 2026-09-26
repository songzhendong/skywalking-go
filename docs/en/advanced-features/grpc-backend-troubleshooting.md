# gRPC Backend Connectivity Troubleshooting

## Multi-address failover

Configure comma-separated backends:

```yaml
reporter:
  grpc:
    backend_service: oap-a:11800,oap-b:11800
```

Use `host:port` entries with numeric ports from 1 to 65535 and brackets around
IPv6 addresses (`[::1]:11800`). The agent trims whitespace, ignores empty entries,
and removes duplicates. Unlike the Python and Node.js parsers, Go rejects a
malformed list instead of silently dropping its invalid entries. A list that
normalizes to one address uses the existing single-address connection behavior.

For multiple addresses, the agent creates one gRPC channel and shuffles its
static endpoint list once. Native `pick_first` selects the first reachable
backend in that order and keeps using it until its transport fails. This follows
the Node.js and Python failover model and distributes agents across backends.
Resolver refreshes preserve the shuffled order.

The configured list stays fixed. Go's TCP dialer resolves hostname entries when
opening a connection; there is no periodic DNS refresh of the list. Python's
grpcio implementation instead expands multi-address hostnames to IP addresses
once at channel creation.

Reporters wait for a READY channel before taking the next queued batch. Buffers
are bounded, so new reports are dropped when an outage fills them. A failed
batch is discarded and **never replayed**: the backend may already have accepted
it. Automatic gRPC retries apply only to the idempotent
`reportInstanceProperties` RPC on `UNAVAILABLE`, with at most three attempts.
Instance properties are refreshed every ten heartbeat intervals so a standby
also learns the metadata after a silent backend switch. A properties-report
failure does not suppress the heartbeat.

RPC deadlines and stream cancellation bound individual operations. RPC errors,
including authentication rejection or resource exhaustion, do not recreate the
channel or rotate a connected backend. TCP keepalive helps detect failed
transports; the agent does not enable aggressive HTTP/2 keepalive pings, which
can conflict with OAP's gRPC ping policy. A half-open connection may remain READY
until the transport detects the failure, so an RPC deadline alone does not
guarantee immediate failover.

The `grpc-multi-backend` E2E case discovers the active collector with a unique
probe, stops it, and checks trace and log growth on the standby. Unit tests also
cover failover, queue retention, failed acknowledgements, and send panics.
`ConnectionManager.ResolvedBackendAddresses()` exposes the published dial order
for tests and diagnostics.

## HTTP(S) proxy

Multi-address connections bypass `HTTP_PROXY` / `HTTPS_PROXY`. Provide a direct
network route to the OAP ports. The single-address path retains its existing
proxy behavior.

## TLS / SNI

The first configured endpoint after normalization supplies the channel's fixed
HTTP/2 `:authority`, including its port. TLS uses that endpoint's host for
certificate verification and SNI where applicable. Shuffling or failing over
does not change this identity. An explicit server-name override in transport
credentials takes precedence.

Every backend must present a certificate valid for this common identity. With
`oap.example.com:11800,10.0.0.2:11800`, both servers need a certificate covering
`oap.example.com`. With an IP address first, certificates must cover that IP in
their SANs; a hostname later in the list does not replace it. Put the intended
common hostname first when using DNS certificates.

See [Transport Layer Security (TLS)](./grpc-tls.md).

## Authentication failures

`UNAUTHENTICATED` / `PERMISSION_DENIED` are not retryable in the multi-backend
service configuration. A bad shared credential will not usefully rotate across
backends. The connection interceptor emits an authentication diagnostic at most
once per 30 seconds and keeps the existing channel. Check
`reporter.grpc.authentication` and the OAP authentication configuration.
