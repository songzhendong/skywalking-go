# gRPC Backend Connectivity Troubleshooting

This page covers multi-address failover, TLS SNI, proxies, and authentication —
topics that often look like “agent bugs” but are configuration or environment
issues.

## Multi-address failover

Configure comma-separated backends:

```yaml
reporter:
  grpc:
    backend_service: oap-a:11800,oap-b:11800
```

The agent publishes the list to gRPC `pick_first` (same idea as the Node.js
`sw-static` target). Entries are **literal** `host:port` endpoints. The first
reachable address is used; unreachable entries are skipped.

**Checks**

- E2E: `test/e2e/case/grpc-multi-backend` identifies the active collector via a
  unique `/sw-failover-probe/{token}` span, kills it, and asserts standby
  `/receiveData` shows growing `POST:/info` and toolkit log text (Node
  `remote-e2e/static-failover`). Multi-backend Collect Sends are time-bounded
  so half-open peers after kill cannot stall the pipeline.
- Unit tests cover refused-first dial and mid-flight listener stop → Ready again.
- In-process: `ConnectionManager.ResolvedBackendAddresses()` returns the last
  dial targets published to gRPC (for tests/diagnostics; Node exposes a similar
  list via `/debug/resolved-backends`).

## HTTP(S) proxy

Multi-address dials **bypass** `HTTP_PROXY` / `HTTPS_PROXY` (Node sets
`grpc.enable_http_proxy=0`). Agent → OAP through an HTTP proxy is unsupported
for that path. If a corporate proxy is required, terminate TLS or route outside
the proxy for OAP ports.

## TLS / SNI

- Prefer hostnames in `backend_service` when using TLS.
- Mixed lists such as `10.0.0.1:11800,oap.example.com:11800` reuse the first
  hostname (`oap.example.com`) as `ServerName` for IP literals so SNI still
  matches the certificate (Node `firstHostnameAuthority`).
- All-IP lists with TLS need certificates that accept IP SANs, or add a hostname
  entry for SNI.

See [Transport Layer Security (TLS)](./grpc-tls.md).

## Authentication failures

`UNAUTHENTICATED` / `PERMISSION_DENIED` are **not** retryable in the multi-backend
service config. A bad shared credential will not usefully rotate across
backends. The agent logs these failures at most once per 30 seconds and asks
you to check `reporter.grpc.authentication` — same intent as Node’s “auth
rejection: throttled log, no backend rotation”.

## Quick checklist

| Symptom | Likely cause |
|---------|----------------|
| Never reaches OAP with multi-address | First entries hang (blackhole IP); prefer connection-refused standbys in tests |
| TLS handshake fails on IP backends | Missing hostname for SNI / cert SAN |
| Proxy oddities | HTTP_PROXY interfering; multi-address path already bypasses proxy |
| Auth errors looping | Wrong token; not a failover problem |
