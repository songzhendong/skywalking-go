# Periodically Resolve Backend DNS

When the backend service is configured with a domain name, enable
`reporter.grpc.resolve_dns_periodically` so the agent periodically resolves the
domain and updates gRPC receiver addresses.

The resolving interval is `reporter.grpc.resolve_dns_period` (seconds). If it is
`0` (default), the agent uses `reporter.check_interval`. A negative period is
rejected as a configuration error.

Unlike the Java agent (`collector.is_resolve_dns_periodically`), which
re-resolves DNS mainly when reconnecting, the Go agent refreshes DNS on the
configured interval while the reporter is running. On a temporary DNS lookup
failure, the agent keeps the last successfully resolved addresses.

Connection setup is not blocked on DNS: the agent first dials using the
configured hostname, then refreshes to resolved IPs asynchronously. Each DNS
lookup is time-bounded (at most 5 seconds, and never longer than the resolve
period).

| Name                                   | Environment Variable                              | Description                                                                                         |
|----------------------------------------|---------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| reporter.grpc.resolve_dns_periodically | SW_AGENT_REPORTER_GRPC_RESOLVE_DNS_PERIODICALLY   | Enable periodically resolving backend service DNS and updating gRPC addresses.                      |
| reporter.grpc.resolve_dns_period       | SW_AGENT_REPORTER_GRPC_RESOLVE_DNS_PERIOD         | DNS resolve period in seconds. `0` means use `reporter.check_interval`. Must not be negative.       |
