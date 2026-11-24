# Security policy

hookgate sits in front of systems that trust what it forwards, so security reports get priority.

## Reporting a vulnerability

Report privately through [GitHub private vulnerability reporting](https://github.com/mrrobertkent/hookgate/security/advisories/new). Do not open a public issue for a vulnerability.

You can expect an acknowledgement within 3 working days and an assessment within 10. Fixes ship as a patch release with a GitHub security advisory; reporters are credited unless they ask not to be.

## Supported versions

Only the latest minor release receives security fixes.

## Scope

In scope: anything that lets a request reach an upstream or a JetStream stream without passing its source's checks, or with bytes other than the ones verified; makes a JetStream source answer `2xx` before the stream has acknowledged the event; leaks a secret (in logs, metrics, responses, published headers or timing); lets one source affect another; or makes `hookgate-relay` give a handler a body that fails its SHA-256 check, deliver a replay to a consumer other than the one it names, deliver an event outside the consumer's configured filters, or drop an event without a dead-letter record. Out of scope: denial of service by volume alone (put a rate limit in front of hookgate) and weaknesses of a sender's own signature scheme.
