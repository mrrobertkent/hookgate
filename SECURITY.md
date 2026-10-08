# Security policy

hookgate sits in front of systems that trust what it forwards, so security reports get priority.

## Reporting a vulnerability

Report privately through [GitHub private vulnerability reporting](https://github.com/mrrobertkent/hookgate/security/advisories/new). Do not open a public issue for a vulnerability.

You can expect an acknowledgement within 3 working days and an assessment within 10. Fixes ship as a patch release with a GitHub security advisory; reporters are credited unless they ask not to be.

## Supported versions

Only the latest minor release receives security fixes.

## Scope

In scope: anything that lets a request reach an upstream without passing its source's checks, leaks a secret (in logs, metrics, responses or timing), or lets one source affect another. Out of scope: denial of service by volume alone (put a rate limit in front of hookgate) and weaknesses of a sender's own signature scheme.
