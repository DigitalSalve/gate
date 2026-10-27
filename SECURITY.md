# Security policy

## Reporting a vulnerability

Please report security problems privately, not in a public issue or pull request.

Use **Report a vulnerability** on this repository's Security tab. The report reaches the maintainers only, and the advisory stays private until a fix is out.

Include what you found, how to reproduce it, and what an attacker could do with it. You will get an answer within seven days.

## Supported versions

Only the current `main` branch is supported. There are no release branches; fixes land on `main`.

## Scope

In scope: the gate (`src/`), the meter package (`packages/meter/`), and their configuration example. Of particular interest is anything that lets one caller read another caller's deduplicated response, bypass the hold cap, or spend another caller's tokens.

Out of scope: the reverse proxy, the runtime and the services behind the gate. Report those to their own maintainers.
