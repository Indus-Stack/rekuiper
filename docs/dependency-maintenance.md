# Dependency maintenance

## Audit and update cadence

- CI checks the workspace lockfile and an isolated core/SQL consumer. Run the same checks with `python3 .github/scripts/dependency-audit.py workspace` and `python3 .github/scripts/dependency-audit.py consumer`. Both require Cargo and cargo-deny. Use `--offline` with a current locally cached advisory database.
- The daily Actions audit checks main, dev, and the highest supported release tag even when no code changes. GitHub Actions failure notifications and the job summary report the impacted ref, advisory, crate version, and dependency paths. The owner in CODEOWNERS reviews new failures within one working day.
- Review weekly dependency update PRs by family. For a transitive package whose parent prevents an upgrade, record the constraint and track the parent release. Keep major storage, TLS, and connector changes independently reviewable.
- Review critical dependencies monthly. Start with storage, TLS, Kafka, MQTT, RabbitMQ, Arrow/Parquet and input parsers. Check deprecation announcements, archived repositories, maintainer handoffs, delayed upstream security fixes, and constraints that block versions. Open a migration ticket for an explicit deprecation or a blocked fix and attach the reverse dependency tree and release impact.

## Exception review

Every RustSec exception must have an owner, dependency path, affected profiles, rationale, tracking reference, and expiry. Confirm the affected version and features for each shipped target before granting or renewing an exception. Remove entries when the affected version leaves the graph. An expired entry fails the workspace policy check.

### Chrono

Track connector-side Arrow/Parquet and rskafka in [issue #66](https://github.com/Indus-Stack/rekuiper/issues/66). Rekuiper code uses Jiff and its existing clock. The standalone consumer cannot resolve Chrono, chrono-tz or chrono-tz-build. Re-evaluate the workspace exception as soon as Parquet and Kafka release compatible alternatives. Do not accept new first-party usage or new external dependency paths.

### MQTT and TLS

In [issue #68](https://github.com/Indus-Stack/rekuiper/issues/68), track RUSTSEC-2026-0049, RUSTSEC-2026-0098, RUSTSEC-2026-0099, RUSTSEC-2026-0104, RUSTSEC-2026-0382 and RUSTSEC-2025-0134 through rumqttc and Rustls. The Rustls transport feature in rumqttc 0.25.1 couples the older WebPKI and PEM dependencies to the selected connector TLS implementation. The resolved workspace uses WebPKI 0.102.8 on the MQTT path and 0.103.17 elsewhere. Recheck both versions and the PEM parser after the next update. Rekuiper configures the MQTT verifier without CRLs. The signed-certificate and trust-chain risks remain applicable when a broker chain meets the advisory conditions. Verify TLS, mutual TLS, session reconnect and native certificate handling before removing exceptions.

### Kafka and compression

In [issue #68](https://github.com/Indus-Stack/rekuiper/issues/68), track RUSTSEC-2026-0391, RUSTSEC-2026-0398 and RUSTSEC-2026-0403 through rskafka 0.6.0 and Zstd 0.13. Preserve codec support and verify compressed producer and consumer traffic before accepting a replacement. Re-evaluate when the Kafka client can resolve the patched compression stack.

### Build and executor

Lapin 4 removed the legacy instant path. Wasmi 1.1 and Boa 0.22 removed the remaining paste edges. Verify RabbitMQ reconnect and acknowledgements after executor updates and test WASM fuel limits and UDF behavior after interpreter changes.

## Release and downstream handoff

Verify the dependency and SQLite version inventories in the release archives. Verify SPDX attestations for the Docker images and test named SQL timezone lookups inside the shipped image. Publish the matching core and SQL crates through the existing tagged release pipeline. In indramqtt, update the Rekuiper core pin and vendored SQL copy together, resolve its own lockfile, and rerun its advisory check before dropping the three temporary Chrono ignores.
