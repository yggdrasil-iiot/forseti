# forseti

forseti puts one principle into executable form: once a part has a durable identity, measurements late-bind to that identity, and the delays and corrections of asynchronous uplinks are reconciled. Stage 1 (attribution) joins process samples with station occupancy by event time and emits one work record per unit visit with per-tool evidence; stage 2 reconciles late quality incidents into the affected units.

forseti has no write path to the plant. Applications of the yggdrasil family couple only through published data and wire contracts, with zero shared code.

## Where forseti applies

(a) The tool controller stamps the unit ID, the visit ID and a visit-present signal in the same message as the measurement. forseti then attributes without a time join (the `plc-stamped` rule) and may cross-check occupancy without changing the attribution.

(b) MES already provides process history and affected-unit queries in time. forseti is not needed.

(c) Record keepers are separate (tool controller, inspection, MES) and an updated list is needed before the next transfer. This is where forseti applies.

## Repository layout

- `core/`: forseti-core, Java 17. Value types, RFC 8785 canonical digests, the release reader, rule and binding-profile loading from a Bifrost-published release, the two stage decision functions as pure functions, ProgressTracker. It depends on no engine and never on `cli/`.
- `cli/`: the `forseti` command line. `generation new|promote|retire|list`, the reference harness that drives the core through cut barriers without an engine, the recording builder and synthesizer, event-time replay, and `forseti gate --engine core`.
- `oracle/`: an independent Python oracle that recomputes every live row from scratch at every cut by sort-and-sweep. It shares no code with the Java modules; the two meet only at the JCS vector file, the recordings, the cut log, the released bytes and the journal files.
- `gate/`: the assertion manifest `core-assertions.json` (82 assertions), the shard plan `core-shards.json`, and the negative-control plan `core-negative-controls.json`.
- `fixtures/`: the registry fixtures, the scenario files with their locks, and the three committed smoke recordings.
- `vectors/jcs-vectors.json`: the canonical-form vectors both test suites run. `schemas/`: the occupancy and incident schemas.
- `scripts/`: `gate-then-promote.sh`, `check-control-report.py`, `check-ascii-names.sh`, `check-dependency-closure.sh`.
- `docs/CORE-GATE.md`: the assertion table, scenarios, formats and controls. `docs/superpowers/`: the design spec and the delivery plans.

## Running the core gate

```bash
mvn -B -ntp install
python -m pip install -e "oracle[dev]"
python -m pytest -q oracle
java -Dforseti.gate.horizonXmx=6g -jar cli/target/forseti.jar gate --engine core --out build/gate --state build/gate/state --python "$(command -v python)"
```

The last line is `assertions run 82 / expected 82` and the exit code is 0 only when every assertion ran and held. The gate builds every recording it does not find committed, verifies each against its committed lock, runs the reference harness, and compares the harness journals with the oracle at every completed cut of every scenario. One shard of the plan runs with `--shard <name>`; `--aggregate <dir>` evaluates the cross-scenario assertions over the shard reports. The lifecycle through the CLI:

```bash
bash scripts/gate-then-promote.sh build/gate-e2e "$(command -v python)"
```

It runs the gate, shows that promotion refuses an unaccepted core scope, promotes the base generation of `horizon-planned-late` and then its event-time replay generation, which retires the base, and checks that the base journal bytes are unchanged.

## Negative controls

Eight controls prove the gate's assertions can fail: `processing-time`, `drop-late`, `no-cascade`, `lenient-oracle`, `first-wins`, `station-granularity`, `ignore-session-gap`, `wrong-rules-pin`. A control runs on the shards that own or feed its intended assertions, with the same manifest, shard plan, fixtures, seed and jar, and must exit 1 with its intended assertions failed; a crash or a missing run never counts as the intended failure, and a control report can never authorize a promotion. Example:

```bash
java -jar cli/target/forseti.jar gate --engine core --negative-control first-wins --shard hand-made --out build/controls/first-wins/hand-made --python "$(command -v python)"
python -I scripts/check-control-report.py build/controls/first-wins/hand-made/report.json $? first-wins hand-made
```

`docs/CORE-GATE.md` lists each control's mutation, shards and intended failures.

## Fixtures and their provenance

`fixtures/registry.lock.json` pins the Bifrost and Tyr commits the registry fixtures were published from and the raw SHA-256 of every fixture file; `fixtures/upstream/manifest.json` pins the Tyr commit and digests of the upstream bundle. The registry fixtures are a Bifrost-published release; forseti reads them through its release reader and never edits them. Scenario files and their locks are committed; recordings other than the three smoke recordings are built deterministically at gate time and checked byte for byte against their locks.

## Pins

Java 17 (CI: Temurin 17.0.21), Python 3.13 (CI: 3.13.18), Jackson 2.17.2, java-json-canonicalization 1.1, Python rfc8785 0.1.4, jsonschema 4.26.0. Every harness run records its seed, fixture digests, pins, JCS library versions and offset ranges in `run.json`; every gate report records the manifest digest, the shard plan digest and the pins.

## Step 3 completion gate

Delivery step 3 is complete when, on main, the six gate shards and the aggregate end with `assertions run 82 / expected 82`, the base and replay generations are promoted through the CLI from that aggregate report, and each of the eight negative controls fails its intended assertions; the controller runs these commands and keeps the logs.

## Scope of the core gate

The core gate establishes the engine-independent semantics: attribution, cascade, corrections, generations and event-time replay, compared with the oracle at every cut. Native event time in an engine, recovery, exactly-once delivery, broker retention, transaction visibility, isolation, bridge classification and engine eligibility are established by the engine gates of delivery steps 5 to 8.
