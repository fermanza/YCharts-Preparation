# Implementation plan and review tickets

The product requirement is: “An advisor should see metrics derived from the current accepted provider prices, and an operator should be able to explain and repair a failed update.” The deliverable is a small, inspectable Django/Airflow/pandas/Celery pipeline using synthetic prices.

The estimates below are planning examples for one engineer familiar with the stack, with an available reviewer. They describe a scoped implementation, not a claim about time already spent or a promise to support the production YCharts universe. Dependencies, provider ambiguities, and integration environments can change them.

## Scoped tickets

| Ticket | Estimate | Dependencies | Acceptance criteria |
| --- | --- | --- | --- |
| 1. Define provider and metric contracts | 0.5–1 day | Product and provider decisions | Document dates, currency, price precision, revision ordering, missing-data behavior, metric warm-up windows, and observation-cutoff semantics. |
| 2. Create source and audit models | 1 day | Ticket 1 | Migrations create unique security/date observations, positive revisions, decimal prices, batch audit fields, and source/formula versions. Constraint violations do not silently corrupt data. |
| 3. Build atomic CSV ingestion | 1–2 days | Ticket 2 | Validate whole files, handle lower/equal/higher revisions deterministically, reject equal-revision conflicts without partial source updates, and provide explainable batch counters. |
| 4. Calculate versioned metrics | 1–2 days | Ticket 3 | Compute returns, SMA, sample volatility, and running drawdown from full adjusted-price history through the cutoff; preserve null warm-up values; publish only with the expected source version. |
| 5. Add read API and freshness checks | 0.5–1 day | Ticket 4 | Valid requests return JSON; unknown securities or unavailable metrics are `404`; otherwise returned snapshot dates must match the latest observed price dates through the cutoff, within a limit of 1–100, and stale/date-mismatched results are `409`. |
| 6. Add Airflow orchestration | 1–2 days | Tickets 3–5 | Resolve input, ingest, map calculations over symbol chunks of 100, join, and verify. Retries are safe; XCom carries small handoffs rather than files or DataFrames. |
| 7. Add explicit Celery repairs | 0.5–1 day | Ticket 4 | A CLI request enqueues a bounded recalculation using the shared service; worker failures remain observable; Django initialization has no implicit Celery imports. |
| 8. Package the demo and operating guide | 1 day | Tickets 1–7 | Provide deterministic synthetic files, replay/correction walkthrough, startup instructions, interview notes, and verification of the documented interfaces. |

Total planning range: roughly 7–11 engineer days, plus review and integration time. Keep a working vertical slice after ticket 5, then add orchestration and asynchronous operations. The completed repository can be smaller than a production implementation while still demonstrating those contracts.

## Test cases that protect the product behavior

These are review requirements, not a test-count report:

- A bad final CSV row leaves all canonical prices unchanged, while preserving a failure diagnostic.
- A repeated file and a differently formatted equivalent row do not duplicate observations or advance source versions unnecessarily.
- An outdated revision cannot undo a newer correction. An equal-revision value conflict rejects the entire source update.
- A correction makes the API reject old metrics; recalculation restores it and changes the affected calculation as expected.
- Metric assertions check actual expected values, including the first return, SMA warm-up, 20-return sample volatility, and drawdown recovery.
- The API rejects a date mismatch or stale version within the requested latest-date slice, returns `404` when no snapshots are available, and bounds checks to the requested limit. Airflow's final verification detects missing historical dates outside that slice.
- An observation cutoff excludes later dates but still uses all earlier available observations for rolling windows.
- Airflow parsing/import and an end-to-end local DAG execution exercise actual task wiring and joins.
- PostgreSQL integration checks concurrent ingest/publication behavior; SQLite is used for local behavior, not row-lock claims.
- A real Celery worker demonstrates the asynchronous repair path where the local environment supports it; eager execution alone does not demonstrate queue delivery.

## Code review checklist

Begin with the failure scenario and contract, then inspect the implementation:

1. **Atomicity:** Can a malformed row or revision conflict leave a partial canonical update? Does a recorded failed batch accurately describe the outcome?
2. **Identity:** Are symbol/date uniqueness and currency assumptions explicit? Are decimals normalized before equality comparison?
3. **Versions:** Do canonical changes advance `data_version`, and do snapshots carry both source and formula versions?
4. **Locks:** Do all PostgreSQL paths acquire security locks in the same order? Can an API response race a correction?
5. **Calculation:** Is history loaded through the cutoff rather than limited to a daily file or API result count? Are unavailable values preserved as null?
6. **Orchestration:** Does verification wait for every mapped task? Are retries idempotent? Are paths usable by every worker in the actual deployment?
7. **Operations:** Is there an actionable error, repair command, and verification step for each failure class?

Do not approve concurrency or scale claims solely because unit tests pass. Request the integration environment and workload evidence relevant to the claim.

## Product and operations communication

An example explanation to a nontechnical stakeholder:

> The provider corrected a historical price. Until we rebuild the affected calculations, the API reports that the metrics need refreshing. This prevents an advisor from presenting an outdated result as current. Once recalculation completes and our completeness check passes, the current results become available again.

An example incident/update note:

> The import for the requested date failed validation because one observation changed without a newer provider revision. No prices from the file were partially accepted. We need a corrected provider revision or a confirmed provider contract change, then a replay and freshness verification.

For a real team, attach the provider issue, batch identifier, affected securities, current freshness lag, owner, and next update time. Avoid inventing impact metrics before measuring them.

## Maintenance and expansion decisions

| Proposed change | Decision needed before implementation |
| --- | --- |
| More securities or metrics | Measure calculation time, database I/O, lock contention, and memory per chunk; use these to set worker parallelism and batch size. |
| Expected-universe checking | Establish an authoritative manifest and exchange calendar so omissions can be distinguished from legitimate no-data cases. |
| Historical knowledge queries | Add versioned observations and knowledge/effective timestamps; `as_of` alone cannot answer “what was known then?” |
| Shorter publication locks | Compute optimistically and compare source versions before publication, with bounded retry behavior. |
| Multiple providers or currencies | Define reconciliation priority, identifiers, FX conversion date, and adjustment semantics before merging observations. |
| Thousands of metric definitions | Introduce formula registry/versioning and dependency tracking; avoid claiming that a four-metric demo validates that scale. |

Use a measured follow-up ticket for each expansion. Ownership includes documenting the contract, implementing it, arranging review, observing the rollout, and maintaining the recovery path.
