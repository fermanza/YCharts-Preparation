# YCharts technical-screen preparation

The supplied Terminal.io email says the next screen lasts 60 minutes and combines live string coding with a discussion of your Python/backend experience. It also says live interview coding must be manual, without AI assistance. Use this project to rehearse explanations and pipeline tradeoffs beforehand; practice the coding exercises without hints or generated code.

The email describes a senior backend seat on a four-person data team and asks for 5+ years of professional experience, whereas the pasted public job description says 2+ years of industry or academic experience. Treat the email's senior scope as the preparation target and clarify the specific level with the recruiter. State your actual experience accurately. The email's broader fullstack, AWS, relational-database, and English-communication signals are preparation topics, not reasons to claim experience you do not have.

The project is an interview practice artifact using fictional securities and synthetic prices. It is not work performed for YCharts and does not establish production experience at its scale. Your strongest background examples should be 2–3 genuine professional projects with specific individual contributions.

## Seven-day preparation plan

| Day | Focus | Concrete output |
| --- | --- | --- |
| 1 | Understand the data flow and tell your background | Run the CLI demo, inspect batches/metrics, prepare a 90-second introduction, and select three real IC stories. |
| 2 | Python and sliding windows | Solve longest substring without repetition aloud; rehearse dictionaries, sets, iterator/generator behavior, exceptions, and complexity. |
| 3 | Strings and normalization | Solve valid palindrome and group anagrams; explicitly discuss case, punctuation, Unicode, and output ordering. |
| 4 | In-place updates and harder windows | Solve string compression and minimum window substring; trace invariants and edge cases before coding. |
| 5 | Django and pipeline correctness | Explain models, transactions, locks, revisions, source/formula versions, retries, Airflow mapping, and Celery repair. Replay and correct the demo data. |
| 6 | Timed rehearsal | Run the 60-minute mock below without AI. Record where explanation, implementation, or testing became unclear. |
| 7 | Focused review | Repeat the weakest exercise from a blank file, tighten the project walkthrough, rehearse IC stories, and prepare three interviewer questions. |

If only a few hours remain, spend 40 minutes on your introduction and two authentic IC stories, 60 minutes on longest substring and compression, 30 minutes on the minimum-window invariant, and 30 minutes walking through ingestion, correction, and freshness. Finish with one timed 60-minute mock. This prioritizes the screen described in the email; graphs/trees are useful follow-up practice if time remains.

## Five string-coding drills

For each problem, restate the contract, ask about constraints, write examples, describe a simple solution, improve it if needed, code, test, and state time/space complexity. Ask clarification questions to the interviewer during the actual screen; for practice, choose and state an explicit contract yourself.

| Problem | Contract and useful tests | Expected reasoning |
| --- | --- | --- |
| Longest substring without repeating characters | Return the length of a contiguous substring. Test `"" → 0`, `"a" → 1`, `"abba" → 2`, `"pwwkew" → 3`. Decide whether case is distinct and what a character means. | Sliding window with last-seen positions; move the left boundary forward only. O(n) time; O(k) space for distinct characters. |
| Valid palindrome | Decide which characters count and whether comparison is case-insensitive. Under ASCII alphanumeric normalization, test empty string, punctuation-only text, `"A man, a plan, a canal: Panama"`, and `"race a car"`. | Two pointers, skipping ignored characters. O(n) time and O(1) extra space under that explicit character policy. Python Unicode casing can expand characters, so a broader policy needs care. |
| Group anagrams | Group words by equal character multiplicities; clarify output order and case. Test `[]`, `["", ""]`, duplicates, and `["eat", "tea", "tan", "ate", "nat", "bat"]`. | Sorted-word key costs O(sum L log L). A fixed-alphabet count tuple costs O(sum L) with alphabet-size overhead, but only under a constrained alphabet contract. |
| String compression | Mutate a list of characters and return the used prefix length; emit counts only when greater than one. Test `[]`, `["a"]`, `list("aabbccc") → list("a2b2c3")`, and a run of 12 `a`s. | Read/write pointers and run counts, including multi-digit counts. O(n) time; O(1) auxiliary space excluding count conversion. Verify the used prefix, not the stale tail. |
| Minimum window substring | Find the shortest contiguous substring containing every target character with multiplicity. Test `("ADOBECODEBANC", "ABC") → "BANC"`, `("a", "aa") → ""`, `("aa", "aa") → "aa"`, empty target, and no solution. | Track required multiplicities, expand until covered, then shrink while covered. O(n+m) time; O(k) tracking space, plus the returned substring. State tie behavior. |

For minimum window, explain precisely when a required character becomes satisfied and unsatisfied. Counting distinct characters in the window is insufficient when the target repeats a character. For longest substring, `abba` catches a left pointer that incorrectly moves backward.

After solving a drill, close the solution and repeat it the next day from memory of the invariant rather than memory of the code. Add one unseen test. Keep a short log of mistakes such as stale counters, incorrect boundaries, premature optimization, or missed empty inputs.

The exercise stubs are in `practice/strings.py`; reference implementations are in `practice/solutions.py`. `uv run pytest tests/test_practice.py` checks the reference implementations by default. To check your own answers, change that test module's import from `practice.solutions` to `practice.strings`. Do not count a passing reference suite as evidence that you independently solved the exercises.

## A ten-minute project walkthrough

1. **Minute 0–1: Product need.** “I built a small practice pipeline to show how corrected provider prices can invalidate derived analytics and how operators restore trustworthy results.” State that the data is synthetic.
2. **Minute 1–3: Data model.** Explain `Security`, canonical `PriceBar`, auditable `ImportBatch`, and versioned `MetricSnapshot`. Explain decimal source prices versus floating-point analytics.
3. **Minute 3–5: Ingestion contract.** Demonstrate safe replay, outdated revisions, equal-revision conflicts, and higher-revision corrections. Explain whole-file validation and atomic source updates.
4. **Minute 5–7: Orchestration.** Show resolve → ingest → symbol chunks → mapped calculations → verify. Explain full-history windows, small XCom handoffs, and why retrying each stage is safe. Mention explicit Celery repairs.
5. **Minute 7–9: Publication and tradeoffs.** Show `409` for stale versions or mismatched dates in the requested slice and current results after repair; no available snapshots return `404`. Contrast the bounded API check with Airflow's full-history quality gate. Discuss PostgreSQL locks, SQLite's local scope, conservative version invalidation, and `as_of` limitations.
6. **Minute 9–10: Production evolution.** Explain that three securities are not a scale benchmark. Propose measuring throughput/lock time and introducing manifests, exchange calendars, object storage, and formula dependencies as requirements justify them.

Useful commands from the repository root:

```bash
uv run python manage.py demo_pipeline
uv run python manage.py demo_pipeline --as-of 2026-10-09
uv run python manage.py run_partition --date 2026-10-09
uv run python manage.py ingest_prices PATH_TO_CSV --date 2026-10-09
uv run python manage.py recalculate --symbols ALFA BETA --as-of 2026-10-09
uv run python manage.py recalculate --symbols ALFA BETA --as-of 2026-10-09 --async
```

The asynchronous command requires a running broker and Celery worker. Use the README startup instructions for the local environment; do not describe an enqueued task as completed until the worker and freshness verification confirm it.

API examples after starting Django:

```text
/health/
/api/securities/
/api/batches/
/api/securities/ALFA/metrics/?as_of=2026-10-09&limit=3
```

Read [architecture.md](architecture.md) for the precise contracts and [implementation-plan.md](implementation-plan.md) for review/ticket examples. Describe the actual implementation you can point to, and distinguish a design proposal from a feature already implemented.

## Prepare 2–3 authentic IC stories

Choose a feature delivered end to end, a reliability/debugging incident, and an architecture or performance tradeoff from your actual work. Use a 2-minute structure:

- **Context:** Who needed the feature or fix, and what constraint mattered?
- **Your responsibility:** What did you personally own, implement, review, investigate, or decide? Distinguish your work from the team's work.
- **Technical choice:** What alternatives did you consider? What did your choice improve, and what cost did it introduce?
- **Evidence:** What changed? Use measured latency, error rate, delivery time, or operational impact only when you can substantiate it. Qualitative evidence is preferable to invented numbers.
- **Maintenance:** How was it tested, released, observed, and repaired afterward? What would you change now?

Fill in a simple worksheet before rehearsal:

| Story | My concrete contribution | Alternative/tradeoff | Evidence | Lesson |
| --- | --- | --- | --- | --- |
| Delivered backend feature |  |  |  |  |
| Reliability or incident response |  |  |  |  |
| Performance/schema/design decision |  |  |  |  |

If your production background uses a framework other than Django, say so and map real concepts to this project: routing/views, models, migrations, transactions, validation, worker tasks, and tests. If you used AI while preparing this artifact, explain the assistance honestly and be ready to reason about every design and code path yourself.

## Sixty-minute mock interview

Ask a practice partner to select a string problem without revealing it beforehand. Solo practice also works with a randomly selected drill, a timer, and a recording.

| Time | Activity | What to observe |
| --- | --- | --- |
| 0–5 min | Introduction and recent experience | Concise, accurate scope; specific personal responsibility. |
| 5–10 min | One real project and follow-ups | Architectural choice, maintenance, and evidence. |
| 10–15 min | Coding contract and examples | Clarifies ambiguous inputs; identifies tricky boundaries. |
| 15–35 min | Live implementation | Explains invariant, writes readable Python, and manages time. |
| 35–43 min | Tests and follow-up | Traces adversarial cases; corrects a discovered bug; explains complexity. |
| 43–53 min | Django/pipeline discussion | Transactions, revisions, retries, freshness, and realistic scale reasoning. |
| 53–60 min | Your questions and wrap-up | Thoughtful questions grounded in this team's work. |

This schedule is a rehearsal format, not a prediction of the interviewer's exact agenda. Score each area from 0–3: 0 = absent, 1 = unclear or heavily assisted, 2 = correct with minor gaps, 3 = clear and independent.

| Area | A strong signal |
| --- | --- |
| Requirements | States input/output behavior, multiplicity, empty inputs, normalization, and ties where relevant. |
| Algorithm | Explains why each boundary/counter update preserves the invariant. |
| Python | Uses appropriate containers, clear names, and manageable functions without obscure shortcuts. |
| Testing | Includes adversarial tests, catches bugs, and distinguishes expected from actual output. |
| Complexity | Gives time and auxiliary-space costs with alphabet/input assumptions. |
| Communication | Thinks aloud in short explanations; responds to prompts; avoids narrating every keystroke. |
| Backend judgment | Explains transaction/freshness failure cases and the cost of consistency. |
| IC ownership | Separates personal contributions from team outcomes and gives truthful evidence. |

The score is for identifying preparation gaps, not estimating hiring probability. Repeat weak areas, then do another complete mock to verify improvement under time pressure.

## Follow-up questions to rehearse

- Why does an equal-revision price change fail rather than overwrite the stored row?
- What happens if a worker dies after ingestion but before calculation? After calculation but before final verification?
- Why do rolling metrics need historical observations beyond the current daily partition?
- How does the API compare the latest three snapshot dates with observed price dates, and which missing dates are checked only by Airflow's full-history quality gate?
- What does `as_of` mean after the provider corrects a historical price?
- What does a PostgreSQL lock protect, and what is the throughput cost of holding it during computation?
- Why is SQLite useful locally, and what does it fail to prove about concurrency?
- How would you detect a provider that omitted a security without producing a malformed row?
- How would you size tasks for 200,000 securities? What would you measure before choosing concurrency and chunk size?
- When would you choose Celery over an Airflow DAG, and how would you observe completion?
- What happens when a metric formula changes while prices stay the same?
- How do you explain stale analytics to an advisor without implementation jargon?

Other useful refreshers: Django QuerySets are lazy; avoid N+1 reads; use constraints as well as validation; identify transaction boundaries; understand mutable default arguments, hashability, generators, decorators, exception propagation, and the difference between I/O concurrency and CPU parallelism. For graphs, rehearse BFS/DFS, visited sets, disconnected components, and cycles if time permits.

## Questions for the interviewer

Choose two or three and ask them naturally:

- “Where does the data team spend the most operational effort today: provider delays, corrections, calculation performance, or another issue?”
- “How do you track freshness and decide whether a data point is safe to show to users?”
- “What would strong ownership look like for this role in its first three months?”
- “How do engineers and product decide the definitions and edge cases for a new financial metric?”
- “How do code review and the daytime on-call rotation work on the smaller data team?”

Before the screen, rehearse your introduction, two IC stories, the five drills, and the ten-minute walkthrough. During live coding, follow the email's no-AI requirement. After the interview, follow up with Terminal.io as requested in the email.
