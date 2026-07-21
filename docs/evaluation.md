---
title: Retrieval evaluation
description: Run the deterministic retrieval regression suite and interpret its limits.
---

# Retrieval evaluation

Memoree ships a deterministic retrieval regression runner and a small versioned starter corpus. It exists to protect the agent-facing `memory.recall`, raw `search`, and `context.build` contracts—not to claim that FTS5 is universally superior.

Run it from the repository root:

```sh
cargo run --locked --bin memoree-eval -- eval/corpus/v2 \
  --recovery-only \
  --case-timeout-ms 60000 \
  --suite-timeout-ms 600000 \
  --jobs 1 \
  --prewarm-models \
  --timings-json /tmp/memoree-timings.json \
  --pretty
```

Use `--case CASE_ID` to isolate one failure. The per-case value is a soft budget checked after a case finishes; a completed over-budget case produces a failing report with `timed_out_case` and `timed_out_stage`. The suite value is also a hard process deadline covering corpus loading, model setup, and every selected case. A hard timeout exits with status 124 and deliberately writes no report or timings file because a trustworthy partial result cannot be recovered from interrupted model work. Version 0.6 deliberately permits only one worker so model execution and ranking remain reproducible.

The runner creates a fresh temporary Memoree store, loads the corpus only through normal protocol mutations, executes every case through `memory.recall`, `search`, and `context.build`, prints one JSON report, and exits nonzero on a hard invariant or committed-baseline regression. It never opens the user's configured Memoree data directory. The hard watchdog assumes model work remains in-process; a future evaluator that launches child processes must terminate their process group too. Mutable run state remains inside the temporary store, and external report/timing files are written only after a complete result, so abrupt timeout cannot leave a plausible partial report.

Each versioned corpus directory contains:

- `seed.jsonl`: labelled artifacts, grounded claims, and lifecycle relations in creation order. Evidence may name a unique exact quote; the runner computes and stores its byte span.
- `cases.jsonl`: ambient context, query, optional explicitly justified horizon, relevant/forbidden labels, expected three-state recall presence, expected conflicts, and context byte budget.
- `probe-recovery.json`: frozen original/reformulated query pairs, selected source labels, exact-fetch expectations, and refined recall checks for the bounded paraphrase-recovery path.
- `baseline.json`: the reviewed aggregate recall/precision result for that corpus version and the allowed regression epsilon.

Hard checks cover:

- `claims`, `artifacts_only`, and `none` presence semantics;
- relevant and forbidden entity labels;
- open conflict surfacing;
- exact evidence revision/span resolution and excerpt round-trip;
- recall/search citation parity;
- ambient/workspace scope containment;
- candidate suggestion recall without changing qualified presence;
- candidate scope, lifecycle, citation, and injection-label boundaries;
- context byte-budget compliance; and
- rendered claim evidence citations in context bundles.

The v2 corpus adds natural paraphrases, typo/abbreviation queries, long-document needles and distractors, honest-none cases, explicit broadening, conflict completeness, semantic bait sources, and a stale-claim/fresh-raw correction. Reports separate direct qualified recall, one-reformulation probe source availability, exact-fetch bytes, and independent refined qualified recovery. Each original/probe query pair is recorded. Reformulations are frozen from Fable 5 runs made outside the repository without memory/source context; title/fetch verdicts are frozen separately. Correctness is then checked deterministically by stable source labels, citations, and qualified returned entities rather than live model judgment.

The semantic gate requires paraphrase source recall of at least 80% at depth five and 90% at the shipped depth eight, end-to-end qualified recovery of at least 90%, zero false answers and bait fetches, false abstention at or below 10%, probe metadata below 2 KiB/3 KiB at depth five/eight, exact citation round trips, at most nine fetched references and 12 KiB of fetched text, and a complete serialized recovery pipeline no larger than 12 KiB. The one-call gate independently requires at least 90% recovery, at most 10% false abstention, zero false answers, semantic-bait/case-forbidden fetches, scope violations, and citation violations, a maximum 12 KiB packet, and p95 below two seconds. These gates run for both the complete corpus and the complete `--recovery-only` selection. Wire accounting conservatively rounds each independently serialized response to a 64-byte block so timestamp formatting cannot make the gate nondeterministic.

For the 0.6.0 release candidate, the pinned isolated semantic/reranker configuration completes all 11 recovery cases. The expanded diagnostic workflow recovers 11/11. The one-call path recovers 10/11 (90.9%), with zero false answers, semantic-bait fetches, and case-forbidden fetches; it safely abstains on a post-migration question whose fresh raw correction has not been promoted to a claim. The maximum packet is below 12 KiB and tested-host p95 is below two seconds. Direct qualified paraphrase claim recall remains limited; the measured improvement belongs to bounded recovery, not ordinary recall or a universal semantic-quality claim.

A separate isolated shadow on one real 390-document development project used ten natural paraphrases chosen before evidence inspection. Nine questions were answerable and all nine completed the probe → exact fetch → qualified recall path; the tenth correctly abstained because memory named the owner of an acknowledgment but not its recipient. No answer relied on an unqualified candidate, no false answer occurred, each exact packet stayed below 3.2 KiB and nine references, and the installed binary and live store were not touched. This is useful external-validity evidence, not a committed statistical gate.

The 11-case slice and ten-question shadow are regression evidence, not a universal benchmark; publish counts alongside rates and expand with anonymized real failures before making broad quality claims.

Macro recall and precision are regression signals against the committed baseline. They are not downstream task success and must not be presented as a universal benchmark result. The timing report separates corpus/seed loading, semantic model loading, embedding generation, projection I/O, reranker loading/inference, recall, probe, citation fetching, refined recall, one-call retrieval, and serialization. Recommended gates are warm recall p95 at or below 500 ms, full one-call recovery p95 at or below two seconds, and the pinned full suite at or below ten minutes. Publish host/model/corpus details with latency; one development-machine run is not a general performance claim.

## Equal-quality token benchmark

The controlled 0.5.0 audit did not demonstrate token savings at equal quality. It measured substantial fixed instruction overhead and multi-call recovery cost; Memoree remained uniquely useful for off-repository history, but that does not prove a default current-code memory benefit. Version 0.6 therefore shortens the skill and adds one-call recovery. Current-source questions remain with repository tools.

A matched one-turn Fable 5 `OK` calibration measured 3,601 baseline versus 4,769 skill-enabled processed input tokens. The 0.6 skill therefore adds 1,168 tokens, down 73.0% from the 0.5 measurement of 4,326. This establishes lower fixed instruction overhead only; cache behavior and retrieval turns still determine end-to-end cost.

A one-run targeted reproduction seeded the prior audit in an isolated 0.6 store and asked the same conclusion question. Both Fable 5 arms answered correctly. The Memoree skill plus one 2,766-byte retrieve response used 6,020 processed input tokens, $0.0405, and 8.04 s; a minimal oracle statement used 3,669 tokens, $0.0106, and 2.34 s. The input ratio improved from the 0.5 audit's 11.2× to 1.64×, but Memoree still cost 3.83× and took 3.43× as long as the oracle. This single reproduction is directional evidence that the redesign attacks overhead—not a savings or speed claim.

Do not claim token savings until at least ten real development tasks compare the same model and task under no-memory, repository-only, and Memoree-assisted conditions. Completeness and false-answer safety must be equal or better, and processed input tokens must fall by at least 10%. Report latency and cost as separate outcomes rather than using fewer output tokens as a proxy.

## Experimental project-map agent evaluation

The structural index has deterministic parser/provenance tests and a separate model-in-the-loop task corpus under `eval/project-structure/v1`. The latter compares the same Fable 5 agent and task with ordinary repository tools against the canonical skill plus an isolated prebuilt project index. Processed input includes ordinary, cache-creation, and cache-read input reported by the provider. Latency and turns are recorded separately; answers are blind-graded for correctness and completeness. The installed binary and normal Memoree database are not used.

The experiment produced an important negative result:

- An initial ten-case complex batch saved 28.9% processed input, but map quality was equal or better in only 7/10 cases. Two correctness rounds added bounded behavioral-test leads, stricter receiver resolution, requested-facet completeness, and explicit blind spots.
- A fresh ten-case complex held-out batch after those fixes saved only 7.38%, was 5.22% slower, and lost blind quality in 8/10 cases with no wins. Answers omitted relevant helpers, paths, integration/harness behavior, or stopped exploration too early.
- Ten narrow lookups in the small repository cost 55.3% more processed input, 36.8% more latency, and 49 versus 42 turns.
- Five narrow lookups in an isolated 4,227-file repository had equal correctness but cost 36.4% more processed input, 54.7% more latency, and 21 versus 17 turns. Agents invoked the map and then verified with reads or grep.

The full-repository baseline used by some graph products is not an acceptable control because competent agents search and read bounded current-source regions. These results measure real agent behavior against that realistic baseline. The canonical skill therefore does not invoke the project index. The aggregate record is committed at `eval/project-structure/v1/results/2026-07-21.json`; raw model transcripts remain outside the repository.

Promotion requires two frozen, pre-registered held-out batches spanning both complex and narrow tasks. Each routed stratum must have at least 90% non-loss with zero severe misses; aggregate blind quality must be equal or better, median processed input at least 10% lower, and median latency no worse than +10%. Index build time and disk/RSS must be reported. Tuning against a failed held-out batch cannot promote the feature.

For ad hoc local performance diagnosis, `memoree profile retrieve QUERY --iterations 10` emits content-free latency, response-size, and count distributions. It sets `query_recorded=false` and `content_recorded=false`; it is not a quality benchmark. Opt-in project metrics observe real operations over time using the same content-free timing fields plus closed outcomes, index activity, errors, and explicit feedback categories. They remain observational and cannot measure the missing counterfactual.

Use `memoree experiment begin --primary tokens` for the real-task gate, create one opaque randomized pair per matched task, and record both assigned arms with consistent provider token accounting. Reports exclude incomplete pairs and always disclose order balance and statistical caveats. Treat task completion and completeness as safety gates before interpreting a lower token or elapsed-time median. See [Project metrics and experiments](metrics.md).

When adding cases, prefer anonymized failures from real agent retrieval. Keep known-hard cases as `"gate":"report"` until the current baseline handles them, then promote them to `hard` in a reviewed corpus-version change. A retrieval-engine or ranking change should reuse the same corpus and byte budgets; semantic or hybrid retrieval earns adoption only after it improves downstream agent task success without breaking these deterministic invariants.

Cross-encoder qualification has a separate disjoint pair evaluator and cannot be enabled by end-to-end recall scores alone:

```sh
memoree semantic evaluate-reranker \
  --model-directory /path/to/pinned/model \
  --calibration eval/reranker/calibration-v1.jsonl \
  --heldout eval/reranker/heldout-v1.jsonl
```

The checked-in pair sets intentionally remain underpowered and therefore cannot promote a qualification threshold. Ordering-only admission and default-on promotion have separate quality/latency gates documented in [Quality gates](quality.md).
