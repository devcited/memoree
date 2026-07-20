# Reranker evaluation

These JSONL files are independent of the end-to-end retrieval corpora. They
measure whether a pinned local cross-encoder can separate answer-bearing memory
passages from realistic near misses after authority filtering.

A passage is relevant when a correct, complete answer would have to cite or
reconcile it. Agreement with an expected answer is not relevance: an in-scope
opposite-polarity statement directly answers the question and is labeled as
conflict-capable evidence. Structurally stale or superseded revisions are absent
because the authority filter must exclude them before reranking.

The boolean annotation decision is: relevant if the passage, interpreted with
its tense and timestamp, entails or materially constrains the truth value of the
queried proposition at query time. This includes evidence for the proposition's
negation. A future plan is relevant to a current-status question only when the
plan entails the current state (for example, "will close the checks" entails
that they are not yet closed); a plan merely to review something does not.

Qualification and ordering labels are separate. `relevant` is the only
qualification label. `ordering_grade` is optional diagnostic metadata: `2`
directly answers, `1` must be cited or reconciled but is indirect, and `0` is
irrelevant. `polarity` (`supports` or `contradicts`) and `temporal_status`
(`current` or `future_plan`) are orthogonal audit facets, never score inputs.

- `calibration-v1.jsonl` is the only set used to choose frozen raw-logit
  thresholds. It emphasizes wrong-entity, polarity, omitted-qualifier,
  temporal, and same-topic hard negatives.
- `heldout-v1.jsonl` is scored only after the threshold and calibration version
  are frozen.

Every record is authored synthetic material. Private shadow evaluation uses a
separate local-only manifest. Its export allowlist is limited to confusion
matrix counts per surface/facet, fixed coarse score histograms, calibration
curve counts, latency percentiles, and corpus-size counts. Cells below five are
suppressed. Private text, queries, identifiers, embeddings, and case hashes must
never be exported or committed.

The decision is a raw-logit threshold. A monotone probability calibration may
be reported as metadata later, but it must never alter qualification. Claim and
artifact-window thresholds are evaluated separately because their passage
length distributions differ.

Model promotion additionally requires at least 60 positives and 120 hard
negatives per surface in both calibration and held-out sets, cluster-robust
confidence intervals over independent templates/documents, a stable
leave-one-query-group-out threshold (maximum shift at most 0.25 logits), a held-out
precision confidence-interval lower bound of at least 0.80, recall of at least
0.60, and conflict completeness of at least 0.90. Smaller checked-in sets remain
useful for development diagnostics but cannot produce an acceptance verdict.

Ordering-only use has a separate pre-registered local CPU gate: scoring sixteen
short passages must remain at or below 500 ms p95 on the reference development
machine. Passing this budget does not authorize the model to qualify or suppress
evidence.

The current production policy is `cross_encoder_ordering_v2`: explicit opt-in,
claim surface only, startup-warmed, at most sixteen non-exact candidates, and
fail-open to deterministic fusion. Artifact and mixed surfaces are disabled
because the realistic shadow showed no artifact uplift and materially exceeded
the ordering budget. Three consecutive inference-only calls above 500 ms open a
daemon-local breaker; after 32 skipped calls, one half-open probe may close it.
This breaker protects subsequent requests and is not a timeout for the slow call.

Default-on promotion for claims requires a cluster-robust top-3 uplift confidence
interval whose lower bound is above zero, no private-shadow facet regression,
inference-only p95 at or below 500 ms over at least 200 shadow queries, and a
breaker trip rate below one percent. Artifacts require their own positive uplift
and latency evidence rather than inheriting claim-surface promotion.
