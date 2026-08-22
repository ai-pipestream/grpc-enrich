# AGENTS.md: grpc-enrich

**Status: implemented.** `grpc-enrich` is a working Java gRPC server: a
`Document` in, a live stream of typed `ItemAnnotation` / `ItemSkipped`
events out, keyed by `self_ref`. 157 tests pass (`./gradlew clean build
test --no-daemon`). Specs (`docs/*.md`) remain the source of truth for
wire shape and rules; this file is the status/orientation layer on top.

## Read this first, in order

1. This file
2. `docs/architecture.md`: fleet boundary, language, what we refuse to own
3. `docs/design.md`: wire API sketch, Document mapping, tests
4. `docs/guidelines.md`: fleet rules (streaming, proto, git, tests)

If architecture and an existing sibling disagree on *process* (diskless,
health, buf), follow the sibling. If they disagree on *product* (live
stream, Document plane), follow architecture.md.

## This service

gRPC enrichment service: VLM picture-describe, chart extract, and formula/code annotations on a gRParse Document.

- **Language:** Java (JDK 25). A gRPC *client of a VLM server* (llama.cpp,
  OVMS, or any OpenAI-compatible HTTP endpoint). No PyTorch, no model
  weights, no transformers vendored in this process.
- **Layout:** two-module Gradle build. `enrich-api` holds the generated
  proto stubs (protos live at the repo root under `proto/`, one buf
  module for the whole repo). `enrich-service` is the implementation:
  `server/` (gRPC + HTTP front ends), `engine/` (item selection, VLM
  orchestration, chart/code/formula post-processing), `vlm/` (the
  OpenAI-compatible HTTP client).
  - `server/GrpcEnrichServer.java`, `server/EnrichServiceImpl.java`: gRPC wiring
  - `server/EnrichHttpServer.java`: the JSON/NDJSON HTTP shim over the same streaming path
  - `engine/ItemSelector.java`: picks pictures, charts, code, formula items
  - `engine/EnrichmentEngine.java`: concurrent VLM calls, skip-on-failure
  - `engine/ChartCsvParser.java`, `engine/CodeFormulaPostProcessor.java`: output typing
  - `vlm/OpenAiCompatVlmClient.java`: the remote VLM HTTP client
- **Stack:** Document in, ItemAnnotation stream out, keyed by self_ref. Parse stays in gRParse / vlm-convert. This only annotates existing pictures/code/formulas.
- **Live stream:** EnrichStarted (counts selected), then ItemAnnotation or ItemSkipped per item as each VLM call returns, EnrichComplete trailer.

## Definition of done (v1) — met

EnrichDocument stream, fake VLM HTTP in tests, skip-on-missing-image, chart table as typed cells not CSV-only, health+reflection: all in place.

Also in place: README with build/run; proto lint clean; tests that fail if
someone turns the stream back into a batch (an event before the input is
fully consumed, or per-item events before Complete) — see
`EnrichStreamAdversarialTest`.

Follow-up work (not yet done) belongs in `docs/design.md` or an issue, not
back in this file as if the service were unstarted.

## Workspace

Checkout path: `/work/main/grpc-services/grpc-enrich`.
Git: `origin` = Forgejo (push `main` here). `github` = GitHub mirror.
Never merge GitHub `main`. See `docs/guidelines.md`.

gRParse wiring (`COLLECTOR_*` enum, endpoint env) is a **follow-up**.
