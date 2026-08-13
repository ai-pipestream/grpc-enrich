# AGENTS.md — grpc-enrich

You are implementing **grpc-enrich** from scratch in this repo. There is no
application code yet. Specs are the source of truth.

## Read this first, in order

1. This file
2. `docs/architecture.md` — fleet boundary, language, what we refuse to own
3. `docs/design.md` — wire API sketch, Document mapping, tests
4. `docs/guidelines.md` — fleet rules (streaming, proto, git, tests)

Do not start coding until those four are in your context. If architecture
and an existing sibling disagree on *process* (diskless, health, buf),
follow the sibling. If they disagree on *product* (live stream, Document
plane), follow architecture.md.

## This service

gRPC enrichment service: VLM picture-describe, chart extract, and formula/code annotations on a gRParse Document

- **Language:** C++ or Java gRPC *client of a VLM server*. No PyTorch in this process.
- **Copy from:** /work/main/grpc-services/gRParse for Document handling; call llama.cpp / OVMS / OpenAI-compat HTTP — do not vendor transformers.
- **Stack:** Document in, ItemAnnotation stream out, keyed by self_ref. Parse stays in gRParse / vlm-convert. This only annotates existing pictures/code/formulas.
- **Live stream:** EnrichStarted (counts selected), then ItemAnnotation or ItemSkipped per item as each VLM call returns, EnrichComplete trailer.

## Definition of done (v1)

EnrichDocument stream, fake VLM HTTP in tests, skip-on-missing-image, chart table as typed cells not CSV-only, health+reflection.

Also: README with build/run; proto lint clean; tests that fail if someone
turns the stream back into a batch (assert an event before the input is
fully consumed, or per-item events before Complete).

## Workspace

Checkout path: `/work/main/grpc-services/grpc-enrich`.
Git: `origin` = Forgejo (push `main` here). `github` = GitHub mirror.
Never merge GitHub `main`. See `docs/guidelines.md`.

gRParse wiring (`COLLECTOR_*` enum, endpoint env) is a **follow-up**.
Ship a working server in this repo first.
