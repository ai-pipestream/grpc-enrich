# grpc-enrich

gRPC enrichment service: VLM picture-describe, chart extract, and formula/code annotations on a gRParse Document

Java gRPC server. A `Document` in, a live stream of typed `ItemAnnotation` /
`ItemSkipped` events out, keyed by `self_ref`. The VLM is a **remote server**
(llama.cpp, OVMS, or any OpenAI-compatible HTTP endpoint) that this process
calls; no model weights, no torch, no transformers in this binary. Diskless:
documents live only in memory for the duration of the RPC.

## Build and test

Requires JDK 25 and (for proto lint) buf.

```sh
./gradlew build        # compiles, generates stubs from proto/, runs tests
buf lint               # STANDARD + COMMENTS, vendored document.proto exempt from COMMENTS
./gradlew :enrich-service:installDist
```

Tests run against an in-process fake VLM HTTP endpoint (no network, no
weights), including proof that events stream before the client half-closes
and that per-item events are never held back into a batch.

## Run

```sh
ENRICH_VLM_URL=http://localhost:8080 \
  enrich-service/build/install/enrich-service/bin/enrich-service
```

| Env | Default | Meaning |
|---|---|---|
| `ENRICH_PORT` | `50056` | gRPC listen port |
| `ENRICH_VLM_URL` | unset | Default VLM endpoint (base URL; requests go to `<url>/v1/chat/completions`). Per-request `EnrichOptions.vlm_endpoint` overrides |
| `ENRICH_MAX_DOCUMENT_MIB` | `70` | Assembled document byte cap (`RESOURCE_EXHAUSTED` above) |
| `ENRICH_MAX_CONCURRENT_VLM` | cores | Cap on concurrent VLM calls per request |
| `ENRICH_VLM_TIMEOUT_SECONDS` | `300` | Per-VLM-call timeout |
| `ENRICH_METRICS_INTERVAL_SECONDS` | `60` | Metrics line interval; 0 disables |

The server registers `grpc.health.v1.Health` and server reflection (v1 and
v1alpha).

## Wire API

`ai.pipestream.enrich.v1.EnrichService` (see
[`proto/ai/pipestream/enrich/v1/enrich_service.proto`](proto/ai/pipestream/enrich/v1/enrich_service.proto)):

- `EnrichDocument(stream EnrichDocumentRequest) returns (stream EnrichDocumentResponse)` —
  first message carries `EnrichOptions` (flags mirroring Docling's
  `do_picture_description` / `do_chart_extraction` / `do_code_enrichment` /
  `do_formula_enrichment`, presets with `*_raw` fallbacks, endpoint /
  concurrency / timeout overrides) plus the document inline or as
  `DocumentChunk` slices; `ItemImage` messages carry stripped crops.
  Events: `EnrichStarted` (counts selected), one `ItemAnnotation` or
  `ItemSkipped` per item as that VLM call returns, `EnrichComplete` trailer.
  Chart extraction lands as typed `TableData` cells, never CSV-only. A failed
  VLM call is an `ItemSkipped` (`SKIP_REASON_VLM_ERROR`), never an RPC error.
- `GetServiceInfo` — versions, default endpoint, byte cap, concurrency cap.

`ai/pipestream/document/v1/document.proto` is vendored verbatim from gRParse
(the canonical copy); do not edit it here.

## Docling parity

Enrichment semantics mirror Docling's enrichment pipeline
(`picture_description_base_model.py`, `chart_extraction/granite_vision.py`,
`code_formula_vlm_model.py`):

- **Area threshold** — `picture_description_area_threshold` unset (0) applies
  Docling's default **0.05** of the page area; a negative value disables the
  threshold entirely.
- **Prompts are Docling's verbatim strings** — SmolVLM: `Describe this image
  in a few sentences.`; Granite Vision: `What is shown in this image?`; chart:
  `Convert the information in this chart into a data table in CSV format.`;
  code/formula with an image crop: the bare `<code>` / `<formula>`.
- **Chart gate** — chart extraction runs when the picture's top
  figure-class prediction is one of Docling's `SUPPORTED_CHART_TYPES`
  (`bar_chart`, `pie_chart`, `line_chart`). One intentional excess over
  Docling: a picture labelled `DOC_ITEM_LABEL_CHART` also triggers it.
- **Chart CSV → TableData** — the first row is a column-header row only when
  all its values are non-numeric; any non-numeric data cell is marked
  `row_header=true` (pandas NaN/empty counts as non-numeric).
- **Code/formula send the image crop** when an `ItemImage` is supplied for
  the item's `self_ref` (code items may also carry an inline data-URI
  `ImageRef`); without a crop, a text-only prompt with the existing item text
  is the fallback (not a Docling mode). All code/formula output is
  post-processed like Docling: truncate at `<end_of_utterance>`, strip
  `</code>` / `</formula>` / the `<loc_0>…` sentinel, lstrip, and parse the
  leading `<_language_>` token into `CodeAnnotation.language` (exact-case
  match against docling-core's value strings, UNKNOWN fallback) and
  `language_raw`. `return_document` also sets `code_language` /
  `code_language_raw` on the patched `CodeItem`.
- **Generation budgets (max_tokens)** — description 200, code/formula 2048
  (Docling's values); chart 4096 (ours: Docling uses the model's max length,
  and a wide table does not fit in 2048).

Where we deliberately exceed Docling: `ItemSkipped` events carry explicit
skip reasons (Docling silently stores empty strings), a failed VLM call is an
event rather than a failed RPC, events stream before the client half-closes,
and description annotations record the model name as provenance (Docling's
API path records "not-implemented").

## Start here (humans and LLMs)

1. [`AGENTS.md`](AGENTS.md) — read order, definition of done, git
2. [`docs/architecture.md`](docs/architecture.md) — where this sits, language, live stream vs Docling
3. [`docs/design.md`](docs/design.md) — wire API, Document mapping, tests
4. [`docs/guidelines.md`](docs/guidelines.md) — how to build it so it matches the fleet

Operational patterns are copied from the grPOIc / grpc-email Java siblings
(two-module Gradle layout, virtual threads, byte cap, in-process test fakes)
and the gRParse Document proto.

## Docs

- [Architecture](docs/architecture.md) — where this sits in the collector fleet
- [Design](docs/design.md) — wire API, Document mapping, tests
- [Guidelines](docs/guidelines.md) — how to build it so it matches the fleet

## Remotes

- **Forgejo** (`git.rokkon.com/ai-pipestream/grpc-enrich`) is the source of truth. `main` lives here.
- **GitHub** is a public push-mirror of `main`. Do not merge to GitHub `main`.
- GitHub's default branch is `development` so LLM / `gh` work lands there instead of clobbering the mirror.

Push Forgejo first. GitHub `main` updates from the Forgejo push-mirror.
