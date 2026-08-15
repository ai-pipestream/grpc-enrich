# grpc-enrich design

## 1. Goals

- Four enrichment jobs behind one flag each: picture description, chart
  extraction, code OCR, formula OCR.
- `Document` in, annotated `Document` (or a stream of patches) out. Same refs
  (`#/pictures/3`). Additive annotations; never rewrite provenance boxes.
  Patches stream as each VLM call returns so a UI can show captions appearing
  on already-parsed figures instead of waiting for the whole enrichment pass.
  A unary full Document at the end is convenience only.
- One slow VLM must not block parse. Independent replica count.
- Typed wire: descriptions are strings on the picture, charts become
  `TableItem`s linked from the picture, code/formula replace or annotate the
  text item. No JSON-blob round trip (`MessageToDict` is forbidden on this
  path).

## 2. Non-goals (v1)

- Choosing a winner among collectors (that is still downstream).
- Running Heron / TableFormer / RapidOCR (those stay in gRParse).
- Hosting the VLM weights in this binary.
- Agent tools / function calling.

## 3. Wire API

`ai.pipestream.enrich.v1.EnrichService`

```text
rpc EnrichDocument(stream EnrichDocumentRequest) returns (stream EnrichDocumentResponse);
```

First message: `EnrichOptions` plus an optional inline `Document`. Further
messages: `DocumentChunk` for large docs, or `ItemImage` if the caller strips
image bytes from the document and sends crops separately.

`EnrichOptions` is flat proto fields: one boolean per job
(`do_picture_description`, `do_chart_extraction`, `do_code_enrichment`,
`do_formula_enrichment`), an enum preset per job with a `*_raw` string
fallback for model names the schema does not know,
`picture_description_area_threshold`, and the per-request overrides
`vlm_endpoint`, `concurrency`, `timeout_seconds`. `return_document` asks for
the patched full document in the trailer.

Events:

1. `EnrichStarted`: item counts selected
2. `ItemAnnotation`: `self_ref` plus one of `description` (string, model,
   confidence), `chart_table` (a `TableData`), or `code` / `formula` (text +
   language)
3. `ItemSkipped`: ref + reason (no image, below threshold, VLM error)
4. `EnrichComplete`: succeeded / skipped / failed counts

The client (gRParse or a sidecar) applies patches. This server may also
return a full `Document` at the end for unary convenience; streaming patches
are the real API.

### HTTP shim

The binary also exposes the service over HTTP on `ENRICH_HTTP_PORT` (default
50057, `0`/empty disables it; gRPC stays on 50056). It is a shim, not a fork:
`EnrichHttpServer` parses the proto3-JSON envelope (`{"options": ...,
"document": ..., "item_images": ...}`) with protobuf's `JsonFormat` and
drives the existing `EnrichServiceImpl` through an in-process
`StreamObserver` harness, playing options, crops, completing chunk,
half-close exactly as a wire client would. `POST /v1/enrich` buffers the
events into one `{"events": [...]}` reply (400 / 413 / 500 mapped from
`INVALID_ARGUMENT` / `RESOURCE_EXHAUSTED` / anything else);
`POST /v1/enrich/stream` forwards each event as a flushed NDJSON line so HTTP
callers get the same live per-item stream; `GET /healthz` is a static 200.
`GetServiceInfo` remains gRPC-only.

## 4. Presets

Enums for the model families the endpoint is expected to serve, plus `*_raw`:

| Preset | Job |
|---|---|
| `SMOLVLM`, `GRANITE_VISION` | picture describe |
| `GRANITE_VISION_CHART2CSV` | chart to table |
| `CODE_FORMULA_V2` | code / formula |
| `UNSPECIFIED` + `preset_raw` | whatever the endpoint serves |

The enrich process does not download Hugging Face repos. The operator points
`ENRICH_VLM_URL` at a server that already has the weights.

## 5. Item selection

- Pictures: area at or above the threshold (default 0.05 of the page; a
  negative value disables the check), and an `ImageRef` with bytes or a
  resolvable URI the server is configured to fetch (default: inline only, no
  outbound HTTP).
- Charts: a picture whose figure-class top prediction is a supported chart
  type (`bar_chart` / `pie_chart` / `line_chart`) or whose label is `CHART`.
- Code/formula: `TextItem.label` already `CODE` / `FORMULA`, or the layout
  label from gRParse. We do not re-run layout. The VLM call sends the item's
  image crop with the bare `<code>` / `<formula>` prompt when a crop is
  available, else a text-only fallback; output is post-processed (sentinel
  strip, `<_language_>` token mapped to `code_language`).

## 6. Tests

- Document with one pictured QR and one paragraph: describe on, one
  `ItemAnnotation` on `#/pictures/0`, text items untouched.
- Missing image on a picture: `ItemSkipped`, RPC still `OK`.
- Chart preset returns a 2x3 table; proto fields are typed cells, not a CSV
  string as the only representation (CSV may additionally ride in an
  annotation).
- VLM endpoint down: items skipped with reason, not a process crash.
  Transient failures (HTTP 429/500/502/503/504 and connection drops) are
  retried first: 5 retries, exponential backoff from 0.1s.
