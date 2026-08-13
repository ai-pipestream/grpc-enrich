# grpc-enrich design

## 1. Goals

- Feature parity with Docling's enrichment flags:
  picture description, chart extraction, code OCR, formula OCR.
- `Document` in / annotated `Document` (or a stream of patches) out.
  Same refs (`#/pictures/3`). Additive annotations; never rewrite
  provenance boxes. Patches stream **as each VLM call returns** so a
  UI can show captions appearing on already-parsed figures. Docling
  waits for the enrichment pipe; we do not. A unary full Document at
  the end is convenience only.
- One slow VLM must not block parse. Independent replica count.
- Typed wire: descriptions are strings on the picture, charts become
  `TableItem`s linked from the picture, code/formula replace or
  annotate the text item. No JSON-blob round trip
  (`MessageToDict` is forbidden on this path).

## 2. Non-goals (v1)

- Choosing a winner among collectors (that is still downstream).
- Running Heron / TableFormer / RapidOCR (those stay in gRParse).
- Hosting the VLM weights in this binary.
- Agent tools / function calling.

## 3. Wire API (sketch)

`ai.pipestream.enrich.v1.EnrichService`

```text
rpc EnrichDocument(stream EnrichRequest) returns (stream EnrichEvent);
```

First message: `EnrichOptions` + optional inline `Document`. Further
messages: `DocumentChunk` for large docs, or `ItemImage` if the caller
strips image bytes from the document and sends crops separately.

Options, mirroring Docling without the Python unions:

- `do_picture_description`, `picture_description_preset` (enum +
  `preset_raw` for unknown names)
- `picture_description_area_threshold`
- `do_chart_extraction`, `chart_preset`
- `do_code_enrichment`, `do_formula_enrichment`
- `vlm_endpoint` (optional override; otherwise process config)
- `concurrency`, `timeout_seconds`

Events:

1. `EnrichStarted` — item counts selected
2. `ItemAnnotation` — `self_ref`, oneof:
   - `description` (string, model, confidence)
   - `chart_table` (a `TableData`)
   - `code` / `formula` (text + language)
3. `ItemSkipped` — ref + reason (no image, below threshold, VLM error)
4. `EnrichComplete` — succeeded / skipped / failed counts

The client (gRParse or a sidecar) applies patches. This server may
also return a full `Document` at the end for unary convenience;
streaming patches are the real API.

## 4. Presets

Enums for the models Docling names, plus `*_raw`:

| Preset | Job |
|---|---|
| `SMOLVLM`, `GRANITE_VISION` | picture describe |
| `GRANITE_VISION_CHART2CSV` | chart → table |
| `CODE_FORMULA_V2` | code / formula |
| `UNSPECIFIED` + `preset_raw` | whatever the endpoint serves |

The enrich process does not download Hugging Face repos. The operator
points `ENRICH_VLM_URL` at a server that already has the weights.

## 5. Item selection

- Pictures: area ≥ threshold, has an `ImageRef` with bytes or a
  resolvable URI the server is configured to fetch (default: inline
  only, no outbound HTTP).
- Charts: picture whose figure-class top label is a chart **or**
  `do_chart_extraction` on all pictures (Docling's broader mode).
- Code/formula: `TextItem.label` already `CODE` / `FORMULA`, or
  layout label from gRParse. We do not re-run layout.

## 6. Tests

- Document with one pictured QR and one paragraph: describe on → one
  `ItemAnnotation` on `#/pictures/0`, text items untouched.
- Missing image on a picture → `ItemSkipped`, RPC still `OK`.
- Chart preset returns a 2×3 table; proto fields are typed cells, not
  a CSV string as the only representation (CSV may additionally ride
  in an annotation).
- VLM endpoint down → items skipped with reason, not a process crash.
