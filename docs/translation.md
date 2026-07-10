# Cross-provider translation

This is the detailed reference for `[route.translate]`, the gateway's cross-provider request/response translation. It covers what gets translated, how buffered and streaming responses differ, what fidelity guarantees exist, and the current limits.

## What it does

A route can declare:

```toml
[route.translate]
from = "anthropic"
to = "openai"
```

`from` names the wire dialect the client speaks; `to` names the wire dialect the upstream speaks. The gateway rewrites the request into `to`'s shape on the way out, and translates the upstream's response back into `from`'s shape on the way back to the client. Supported providers are `anthropic`, `openai`, and `google` (the same `KNOWN_PROVIDERS` list used everywhere else in the gateway).

The client always calls the route using `from`'s own endpoint shape, for example `POST /<route-id>/v1/messages` for an Anthropic-shaped `from`. The gateway rewrites the outbound path to `to`'s own endpoint before forwarding:

- `anthropic` -> `/v1/messages`
- `openai` -> `/v1/chat/completions`
- `google` -> `/v1beta/models/<model>:generateContent` (or `:streamGenerateContent` when the request streams)

So a client that only knows how to speak Anthropic's Messages API can be pointed at an OpenAI or Google upstream, and vice versa, without touching a line of client code.

## The canonical IR

Translation does not go pairwise. There is one canonical, lossless intermediate representation (`CanonicalRequest`/`CanonicalResponse`/`CanonicalStreamEvent`) that every provider adapter parses into and renders out of. A request flows client dialect (`from`) -> canonical -> upstream dialect (`to`); a response flows the reverse. Every field a provider's wire format can carry has a home somewhere in the canonical shape (messages with typed content blocks, tools, tool_choice, sampling parameters, stop reason, usage, and a catch-all `extra` map for anything the typed fields don't cover).

Going through one intermediate instead of N-squared per-pair shims is what keeps adding a fourth provider a linear amount of work (one new adapter) instead of a combinatorial one (a new shim for every existing provider). It also means the fidelity story only has to be told twice per provider (what it loses parsing in, what it loses rendering out), not once per pair.

The invariant this buys: the client only ever sees the `from` dialect, the upstream only ever sees the `to` dialect, and neither one ever sees the canonical form. It exists purely inside the gateway, for the duration of one request/response cycle.

## Buffered vs streaming

**Buffered.** When the upstream answers with a normal (non-streaming) body, or when the route also has `on_response` steps, the whole response is parsed into the canonical shape and re-rendered whole into the client's dialect. This is the simpler path: one parse, one render.

**Streaming.** When the request has `stream: true` and the route has no `on_response` steps, the upstream's SSE response is translated event by event as it arrives, without ever buffering the whole thing. Each provider adapter has a parse-side and a render-side per-stream state machine (`StreamParseState`/`StreamRenderState`) because the three dialects don't share one streaming granularity:

- Anthropic's stream is already the granular lifecycle the canonical stream event shape is modeled on: an explicit `message_start`, a `content_block_start`/`delta`/`stop` per block, a `message_delta`, a `message_stop`.
- OpenAI and Google are coarser. A single `chat.completion.chunk` (or Gemini chunk) packs together what Anthropic splits across several events, and a tool call streams as one fragment carrying its id/name followed by bare argument fragments with no per-fragment identity.

Parsing a coarse source lifts each wire event to the single most salient canonical event and remembers just enough state to give the one streamed text block a stable index and correlate a tool call's later argument fragments back to its opening event. Rendering to a granular target (Anthropic) does the opposite: it synthesizes the lifecycle scaffolding the coarse source never sent, a `message_start` and a text block's `content_block_start` the first time content actually needs to flow, closing whatever block is still open before starting a new one. Rendering to a coarse target (OpenAI) collapses the granular start/stop events into its chunk shape instead: a canonical `MessageStart` becomes the role-bearing first chunk, a `ContentBlockStop` emits nothing.

A source dialect that has no terminal event of its own (Google's Gemini stream never sends one) still needs to hand the client whatever terminal the target dialect requires (Anthropic's `message_stop`, OpenAI's `data: [DONE]`). The renderer tracks whether a terminal has already gone out and synthesizes one at stream end if not, so the target's required terminal appears exactly once regardless of what the source sent.

## Fidelity: extra fields and the translation report

Every provider adapter's parse side tags any top-level field it doesn't have a typed slot for with its own provider name, `"<provider>.<field>"`, and stashes it in the canonical `extra` map. On render, that tag is what lets the gateway tell a genuine same-provider round trip from a cross-provider one: a field tagged `anthropic.metadata` re-emits losslessly when rendering back to Anthropic, but is dropped (with a report entry) when rendering to OpenAI or Google, since it names a field that dialect never defined.

Nothing is ever silently discarded. Every field that has no home on the target dialect, whether it's an untranslatable `extra` entry or a canonical field the target simply doesn't support (OpenAI has no `top_k`; Google's adapter has never parsed `tool_choice`), is recorded in a `TranslationReport`: a list of dropped fields (`path` plus a human-readable `reason`) and looser `notes` for downgrades that still produce a valid field, just a less specific one (Anthropic's `stop_sequence` stop reason downgrading to OpenAI's plain `"stop"`, for example).

Setting `report_header = true` on `[route.translate]` surfaces that report to the client as a response header:

```
x-sluice-translation: dropped=1; fields=sampling.top_k
```

The header carries the dropped-field count and the comma-joined field paths only, never the dropped values themselves (a dropped value could carry request or response content). This header lives in the gateway's internal `x-sluice-*` namespace, which normally only reaches the client when it's listed in `[gateway] expose_headers`; `report_header = true` opts this one header in for this one route on its own, without requiring an `expose_headers` entry. On the streaming path, only the request-side render's drops are folded into the header; response-stream-side drops are logged, not added to it, since the header has already been sent by the time the body starts flowing.

A buffered render into a strict dialect also needs a few fields the canonical shape has no equivalent for at all: Anthropic requires `type: "message"`, `role: "assistant"`, and a string `id`; OpenAI requires `object: "chat.completion"`, a string `id`, and an integer `created`. These are synthesized on the buffered response render so the body validates against a strict SDK, but only when a genuine same-provider `extra` value hasn't already supplied them. `type`/`role`/`object` are fixed dialect constants, but `id` (both dialects) and `created` (OpenAI) have no canonical source to synthesize a real value from, so they render as stable placeholders (`"msg_translated"`, `"chatcmpl-translated"`, `created: 0`), not real generated ids or timestamps.

## `[route.translate]` fields

```toml
[route.translate]
from = "anthropic"
to = "openai"
model = "gpt-4o"
report_header = false
```

- `from` (required): the client-facing dialect. Must be one of `anthropic`, `openai`, `google`.
- `to` (required): the upstream-facing dialect. Must be one of `anthropic`, `openai`, `google`, and must differ from `from` (a translate that doesn't translate is rejected at config load).
- `model` (optional): pins the model id the upstream request is sent with, overriding whatever model the client's body named. If set, it must resolve in the model registry under `to` (checked at config load).
- `report_header` (optional, default `false`): adds the `x-sluice-translation` response header described above.

## The llm view on translate routes

Guardrail and budget steps that run on `on_request`/`on_response` read a parsed `llm` view of the request, the same lossy read-side projection every route gets. On a translate route, that view is parsed using `translate.from` as the effective ingress dialect, so `on_request` steps see the request exactly as the client sent it, before it's rewritten into `to`'s shape. If a route also sets `[route.adapter]`, `adapter.ingress` takes priority when both are present, which is why config validation rejects a route where `adapter.ingress` and `translate.from` disagree: a silent mismatch there would parse the `llm` view with the wrong dialect and quietly disable any guardrail relying on it.

## Current limits

Be aware of these before pointing translation at anything that matters:

- A translate route cannot also run `on_stream` steps. Guardrails and per-chunk mutation on a translated route have to live on `on_request`/`on_response` instead; a route combining `[route.translate]` with an `on_stream` step is rejected at config load.
- A translated response that exceeds `max_response_bytes` is rejected with HTTP 413, never streamed through untranslated. This is forced even when `[gateway] oversize = "stream_through"` globally: streaming raw `to`-dialect bytes to a `from`-dialect client would hand it the wrong wire format as an HTTP 200, which is worse than a clean rejection.
- If a route sets both `[route.adapter]` and `[route.translate]`, `adapter.ingress` must equal `translate.from`. A mismatch is rejected at config load.
- `translate.from` and `translate.to` must differ.
- `[route.translate]` cannot be combined with a `mode = "loopback"` step on the same route; that interaction is out of scope.
- Streaming translation only applies when the route has no `on_response` steps. A translate route with `on_response` steps always uses the buffered path, and an upstream that answers such a route with an SSE stream anyway gets a diagnosable 502 rather than a silently broken response.
- Field-level fidelity is best-effort by construction, not a promise that every request round-trips byte-for-byte. Anything without an equivalent on the target dialect shows up in the translation report, never silently invented or dropped without a trace.

## Example

```toml
[[route]]
id = "tr"
upstream = "https://api.openai.com"

  [route.translate]
  from = "anthropic"
  to = "openai"
  model = "gpt-4o"
  report_header = true
```

A client posts an Anthropic-shaped `/v1/messages` request to `http://<gateway>/tr/v1/messages`. The gateway parses it as Anthropic, renders it as OpenAI, and forwards it to `https://api.openai.com/v1/chat/completions`. The response comes back OpenAI-shaped, gets rendered back into Anthropic's `/v1/messages` response shape, and the client never has to know the upstream wasn't Anthropic at all.

See [configuration](configuration.md) for the rest of `[gateway]` and `[[route]]`, [steps and directives](steps-and-directives.md) for how `on_request`/`on_response` steps interact with a translate route, and `../examples/` for a runnable translate config.
