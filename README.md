# Codex Router

A local proxy for OpenAI Codex Desktop that intercepts and rewrites the `/v1/responses/compact` API call, replacing Codex's built-in context compaction with a custom LLM summarization pipeline.

## Table of Contents

- [The Compact Problem](#the-compact-problem)
- [Architecture](#architecture)
- [How Compaction Works in Codex](#how-compaction-works-in-codex)
- [Our Solution: Interception + Rewriting](#our-solution-interception--rewriting)
- [SSE to JSON Conversion](#sse-to-json-conversion)
- [Compaction Prompt Strategy](#compaction-prompt-strategy)
- [Debug Logging System](#debug-logging-system)
- [Setup Guide](#setup-guide)
- [Configuration Reference](#configuration-reference)
- [Monitoring and Debugging](#monitoring-and-debugging)

---

## The Compact Problem

OpenAI Codex Desktop automatically compacts conversation context when the token count exceeds `model_auto_compact_token_limit`. It does this by calling the `/v1/responses/compact` API endpoint with the full conversation history.

The problem: most API proxies and third-party LLM services (including One API, chat.cloudapi.vip, LiteLLM, etc.) **do not implement** the `/v1/responses/compact` endpoint. This endpoint is OpenAI-specific and is not part of the standard OpenAI API.

When Codex calls `/v1/responses/compact` against an unsupported upstream, one of two things happens:

1. **Upstream returns an error** (404, 405, or 501) — Codex receives the error and displays "Error running remote compact task"
2. **Upstream returns an unexpected format** — Codex's compact handler crashes parsing the response

The result is that **context compaction silently fails**, and Codex may lose conversation context or crash.

### The Deeper Issue: SSE Streaming

Even if we forward the compact request as a regular `/v1/responses` request with `stream: true`, the upstream returns **Server-Sent Events (SSE)** format (`text/event-stream`). However, Codex's internal compact handler **expects non-streaming JSON** (`application/json`). When it receives SSE, it fails with:

```
Error running remote compact task: stream disconnected before completion:
expected value at line 1 column 1
```

This is a format mismatch: Codex's compact parsing code tries to `JSON.parse()` the SSE text, encounters `data: {`, and fails at character `d` (expected a JSON opening token).

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  Codex Desktop   │────▶│  codex-router    │────▶│  chat.cloudapi.vip   │
│  (127.0.0.1)     │     │  (127.0.0.1:     │     │  (One API / upstream)│
│                  │◀────│   18921)          │◀────│                      │
└─────────────────┘     └──────────────────┘     └──────────────────────┘

Codex config.toml:
  openai_base_url = "http://127.0.0.1:18921/v1"
```

The proxy sits between Codex Desktop and the upstream LLM service. It passes through normal requests unchanged, but intercepts compact requests and transforms them.

## How Compaction Works in Codex

1. Codex tracks the total token count of the conversation
2. When tokens exceed `model_auto_compact_token_limit` (default 20000 in our config), Codex triggers compaction
3. Codex builds an OpenAI Responses API request to `/v1/responses/compact` with:
   - The full conversation `input` array
   - The current system `instructions`
   - Model name (`gpt-5.4-xhigh`)
4. Codex expects a JSON response in the standard Responses API format
5. The response text replaces the conversation context window

## Our Solution: Interception + Rewriting

The proxy intercepts compact requests and performs a three-step transformation:

### Step 1: URL Rewrite

```javascript
// /v1/responses/compact → /v1/responses
upstreamPath = req.url
  .replace(/\/responses\/compact/, "/responses")
  .replace(/\/responses%2Fcompact/, "/responses");
```

### Step 2: Inject Compaction Instructions

We prepend a "senior engineer handoff notes" prompt to Codex's existing instructions. This tells the LLM to produce a dense, structured summary instead of a normal conversational response.

### Step 3: Ensure Streaming Mode

```javascript
if (json.stream === undefined) {
  json.stream = true;
}
```

**Why `stream: true`?** The One API relay at chat.cloudapi.vip requires `stream: true` to properly forward SSE responses from the underlying LLM provider. Without it, One API tries to JSON-parse the SSE stream and fails with "invalid character 'e' looking for beginning of value." With `stream: true`, One API passes through the SSE events correctly, and our proxy handles the SSE→JSON conversion.

### Step 4: SSE to JSON Conversion

After receiving the streaming SSE response from upstream, we parse it, extract the text content, and rebuild a standard non-streaming JSON response that Codex can parse.

## SSE to JSON Conversion

This is the **critical technical fix** that makes the pipeline work.

### SSE Event Types Received

| Event Type | Content |
|---|---|
| `response.created` | Response ID and model name |
| `response.output_text.delta` | Incremental text chunks |
| `response.completed` | Final status and usage statistics |
| `[DONE]` | Stream termination marker |

### Conversion Logic

```javascript
if (isCompact && contentType.includes("text/event-stream")) {
  let outputText = "";
  let outputUsage = null;
  let respId = "resp_compact";
  let respModel = json.model || "gpt-5.4";

  for (const line of responseText.split("\n")) {
    if (!line.startsWith("data:")) continue;
    const jsonStr = line.substring(5).trim();
    if (jsonStr === "[DONE]") continue;

    const evt = JSON.parse(jsonStr);
    if (evt.type === "response.created" && evt.response) {
      respId = evt.response.id;
      respModel = evt.response.model;
    }
    if (evt.type === "response.output_text.delta" && evt.delta) {
      outputText += evt.delta;
    }
    if (evt.type === "response.completed" && evt.response?.usage) {
      outputUsage = evt.response.usage;
    }
  }

  // Build non-streaming JSON response
  const compactJson = {
    id: respId,
    object: "response",
    status: "completed",
    model: respModel,
    output: [{
      type: "message",
      role: "assistant",
      content: [{ type: "output_text", text: outputText }]
    }],
    usage: outputUsage || { input_tokens: 0, output_tokens: 0, total_tokens: 0 }
  };

  // Replace response body and headers
  result.body = Buffer.from(JSON.stringify(compactJson), "utf8");
  result.headers["content-type"] = "application/json";
  result.headers["content-length"] = String(compactBody.length);
  delete result.headers["transfer-encoding"];
}
```

### What Codex Receives

```json
{
  "id": "resp_...",
  "object": "response",
  "status": "completed",
  "model": "gpt-5.4",
  "output": [
    {
      "type": "message",
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "What we were doing: ...\nFiles touched: ...\n..."
        }
      ]
    }
  ],
  "usage": {
    "input_tokens": 23213,
    "output_tokens": 349,
    "total_tokens": 23562
  }
}
```

## Compaction Prompt Strategy

We evaluated five prompt variants and selected the **"Senior Engineer Handoff Notes" (Role-Based)** approach:

### Selected Prompt (Production)

```
You are a senior engineer writing handoff notes after a pair-programming session.
Your colleague will continue the work using only these notes — they won't have
access to the full conversation.

Write notes as you would in a detailed PR comment or Slack handoff. Be direct,
specific, assume technical competence.

MUST include:
- What we were doing and where we got to
- Every file touched (exact paths)
- Decisions made and WHY (so they don't re-litigate)
- Errors hit and how we fixed them (so they don't repeat)
- What's left to do (prioritized)
- Landmines or gotchas ("don't run X before Y")

Natural but dense style. No fluff. If uncertain, say so explicitly.
```

### Why This Prompt?

1. **Role-based framing** produces more actionable output — the model writes for a colleague who needs to resume work
2. **"MUST include" checklist** ensures critical information isn't lost
3. **Anti-repetition instruction** ("so they don't re-litigate") prevents the next turn from re-debating settled decisions
4. **Landmines section** captures non-obvious pitfalls that purely extractive summaries miss
5. **Dense, no-fluff style** maximizes information density within the context budget

### Other Prompts Evaluated

| Variant | Approach | Weakness |
|---|---|---|
| Baseline | Simple "compress the conversation" | Too vague; loses decisions and rationale |
| Claude Code Style | `<summary>` tags with categories | Over-structured; wastes tokens on format |
| Structured Extraction | Explicit category extraction | Rigid format; misses cross-category context |
| Chain-of-Thought | Analyze-then-write | Unnecessary reasoning tokens; ~2x cost |
| **Role-Based** | **Senior engineer handoff** | **(Selected) Best balance of density and completeness** |

### Compression Performance

Typical results observed in production (from compact_logs):

- **Compression ratio**: 99.8-99.9% (e.g., ~26000 tokens in → ~350 tokens out)
- **Output chars**: 80-250 characters of dense summary
- **Duration**: 8-18 seconds
- **Cached tokens**: ~50% cache hit rate (conversation prefix is reused)

## Debug Logging System

The proxy writes detailed logs to `compact_logs/` for every compact-related event:

### Log File Types

| Prefix | Content |
|---|---|
| `detect_*.json` | Any request where URL or body contains "compact" (catch-all detection) |
| `compact_*.json` | Full compact request details: input count, chars, token estimate, instructions |
| `compact_resp_*.json` | Response details: status, content-type, output text, usage, compression ratio |
| `compact_debug_*.json` | Full request/response dump for 400+ error responses |

### compact_*.json Fields

```json
{
  "timestamp": "2026-05-21T05:55:08.790Z",
  "originalUrl": "/v1/responses/compact",
  "rewrittenUrl": "/v1/responses",
  "model": "gpt-5.4-xhigh",
  "stream": true,
  "instructions": "You are a senior engineer writing handoff notes...",
  "inputMessageCount": 45,
  "inputChars": 240000,
  "inputTokensEstimate": 60000,
  "rawBodyBytes": 250000,
  "rewrittenBodyBytes": 251200
}
```

### compact_resp_*.json Fields

```json
{
  "timestamp": "2026-05-21T05:55:17.280Z",
  "status": 200,
  "durationMs": 8493,
  "contentType": "application/json",
  "responseBytes": 604,
  "outputChars": 83,
  "outputPreview": "What we were doing: ...",
  "responseBodyPreview": "{\"id\":\"resp_...\",\"object\":\"response\"...",
  "usage": {
    "input_tokens": 23213,
    "output_tokens": 349,
    "total_tokens": 23562
  },
  "compressionRatio": "99.9%"
}
```

## Setup Guide

### Prerequisites

- Node.js (v18+ recommended, v24 tested)
- Codex Desktop installed
- Access to an upstream LLM API (OpenAI-compatible Responses API with streaming)

### 1. Clone the Repository

```bash
git clone https://github.com/BochaoLi/codex-router.git
cd codex-router
```

### 2. Configure the Proxy

Edit `proxy.js` and check these constants if needed:

```javascript
const LISTEN_PORT = 18921;
const TARGET_HOST = "chat.cloudapi.vip";
const TARGET_BASE = "https://chat.cloudapi.vip";
```

These are already set to the defaults that work with the One API relay.

### 3. Start the Proxy

```bash
node proxy.js
```

You should see:

```
[2026-05-21 16:00:00] Proxy running: http://127.0.0.1:18921 -> https://chat.cloudapi.vip
[2026-05-21 16:00:00] Model suffix rewriting: enabled
[2026-05-21 16:00:00] Stats:     GET  http://127.0.0.1:18921/__stats
```

### 4. Configure Codex Desktop

Edit `C:\Users\<username>\.codex\config.toml`:

```toml
model = "gpt-5.4-xhigh"
model_provider = "openai"
openai_base_url = "http://127.0.0.1:18921/v1"
model_context_window = 200000
model_auto_compact_token_limit = 20000
```

**Critical**: `openai_base_url` must point to the proxy's `/v1` path. Without this, Codex will bypass the proxy entirely.

**Critical**: `model_auto_compact_token_limit` controls when compaction triggers. A lower value (like 20000) causes more frequent compaction — useful for testing. For production, raise this to 180000 to reduce compaction frequency.

### 5. Restart Codex Desktop

Restart Codex Desktop for the config changes to take effect.

### 6. Verify the Setup

1. Browse to `http://127.0.0.1:18921/dashboard` to see the proxy dashboard
2. Send a message in Codex — you should see it logged in the proxy console
3. Check `compact_logs/` directory for `detect_*.json` files (confirms requests are flowing through)
4. To trigger an actual compact: have a long conversation (exceeding `model_auto_compact_token_limit`) or temporarily set `model_auto_compact_token_limit` to a very low value like `1000`

## Configuration Reference

### Codex config.toml

```toml
model = "gpt-5.4-xhigh"
model_provider = "openai"
model_reasoning_effort = "xhigh"
openai_base_url = "http://127.0.0.1:18921/v1"
model_context_window = 200000
model_auto_compact_token_limit = 20000
```

### Proxy Environment

No environment variables required. The proxy reads nothing from the environment — all configuration is in the source code constants at the top of `proxy.js`.

## Monitoring and Debugging

### Dashboard

- `http://127.0.0.1:18921/dashboard` — HTML dashboard
- `http://127.0.0.1:18921/__stats` — JSON statistics (uptime, cache hit rate, token totals)
- `http://127.0.0.1:18921/__recent?n=20` — Recent request log
- `http://127.0.0.1:18921/__errors` — Error log
- `POST http://127.0.0.1:18921/__reset` — Reset statistics

### Console Output

The proxy logs every compact interception and response to the console:

```
[2026-05-21 05:55:08] *** COMPACT INTERCEPTED ***
[2026-05-21 05:55:08]   URL: /v1/responses/compact -> /v1/responses
[2026-05-21 05:55:08]   Model: gpt-5.4-xhigh | Stream: true
[2026-05-21 05:55:08]   Input: 45 messages, 240000 chars (~60000 tokens)
[2026-05-21 05:55:17] *** COMPACT RESPONSE ***
[2026-05-21 05:55:17]   Status: 200 | 8493ms
[2026-05-21 05:55:17]   Output: 83 chars | Response: 604 bytes
[2026-05-21 05:55:17]   Compression: input 251200 bytes -> output 83 chars
```

### Log Files

All compact-related logs are written to `compact_logs/`:

```
compact_logs/
  detect_2026-05-21T04-43-01-848Z.json       # detect logs (all requests with "compact")
  compact_2026-05-21T04-43-07-088Z.json      # compact request logs
  compact_resp_2026-05-21T04-43-18-188Z.json  # compact response logs
  compact_debug_2026-05-21T04-44-16-749Z.json # error dumps (status >= 400)
```

### Troubleshooting

| Symptom | Check |
|---|---|
| No compact files at all | Is Codex pointed to `127.0.0.1:18921`? Check `config.toml` `openai_base_url` |
| Only `detect_*.json`, no `compact_*.json` | The proxy sees requests but they aren't compact paths. Normal. |
| `compact_resp_*.json` shows status 400 | Check `compact_debug_*.json` for the error body |
| Codex reports "stream disconnected" | The SSE→JSON conversion may have failed. Check `contentType` in response log — should be `application/json`, not `text/event-stream` |
| Proxy won't start (EADDRINUSE) | Another process is on port 18921. Kill it: `npx kill-port 18921` |
| Codex Desktop won't start after config change | Verify `config.toml` is valid UTF-8. Open in Notepad++ and check encoding. |

## Technical Details

### Model Name Suffix Stripping

The proxy also handles model name suffixes like `gpt-5.4-xhigh-openai-stream` by stripping the suffix to get the base model name. This is a separate feature from compact handling but runs in the same pipeline.

### Cache Header Detection

The proxy detects cache status from upstream response headers (`cf-cache-status` for Cloudflare, `x-cache` for others) and reports cache hit rates via the dashboard.

### Codex Reinstallation Caveats

When reinstalling Codex Desktop:
1. Delete only `C:\Users\<username>\AppData\Local\OpenAI\` (the application)
2. Keep `C:\Users\<username>\.codex\` (user data, conversations, config)
3. After reinstall, verify `config.toml` still has `openai_base_url = "http://127.0.0.1:18921/v1"`
4. The proxy does not need to be restarted after Codex reinstall

## License

MIT
