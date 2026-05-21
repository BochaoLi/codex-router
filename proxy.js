const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

const LISTEN_PORT = 18921;
const TARGET_HOST = "chat.cloudapi.vip";
const TARGET_BASE = "https://chat.cloudapi.vip";

// ── Statistics ──────────────────────────────────────────────
const stats = {
  startedAt: new Date().toISOString(),
  totalRequests: 0,
  totalInputBytes: 0,
  totalOutputBytes: 0,
  totalPromptTokens: 0,
  totalCompletionTokens: 0,
  cacheHits: 0,
  cacheMisses: 0,
  rewrittenModels: 0,
  statusCodes: {},
  endpoints: {},
  models: {},
  recent: [],
  errors: [],
};

function timestamp() {
  return new Date().toISOString().replace("T", " ").substring(0, 19);
}

function recordStat(entry) {
  stats.totalRequests++;
  stats.totalInputBytes += entry.inBytes;
  stats.totalOutputBytes += entry.outBytes;
  stats.totalPromptTokens += entry.promptTokens || 0;
  stats.totalCompletionTokens += entry.completionTokens || 0;

  const sc = String(entry.status);
  stats.statusCodes[sc] = (stats.statusCodes[sc] || 0) + 1;

  const ep = entry.path;
  if (!stats.endpoints[ep]) {
    stats.endpoints[ep] = { requests: 0, inputBytes: 0, outputBytes: 0, promptTokens: 0, completionTokens: 0 };
  }
  stats.endpoints[ep].requests++;
  stats.endpoints[ep].inputBytes += entry.inBytes;
  stats.endpoints[ep].outputBytes += entry.outBytes;
  stats.endpoints[ep].promptTokens += entry.promptTokens || 0;
  stats.endpoints[ep].completionTokens += entry.completionTokens || 0;

  if (entry.model) {
    stats.models[entry.model] = (stats.models[entry.model] || 0) + 1;
  }

  if (entry.cache === "hit") stats.cacheHits++;
  if (entry.cache === "miss") stats.cacheMisses++;
  if (entry.rewritten) stats.rewrittenModels++;

  stats.recent.unshift(entry);
  if (stats.recent.length > 200) stats.recent.length = 200;

  if (entry.errorBody) {
    stats.errors.unshift({
      time: entry.time,
      path: entry.path,
      status: entry.status,
      body: entry.errorBody.substring(0, 500),
    });
    if (stats.errors.length > 50) stats.errors.length = 50;
  }
}

// ── Model name suffix stripping ──────────────────────────────
function mapModelName(model) {
  const m = model.match(/^(.+)-openai-(.+)$/);
  if (m) {
    return { original: model, mapped: m[1], operation: m[2] };
  }
  return { original: model, mapped: model, operation: null };
}

// ── Cache header detection ──────────────────────────────────
function detectCache(headers) {
  const cf = headers["cf-cache-status"];
  if (cf) return cf.toLowerCase() === "hit" ? "hit" : "miss";
  const xCache = headers["x-cache"] || headers["x-cache-status"];
  if (xCache) return xCache.toLowerCase().includes("hit") ? "hit" : "miss";
  return null;
}

// ── Compact request rewriting ───────────────────────────────
// Codex sends /v1/responses/compact to compress conversation context.
// If upstream doesn't support it, rewrite as a regular chat request
// that asks the model to summarize the conversation.
function isCompactPath(url) {
  return url.includes("/responses/compact") || url.includes("/responses%2Fcompact");
}

function rewriteCompactRequest(json) {
  const input = json.input;
  let conversationText;
  if (typeof input === "string") {
    conversationText = input;
  } else if (Array.isArray(input)) {
    conversationText = input
      .map((m) => `[${m.role || "unknown"}]: ${typeof m.content === "string" ? m.content : JSON.stringify(m.content)}`)
      .join("\n");
  } else {
    conversationText = JSON.stringify(input);
  }

  return {
    ...json,
    instructions: (json.instructions ? json.instructions + "\n\n" : "") +
      "Compress the following conversation into a concise structured summary. " +
      "Preserve all key information: decisions, action items, code patterns, file paths, " +
      "error messages, and important context. Output ONLY the compressed transcript.",
    input: [
      { role: "user", content: `Compress this conversation:\n\n${conversationText}` }
    ],
  };
}

// ── Read request body ────────────────────────────────────────
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

// ── Forward a single request to upstream ─────────────────────
function forwardToUpstream(reqUrl, method, reqHeaders, body) {
  return new Promise((resolve, reject) => {
    const targetUrl = new URL(reqUrl, TARGET_BASE);
    const options = {
      hostname: TARGET_HOST,
      port: 443,
      path: targetUrl.pathname + targetUrl.search,
      method,
      headers: {
        ...reqHeaders,
        host: TARGET_HOST,
        "content-length": String(body.length),
      },
    };
    delete options.headers["connection"];

    const proxyReq = https.request(options, (proxyRes) => {
      const chunks = [];
      proxyRes.on("data", (c) => chunks.push(c));
      proxyRes.on("end", () => {
        resolve({
          statusCode: proxyRes.statusCode,
          headers: proxyRes.headers,
          body: Buffer.concat(chunks),
        });
      });
    });
    proxyReq.on("error", reject);
    proxyReq.write(body);
    proxyReq.end();
  });
}

// ── Parse JSON body, return null if not JSON ─────────────────
function parseJsonBody(buf) {
  try {
    return JSON.parse(buf.toString("utf8"));
  } catch (_) {
    return null;
  }
}

// ── Extract token usage from upstream response ──────────────
// Handles both regular JSON and SSE streaming responses.
function extractTokensFromResponse(body, contentType) {
  const text = body.toString("utf8");
  if (!text) return { promptTokens: 0, completionTokens: 0 };

  // SSE streaming: find the last data: chunk that contains "usage"
  if (contentType && contentType.includes("text/event-stream")) {
    const lines = text.split("\n");
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i].trim();
      if (line.startsWith("data:") && line.includes('"usage"')) {
        try {
          const json = JSON.parse(line.substring(5).trim());
          const u = json.usage;
          if (u) {
            return {
              promptTokens: u.prompt_tokens || u.input_tokens || 0,
              completionTokens: u.completion_tokens || u.output_tokens || 0,
            };
          }
        } catch (_) {}
        break;
      }
    }
    return { promptTokens: 0, completionTokens: 0 };
  }

  // Regular JSON response
  try {
    const json = JSON.parse(text);
    const u = json.usage;
    if (u) {
      return {
        promptTokens: u.prompt_tokens || u.input_tokens || 0,
        completionTokens: u.completion_tokens || u.output_tokens || 0,
      };
    }
  } catch (_) {}
  return { promptTokens: 0, completionTokens: 0 };
}

// ── Server ──────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const startTime = Date.now();

  // ── Dashboard ────────────────────────────────────────────
  if (req.method === "GET" && (req.url === "/" || req.url === "/dashboard")) {
    try {
      const html = fs.readFileSync(path.join(__dirname, "dashboard.html"), "utf8");
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(html);
    } catch (_) {
      res.writeHead(500);
      res.end("Dashboard not found");
    }
    return;
  }

  // ── Internal endpoints ───────────────────────────────────
  if (req.method === "GET" && req.url === "/__stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    const summary = {
      ...stats,
      uptimeSeconds: Math.round((Date.now() - new Date(stats.startedAt).getTime()) / 1000),
      cacheHitRate:
        stats.cacheHits + stats.cacheMisses > 0
          ? ((stats.cacheHits / (stats.cacheHits + stats.cacheMisses)) * 100).toFixed(1) + "%"
          : "N/A",
      recent: undefined,
    };
    res.end(JSON.stringify(summary, null, 2));
    return;
  }

  if (req.method === "GET" && req.url.startsWith("/__recent")) {
    const url = new URL(req.url, "http://localhost");
    const n = parseInt(url.searchParams.get("n") || "20");
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(stats.recent.slice(0, n), null, 2));
    return;
  }

  if (req.method === "GET" && req.url === "/__errors") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(stats.errors, null, 2));
    return;
  }

  if (req.method === "POST" && req.url === "/__reset") {
    stats.totalRequests = 0;
    stats.totalInputBytes = 0;
    stats.totalOutputBytes = 0;
    stats.totalPromptTokens = 0;
    stats.totalCompletionTokens = 0;
    stats.cacheHits = 0;
    stats.cacheMisses = 0;
    stats.rewrittenModels = 0;
    stats.statusCodes = {};
    stats.endpoints = {};
    stats.models = {};
    stats.recent = [];
    stats.errors = [];
    stats.startedAt = new Date().toISOString();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, message: "Stats reset" }));
    console.log(`[${timestamp()}] Stats reset`);
    return;
  }

  // ── Read request body ────────────────────────────────────
  let rawBody;
  try {
    rawBody = await readBody(req);
  } catch (_) {
    res.writeHead(400);
    res.end("Bad Request");
    return;
  }

  // ── ALL-REQUEST VERBOSE LOG ─────────────────────────────
  const json = parseJsonBody(rawBody);
  const reqModel = json ? json.model : null;
  const reqStream = json ? json.stream : null;
  const hasInstructions = json ? !!json.instructions : false;
  const inputCount = json && Array.isArray(json.input) ? json.input.length : 0;
  console.log(`[${timestamp()}] >> ${req.method} ${req.url} | model=${reqModel} stream=${reqStream} instructions=${hasInstructions} inputItems=${inputCount} body=${rawBody.length}b`);
  // If URL contains "compact" anywhere, or body contains "compact", flag it
  if (req.url.toLowerCase().includes("compact") || (rawBody.length < 500000 && rawBody.toString().toLowerCase().includes("compact"))) {
    console.log(`[${timestamp()}] !! COMPACT KEYWORD DETECTED in URL or body !!`);
    const logDir = path.join(__dirname, "compact_logs");
    try { fs.mkdirSync(logDir, { recursive: true }); } catch (_) {}
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    fs.writeFileSync(path.join(logDir, `detect_${ts}.json`), JSON.stringify({
      url: req.url,
      method: req.method,
      model: reqModel,
      bodyLength: rawBody.length,
      bodyPreview: rawBody.toString("utf8").substring(0, 2000),
    }, null, 2));
  }
  // ── END ALL-REQUEST VERBOSE LOG ─────────────────────────
  let originalModel = null;
  let baseModel = null;
  let rewritten = false;
  let bodyAfterSuffix = rawBody;
  let isCompact = isCompactPath(req.url);

  // ── Handle compact requests: rewrite /responses/compact -> /responses ──
  // chat.cloudapi.vip doesn't support the /v1/responses/compact endpoint.
  // Codex's compact.rs processes the response the same way as a regular turn
  // (consuming OutputItemDone, Completed, etc. streaming events). So we:
  //   1. Keep the original input items (do NOT convert to a giant string)
  //   2. Add summarization instructions
  //   3. Forward as a regular /v1/responses streaming request
  let upstreamPath = req.url;
  if (isCompact && json) {
    upstreamPath = req.url.replace(/\/responses\/compact/, "/responses")
                         .replace(/\/responses%2Fcompact/, "/responses");
    // Ensure stream is set (normal requests have it, compact may not)
    if (json.stream === undefined) {
      json.stream = true;
    }
    if (json.instructions) {
      json.instructions = "You are a senior engineer writing handoff notes after a pair-programming session. Your colleague will continue the work using only these notes — they won't have access to the full conversation.\n\nWrite notes as you would in a detailed PR comment or Slack handoff. Be direct, specific, assume technical competence.\n\nMUST include:\n- What we were doing and where we got to\n- Every file touched (exact paths)\n- Decisions made and WHY (so they don't re-litigate)\n- Errors hit and how we fixed them (so they don't repeat)\n- What's left to do (prioritized)\n- Landmines or gotchas (\"don't run X before Y\")\n\nNatural but dense style. No fluff. If uncertain, say so explicitly.\n\n" + json.instructions;
    } else {
      json.instructions = "You are a senior engineer writing handoff notes after a pair-programming session. Your colleague will continue the work using only these notes — they won't have access to the full conversation.\n\nWrite notes as you would in a detailed PR comment or Slack handoff. Be direct, specific, assume technical competence.\n\nMUST include:\n- What we were doing and where we got to\n- Every file touched (exact paths)\n- Decisions made and WHY (so they don't re-litigate)\n- Errors hit and how we fixed them (so they don't repeat)\n- What's left to do (prioritized)\n- Landmines or gotchas (\"don't run X before Y\")\n\nNatural but dense style. No fluff. If uncertain, say so explicitly.";
    }
    bodyAfterSuffix = Buffer.from(JSON.stringify(json), "utf8");
    rewritten = true;

    // ── COMPACT INTERCEPT LOG ──────────────────────────────────
    const logDir = path.join(__dirname, "compact_logs");
    try { fs.mkdirSync(logDir, { recursive: true }); } catch (_) {}
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    const logFile = path.join(logDir, `compact_${ts}.json`);
    const inputItems = json.input || [];
    const inputChars = JSON.stringify(inputItems).length;
    const inputMsgCount = Array.isArray(inputItems) ? inputItems.length : 0;
    const logData = {
      timestamp: new Date().toISOString(),
      originalUrl: req.url,
      rewrittenUrl: upstreamPath,
      model: json.model,
      stream: json.stream,
      instructions: (json.instructions || "").substring(0, 500),
      inputMessageCount: inputMsgCount,
      inputChars,
      inputTokensEstimate: Math.round(inputChars / 4),
      rawBodyBytes: rawBody.length,
      rewrittenBodyBytes: bodyAfterSuffix.length,
      firstInputPreview: inputItems.length > 0
        ? JSON.stringify(inputItems[0]).substring(0, 1000)
        : null,
      lastInputPreview: inputItems.length > 1
        ? JSON.stringify(inputItems[inputItems.length - 1]).substring(0, 1000)
        : null,
    };
    fs.writeFileSync(logFile, JSON.stringify(logData, null, 2), "utf8");
    console.log(`[${timestamp()}] *** COMPACT INTERCEPTED ***`);
    console.log(`[${timestamp()}]   URL: ${req.url} -> ${upstreamPath}`);
    console.log(`[${timestamp()}]   Model: ${json.model} | Stream: ${json.stream}`);
    console.log(`[${timestamp()}]   Input: ${inputMsgCount} messages, ${inputChars} chars (~${Math.round(inputChars/4)} tokens)`);
    console.log(`[${timestamp()}]   Raw body: ${rawBody.length} bytes -> Rewritten: ${bodyAfterSuffix.length} bytes`);
    console.log(`[${timestamp()}]   Log saved: ${logFile}`);
    // ── END COMPACT INTERCEPT LOG ──────────────────────────────
  }

  if (json && json.model) {
    originalModel = json.model;
    const mapped = mapModelName(json.model);
    if (mapped.operation) {
      baseModel = mapped.mapped;
      json.model = mapped.mapped;
      rewritten = true;
      console.log(`[${timestamp()}] suffix-strip: ${mapped.original} -> ${mapped.mapped} (op: ${mapped.operation})`);
    } else {
      baseModel = json.model;
    }

    if (rewritten) {
      bodyAfterSuffix = Buffer.from(JSON.stringify(json), "utf8");
    }
  }

  // ── Step 2: first attempt ────────────────────────────────
  let result;
  try {
    result = await forwardToUpstream(upstreamPath, req.method, req.headers, bodyAfterSuffix);
  } catch (err) {
    console.error(`[${timestamp()}] Proxy error:`, err.message);
    const entry = {
      time: new Date().toISOString(),
      method: req.method,
      path: req.url,
      model: originalModel,
      rewritten: rewritten ? baseModel : null,
      inBytes: bodyAfterSuffix.length,
      outBytes: 0,
      promptTokens: 0,
      completionTokens: 0,
      status: 502,
      cache: null,
      durationMs: Date.now() - startTime,
    };
    recordStat(entry);
    res.writeHead(502);
    res.end("Bad Gateway");
    return;
  }

  // ── Step 3: send response back to client ─────────────────
  const responseText = result.body.toString("utf8");
  const cache = detectCache(result.headers);
  const contentType = result.headers["content-type"] || "";

  // For compact requests: convert SSE -> non-streaming JSON so Codex's
  // compact handler can parse it (it expects JSON, not text/event-stream).
  let outputText = "";
  let outputUsage = null;
  if (isCompact && contentType.includes("text/event-stream")) {
    let respId = "resp_compact";
    let respModel = json.model || "gpt-5.4";
    const lines = responseText.split("\n");
    for (const line of lines) {
      if (!line.startsWith("data:")) continue;
      const jsonStr = line.substring(5).trim();
      if (jsonStr === "[DONE]") continue;
      try {
        const evt = JSON.parse(jsonStr);
        if (evt.type === "response.created" && evt.response) {
          if (evt.response.id) respId = evt.response.id;
          if (evt.response.model) respModel = evt.response.model;
        }
        if (evt.type === "response.output_text.delta" && evt.delta) outputText += evt.delta;
        if (evt.type === "response.completed" && evt.response && evt.response.usage) outputUsage = evt.response.usage;
        if (evt.usage) outputUsage = evt.usage;
      } catch (_) {}
    }
    // Build a non-streaming JSON response that Codex can parse
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
    const compactBody = Buffer.from(JSON.stringify(compactJson), "utf8");
    result.body = compactBody;
    // Replace content-type so Codex sees application/json
    const newHeaders = { ...result.headers };
    newHeaders["content-type"] = "application/json";
    newHeaders["content-length"] = String(compactBody.length);
    delete newHeaders["transfer-encoding"];
    result.headers = newHeaders;
  }

  res.writeHead(result.statusCode, result.headers);
  res.end(result.body);

  // ── COMPACT RESPONSE LOG ─────────────────────────────────
  if (isCompact) {
    const logDir = path.join(__dirname, "compact_logs");
    const ts2 = new Date().toISOString().replace(/[:.]/g, "-");
    const respLogFile = path.join(logDir, `compact_resp_${ts2}.json`);
    const elapsed = Date.now() - startTime;
    const contentTypeLog = result.headers["content-type"] || "";

    if (contentType.includes("text/event-stream") && !outputText) {
      // Parse SSE for logging if we didn't already do it above
      const lines = responseText.split("\n");
      for (const line of lines) {
        if (!line.startsWith("data:")) continue;
        const jsonStr = line.substring(5).trim();
        if (jsonStr === "[DONE]") continue;
        try {
          const evt = JSON.parse(jsonStr);
          if (evt.type === "response.output_text.delta" && evt.delta) outputText += evt.delta;
          if (evt.type === "response.completed" && evt.response && evt.response.usage) outputUsage = evt.response.usage;
          if (evt.usage) outputUsage = evt.usage;
        } catch (_) {}
      }
    } else if (contentTypeLog.includes("application/json") && outputText) {
      // Already parsed above, outputText and outputUsage are set
    } else {
      try {
        const rj = JSON.parse(responseText);
        if (rj.output && Array.isArray(rj.output)) {
          for (const item of rj.output) {
            if (item.type === "message" && item.content) {
              for (const c of item.content) {
                if (c.type === "output_text" && c.text) outputText += c.text;
                else if (c.text) outputText += c.text;
              }
            }
          }
        }
        if (rj.usage) outputUsage = rj.usage;
      } catch (_) {}
    }

    // Use the actual content-type being sent (may differ from original after conversion)
    const actualContentType = result.headers["content-type"] || contentType;
    const actualBody = result.body.toString("utf8");
    const respLog = {
      timestamp: new Date().toISOString(),
      status: result.statusCode,
      durationMs: elapsed,
      contentType: actualContentType,
      responseBytes: result.body.length,
      outputChars: outputText.length,
      outputPreview: outputText.substring(0, 3000),
      responseBodyPreview: actualBody.substring(0, 500),
      usage: outputUsage,
      compressionRatio: bodyAfterSuffix.length > 0 ? ((1 - outputText.length / bodyAfterSuffix.length) * 100).toFixed(1) + "%" : null,
    };
    try { fs.writeFileSync(respLogFile, JSON.stringify(respLog, null, 2), "utf8"); } catch (_) {}
    console.log(`[${timestamp()}] *** COMPACT RESPONSE ***`);
    console.log(`[${timestamp()}]   Status: ${result.statusCode} | ${elapsed}ms`);
    console.log(`[${timestamp()}]   Output: ${outputText.length} chars | Response: ${result.body.length} bytes`);
    if (outputUsage) console.log(`[${timestamp()}]   Tokens: in=${outputUsage.input_tokens||0} out=${outputUsage.output_tokens||0}`);
    console.log(`[${timestamp()}]   Compression: input ${bodyAfterSuffix.length} bytes -> output ${outputText.length} chars`);
    console.log(`[${timestamp()}]   Log: ${respLogFile}`);

    // ── COMPACT ERROR DUMP ──────────────────────────────────
    if (result.statusCode >= 400) {
      const dumpFile = path.join(logDir, `compact_debug_${ts2}.json`);
      const dump = {
        timestamp: new Date().toISOString(),
        status: result.statusCode,
        requestUrl: upstreamPath,
        requestModel: json.model,
        requestHeaders: req.headers,
        requestBody: bodyAfterSuffix.toString("utf8").substring(0, 20000),
        requestBodyFullBytes: bodyAfterSuffix.length,
        responseHeaders: result.headers,
        responseBody: responseText.substring(0, 5000),
        responseBodyFullBytes: result.body.length,
      };
      try { fs.writeFileSync(dumpFile, JSON.stringify(dump, null, 2), "utf8"); } catch (_) {}
      console.log(`[${timestamp()}]   Debug dump: ${dumpFile}`);
    }
    // ── END COMPACT ERROR DUMP ──────────────────────────────
  }
  // ── END COMPACT RESPONSE LOG ─────────────────────────────

  const isError = result.statusCode >= 400;
  const tokens = isError
    ? { promptTokens: 0, completionTokens: 0 }
    : extractTokensFromResponse(result.body, result.headers["content-type"] || "");
  const entry = {
    time: new Date().toISOString(),
    method: req.method,
    path: req.url,
    model: originalModel || baseModel || null,
    rewritten: rewritten ? baseModel : null,
    inBytes: bodyAfterSuffix.length,
    outBytes: result.body.length,
    promptTokens: tokens.promptTokens,
    completionTokens: tokens.completionTokens,
    status: result.statusCode,
    cache,
    durationMs: Date.now() - startTime,
    errorBody: isError ? responseText : null,
  };
  recordStat(entry);

  if (isError) {
    const preview = responseText.substring(0, 200);
    const sentBody = bodyAfterSuffix.toString("utf8").substring(0, 300);
    console.log(`[${timestamp()}] ${result.statusCode} ${req.method} ${req.url} model=${entry.model} in=${bodyAfterSuffix.length} out=${result.body.length} cache=${cache || "-"} ${Date.now() - startTime}ms`);
    console.log(`[${timestamp()}]   sent: ${sentBody}`);
    console.log(`[${timestamp()}]   recv: ${preview}`);
  }
});

server.listen(LISTEN_PORT, "127.0.0.1", () => {
  console.log(`[${timestamp()}] Proxy running: http://127.0.0.1:${LISTEN_PORT} -> ${TARGET_BASE}`);
  console.log(`[${timestamp()}] Model suffix rewriting: enabled`);
  console.log(`[${timestamp()}] Stats:     GET  http://127.0.0.1:${LISTEN_PORT}/__stats`);
  console.log(`[${timestamp()}] Recent:    GET  http://127.0.0.1:${LISTEN_PORT}/__recent?n=20`);
  console.log(`[${timestamp()}] Errors:    GET  http://127.0.0.1:${LISTEN_PORT}/__errors`);
  console.log(`[${timestamp()}] Reset:     POST http://127.0.0.1:${LISTEN_PORT}/__reset`);
  console.log(`[${timestamp()}] Press Ctrl+C to stop`);
});
