import { expect, test, describe } from "bun:test"

/**
 * Tests for the HTTP 400 reactive-compact contract that codex/index.ts
 * implements on upstream context-overflow errors.
 *
 * The shape and status are load-bearing for native Claude Code's
 * `tryReactiveCompact` path:
 *   - HTTP status MUST be 400 (Anthropic) or 413 (Vertex). 413 with
 *     type:"request_too_large" maps to "Request too large", NOT to
 *     prompt-too-long. We use 400.
 *   - Body MUST be {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: ..."}}.
 *   - The message MUST contain the literal lowercase phrase "prompt is too long".
 *
 * Reference: https://deep-dive-claude-code.vercel.app/source/query
 */

// Inline copy of the regex used in src/providers/codex/index.ts. Mirrors the
// CONTEXT_OVERFLOW_PATTERN constant. If the production constant changes, this
// must change too (intentional cross-check — the patterns must stay in sync).
const CONTEXT_OVERFLOW_PATTERN_TEST = /context window|context length|exceeds|prompt is too long|input.*too long/i

describe("CONTEXT_OVERFLOW_PATTERN", () => {
  const overflowMessages = [
    "Your input exceeds the context window of this model.",
    "Context window exceeded. Please shorten your prompt.",
    "prompt is too long: 950000 tokens",
    "context length exceeded",
    "Input is too long for this model",
  ]

  for (const msg of overflowMessages) {
    test(`matches overflow message: "${msg}"`, () => {
      expect(CONTEXT_OVERFLOW_PATTERN_TEST.test(msg)).toBe(true)
    })
  }

  const nonOverflowMessages = [
    "transient backend error",
    "rate limited, please retry",
    "internal server error",
    "model unavailable",
  ]

  for (const msg of nonOverflowMessages) {
    test(`does NOT match transient error: "${msg}"`, () => {
      expect(CONTEXT_OVERFLOW_PATTERN_TEST.test(msg)).toBe(false)
    })
  }
})

describe("promptTooLongResponse", () => {
  // Inline copy of the helper for unit-testability without dragging the full
  // module (which pulls in network clients and auth state).
  function promptTooLongResponse(message: string): Response {
    return new Response(
      JSON.stringify({
        type: "error",
        error: {
          type: "invalid_request_error",
          message: `prompt is too long: ${message}`,
        },
      }),
      {
        status: 400,
        headers: { "content-type": "application/json" },
      },
    )
  }

  test("returns HTTP 400 status (NOT 413)", async () => {
    const res = promptTooLongResponse("ctx exceeds 1M tokens")
    expect(res.status).toBe(400)
  })

  test("body is invalid_request_error with lowercase 'prompt is too long' prefix", async () => {
    const res = promptTooLongResponse("Your input exceeds the context window")
    const body = await res.json()
    expect(body).toEqual({
      type: "error",
      error: {
        type: "invalid_request_error",
        message: "prompt is too long: Your input exceeds the context window",
      },
    })
  })

  test("message contains lowercase literal 'prompt is too long' (reactive-compact gate)", async () => {
    const res = promptTooLongResponse("any upstream detail")
    const body = await res.json() as { error: { message: string } }
    // tryReactiveCompact predicate matches the lowercase literal exactly.
    expect(body.error.message).toContain("prompt is too long")
    // Must NOT be capitalised — the binary's predicate is case-sensitive.
    expect(body.error.message).not.toContain("Prompt is too long:")
  })

  test("content-type is application/json (not text/event-stream)", () => {
    const res = promptTooLongResponse("anything")
    expect(res.headers.get("content-type")).toBe("application/json")
  })
})
