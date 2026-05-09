import { expect, test, describe } from "bun:test"
import type { Logger } from "../../../log.ts"
import { translateStream } from "./stream.ts"

/**
 * Regression tests for mid-stream context-overflow handling.
 *
 * Preflight tee in providers/codex/index.ts returns HTTP 400
 * invalid_request_error BEFORE SSE headers commit. But when upstream emits
 * one or more normal events first and THEN errors with a context-overflow
 * message, HTTP 200 SSE is already on the wire — translateStream must emit
 * an Anthropic-shaped `event: error` (type: invalid_request_error, message
 * prefixed with literal "prompt is too long") and call `controller.error()`
 * to terminate the response as a protocol error. Claude Code's
 * `tryReactiveCompact` only fires on this shape — a synthetic 200 SSE with
 * a graceful `end_turn` message_delta is rendered as model output and never
 * triggers reactive compact.
 */

const noopLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  child() {
    return noopLogger
  },
}

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s)
}

/**
 * Build an upstream that emits a `response.output_item.added` text block
 * (so messageStarted becomes true), then a `response.failed` event with a
 * context-overflow message that matches CONTEXT_OVERFLOW_PATTERN.
 */
function midStreamOverflowUpstream(message: string): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      // Emit one text-delta-shaped event so the reducer transitions to
      // text-start → text-delta and the translateStream emits message_start.
      const created = {
        type: "response.created",
        response: { id: "resp_test" },
      }
      controller.enqueue(encode(`event: response.created\ndata: ${JSON.stringify(created)}\n\n`))
      const itemAdded = {
        type: "response.output_item.added",
        output_index: 0,
        item: { type: "message", id: "msg_test", role: "assistant", content: [] },
      }
      controller.enqueue(encode(`event: response.output_item.added\ndata: ${JSON.stringify(itemAdded)}\n\n`))
      const partAdded = {
        type: "response.content_part.added",
        item_id: "msg_test",
        output_index: 0,
        content_index: 0,
        part: { type: "output_text", text: "" },
      }
      controller.enqueue(encode(`event: response.content_part.added\ndata: ${JSON.stringify(partAdded)}\n\n`))
      const delta = {
        type: "response.output_text.delta",
        item_id: "msg_test",
        output_index: 0,
        content_index: 0,
        delta: "hello",
      }
      controller.enqueue(encode(`event: response.output_text.delta\ndata: ${JSON.stringify(delta)}\n\n`))
      // Now upstream fails with context-overflow.
      const failed = {
        type: "response.failed",
        response: {
          status: "failed",
          error: { code: "context_window_exceeded", message },
        },
      }
      controller.enqueue(encode(`event: response.failed\ndata: ${JSON.stringify(failed)}\n\n`))
      controller.close()
    },
  })
}

async function collectWithError(
  stream: ReadableStream<Uint8Array>,
): Promise<{ sse: string; errored: boolean; errorMessage?: string }> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let out = ""
  let errored = false
  let errorMessage: string | undefined
  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      out += decoder.decode(value, { stream: true })
    }
    out += decoder.decode()
  } catch (err) {
    errored = true
    errorMessage = err instanceof Error ? err.message : String(err)
  }
  return { sse: out, errored, errorMessage }
}

describe("translateStream mid-stream context-overflow", () => {
  test("emits event: error with invalid_request_error and 'prompt is too long' literal", async () => {
    const upstream = midStreamOverflowUpstream(
      "Your input exceeds the context window of this model.",
    )
    const { sse, errored, errorMessage } = await collectWithError(
      translateStream(upstream, {
        messageId: "msg_test",
        model: "gpt-5.4",
        log: noopLogger,
      }),
    )

    // Stream must emit an Anthropic-shaped error event (NOT a synthetic
    // graceful end_turn message_delta).
    expect(sse).toContain("event: error")

    // Extract the JSON payload after `data: ` on the line(s) following
    // `event: error`.
    const errorBlockMatch = sse.match(/event: error\ndata: (\{[^\n]*\})/)
    expect(errorBlockMatch).not.toBeNull()
    const payloadJson = errorBlockMatch?.[1] ?? "{}"
    const payload = JSON.parse(payloadJson)
    expect(payload).toEqual({
      type: "error",
      error: {
        type: "invalid_request_error",
        message: expect.stringContaining("prompt is too long"),
      },
    })
    expect(payload.error.message).toContain(
      "Your input exceeds the context window",
    )

    // Stream must NOT emit a synthetic graceful end_turn for context-overflow
    // (that's the bug — Claude Code renders it as model output).
    // The error path returns BEFORE emitting message_delta with end_turn.
    // (message_start may or may not have been emitted depending on whether
    // upstream already sent a delta — both are acceptable as long as we end
    // with `event: error` + controller.error.)
    const errorIdx = sse.indexOf("event: error")
    const messageDeltaIdx = sse.indexOf('"stop_reason":"end_turn"')
    if (messageDeltaIdx !== -1) {
      // If a message_delta exists, it must come BEFORE event: error from
      // legitimate prior content — but for the overflow path we should not
      // see a synthetic end_turn AFTER the error.
      expect(messageDeltaIdx).toBeLessThan(errorIdx)
    }

    // controller.error() must have been called → reader observes a stream
    // error during read.
    expect(errored).toBe(true)
    expect(errorMessage ?? "").toContain("prompt is too long")
  })

  test("non-overflow upstream errors emit event: error (strict upstream shape)", async () => {
    // Simulate a transient upstream failure mid-stream (no context-overflow
    // keywords). Per upstream PR #1 strict shape (Rajiv 2026-05-09 12:49 IST),
    // emits `event: error` with api_error — no synthetic graceful end_turn
    // message_delta. Tradeoff documented in
    // feedback_raine_strict_error_shape_dropped_synthetic_recovery.md.
    const upstream = new ReadableStream<Uint8Array>({
      start(controller) {
        const created = {
          type: "response.created",
          response: { id: "resp_test" },
        }
        controller.enqueue(encode(`event: response.created\ndata: ${JSON.stringify(created)}\n\n`))
        const itemAdded = {
          type: "response.output_item.added",
          output_index: 0,
          item: { type: "message", id: "msg_test", role: "assistant", content: [] },
        }
        controller.enqueue(encode(`event: response.output_item.added\ndata: ${JSON.stringify(itemAdded)}\n\n`))
        const partAdded = {
          type: "response.content_part.added",
          item_id: "msg_test",
          output_index: 0,
          content_index: 0,
          part: { type: "output_text", text: "" },
        }
        controller.enqueue(encode(`event: response.content_part.added\ndata: ${JSON.stringify(partAdded)}\n\n`))
        const delta = {
          type: "response.output_text.delta",
          item_id: "msg_test",
          output_index: 0,
          content_index: 0,
          delta: "hello",
        }
        controller.enqueue(encode(`event: response.output_text.delta\ndata: ${JSON.stringify(delta)}\n\n`))
        const failed = {
          type: "response.failed",
          response: {
            status: "failed",
            error: { code: "internal_error", message: "transient backend error" },
          },
        }
        controller.enqueue(encode(`event: response.failed\ndata: ${JSON.stringify(failed)}\n\n`))
        controller.close()
      },
    })
    const { sse, errored } = await collectWithError(
      translateStream(upstream, {
        messageId: "msg_test",
        model: "gpt-5.4",
        log: noopLogger,
      }),
    )

    // Strict upstream shape — emit event: error with api_error type. Stream
    // closes cleanly (no controller.error) because non-overflow errors are
    // not protocol errors.
    expect(sse).toContain("event: error")
    expect(sse).toContain('"type":"api_error"')
    expect(sse).toContain("transient backend error")
    expect(errored).toBe(false)
    // Must NOT emit synthetic message_delta with end_turn — that was the
    // dropped synthetic-recovery path.
    expect(sse).not.toContain('"stop_reason":"end_turn"')
    // Must NOT carry the prompt-is-too-long literal — that's the overflow
    // branch only.
    expect(sse).not.toContain("prompt is too long")
  })
})
