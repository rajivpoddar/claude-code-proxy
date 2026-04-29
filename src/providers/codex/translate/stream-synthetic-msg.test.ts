import { expect, test } from "bun:test"
import type { Logger } from "../../../log.ts"
import { translateStream } from "./stream.ts"

const noopLogger: Logger = {
  debug() {}, info() {}, warn() {}, error() {},
  child() { return noopLogger },
}

function upstreamWithFailedEvent(message: string): ReadableStream<Uint8Array> {
  // Emits a Codex `response.failed` event which the reducer throws as
  // UpstreamStreamError (kind: "failed"). This drives the synthetic-message
  // recovery path in translateStream.
  //
  // Context-overflow errors are now intercepted upstream of translateStream
  // (preflight tee in providers/codex/index.ts → HTTP 400 response) so the
  // tests here cover transient/non-overflow failures only. Synthetic SSE
  // recovery still runs to keep the client's usage accumulator consistent
  // when an upstream chunk fails mid-flight.
  return new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder()
      const evt = `event: response.failed\ndata: ${JSON.stringify({ type: "response.failed", response: { error: { message } } })}\n\n`
      controller.enqueue(encoder.encode(evt))
      controller.close()
    },
  })
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let out = ""
  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    out += decoder.decode(value, { stream: true })
  }
  out += decoder.decode()
  return out
}

test("translateStream emits synthetic complete message on pre-content UpstreamStreamError", async () => {
  const sse = await collect(
    translateStream(upstreamWithFailedEvent("transient backend hiccup, please retry"), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  // Synthetic complete message keeps Claude Code's usage accumulator healthy
  expect(sse).toContain("event: message_start")
  expect(sse).toContain("event: content_block_delta")
  expect(sse).toContain("[upstream error]")
  expect(sse).toContain("transient backend hiccup")
  expect(sse).toContain("event: message_delta")
  expect(sse).toContain("\"stop_reason\":\"end_turn\"")
  expect(sse).toContain("event: message_stop")
})

test("translateStream emits event: error and controller.error on context-overflow (mid-stream)", async () => {
  // Mid-stream overflow path: when preflight didn't catch the overflow (e.g.,
  // upstream emitted some content then errored with a context-window message),
  // translateStream must emit an Anthropic-shaped `event: error` with
  // type:"invalid_request_error" + literal "prompt is too long" prefix and
  // call controller.error() so Claude Code's tryReactiveCompact fires.
  // Synthetic message-delta SSE renders as model output and never triggers
  // reactive compact.
  const upstream = upstreamWithFailedEvent("Your input exceeds the context window of this model.")
  const stream = translateStream(upstream, {
    messageId: "msg_test",
    model: "gpt-5.5",
    log: noopLogger,
  })
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let out = ""
  let errored = false
  let errorMessage = ""
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

  expect(out).toContain("event: error")
  expect(out).toContain("invalid_request_error")
  expect(out).toContain("prompt is too long")
  expect(errored).toBe(true)
  expect(errorMessage).toContain("prompt is too long")
  // Synthetic graceful-close shape MUST NOT appear on the overflow path.
  expect(out).not.toContain("\"stop_reason\":\"end_turn\"")
})

test("translateStream does NOT inflate input_tokens for non-overflow upstream errors", async () => {
  const sse = await collect(
    translateStream(upstreamWithFailedEvent("transient backend hiccup, please retry"), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  expect(sse).toContain("\"input_tokens\":0")
  expect(sse).not.toContain("950000")
})
