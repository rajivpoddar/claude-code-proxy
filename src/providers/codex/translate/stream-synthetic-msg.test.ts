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

test("translateStream reports zero input_tokens on upstream errors (no inflation)", async () => {
  // After the HTTP 400 fix (PR: return-http-400-on-context-overflow), token
  // inflation is no longer used to trigger reactive auto-compact. Reactive
  // compact fires from the upstream HTTP 400 + invalid_request_error path
  // instead. Synthetic SSE here always reports input_tokens:0.
  const sse = await collect(
    translateStream(upstreamWithFailedEvent("Your input exceeds the context window of this model."), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  expect(sse).toContain("\"input_tokens\":0")
  expect(sse).not.toContain("950000")
  expect(sse).not.toContain("990000")
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
