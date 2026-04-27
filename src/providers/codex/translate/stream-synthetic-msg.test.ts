import { expect, test } from "bun:test"
import type { Logger } from "../../../log.ts"
import { translateStream } from "./stream.ts"
import { UpstreamStreamError } from "./reducer.ts"

const noopLogger: Logger = {
  debug() {}, info() {}, warn() {}, error() {},
  child() { return noopLogger },
}

function upstreamWithFailedEvent(message: string): ReadableStream<Uint8Array> {
  // Emits a Codex `response.failed` event which the reducer throws as
  // UpstreamStreamError (kind: "failed"). This triggers our synthetic-message
  // recovery path — the very crash mode that was breaking GPT-5.5 slots when
  // input exceeded the model's context window.
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
    translateStream(upstreamWithFailedEvent("Your input exceeds the context window of this model."), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  // Synthetic complete message keeps Claude Code's usage accumulator healthy
  expect(sse).toContain("event: message_start")
  expect(sse).toContain("event: content_block_delta")
  expect(sse).toContain("[upstream error]")
  expect(sse).toContain("Your input exceeds the context window")
  expect(sse).toContain("event: message_delta")
  expect(sse).toContain("\"stop_reason\":\"end_turn\"")
  expect(sse).toContain("event: message_stop")
})

test("translateStream inflates input_tokens for context-overflow to trigger autocompact", async () => {
  const sse = await collect(
    translateStream(upstreamWithFailedEvent("Your input exceeds the context window of this model. Please adjust your input and try again."), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  // Inflated input_tokens = 950000 (95% of 1M declared) → Claude Code auto-compacts on next turn
  expect(sse).toContain("\"input_tokens\":950000")
})

test("translateStream does NOT inflate input_tokens for non-overflow upstream errors", async () => {
  const sse = await collect(
    translateStream(upstreamWithFailedEvent("transient backend hiccup, please retry"), {
      messageId: "msg_test",
      model: "gpt-5.5",
      log: noopLogger,
    }),
  )

  // Non-overflow errors keep input_tokens at 0 (no false autocompact trigger)
  expect(sse).toContain("\"input_tokens\":0")
  expect(sse).not.toContain("\"input_tokens\":950000")
})
