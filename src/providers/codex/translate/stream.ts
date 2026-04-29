import { encodeSseEvent } from "../../../sse.ts"
import type { Logger } from "../../../log.ts"
import { mapUsageToAnthropic, reduceUpstream, UpstreamStreamError } from "./reducer.ts"

// Mirrors CONTEXT_OVERFLOW_PATTERN in providers/codex/index.ts. Kept inline to
// avoid an import cycle (index.ts → stream.ts → index.ts). If the upstream
// constant changes, this must change too.
const CONTEXT_OVERFLOW_PATTERN =
  /context window|context length|exceeds|prompt is too long|input.*too long/i

/**
 * Translate a Codex Responses SSE stream into Anthropic SSE events.
 * Returns a ReadableStream<Uint8Array> ready to pipe to the client.
 *
 * The HTTP status has already been flushed (200) before the first
 * upstream event is consumed, so rate-limit and upstream-failed cases
 * surface as SSE error events rather than non-200 statuses.
 */
export function translateStream(
  upstream: ReadableStream<Uint8Array>,
  opts: {
    messageId: string
    model: string
    log: Logger
    onFinish?: (finish: { stopReason: "end_turn" | "tool_use" | "max_tokens"; usage?: Parameters<typeof mapUsageToAnthropic>[0] }) => void
  },
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = (event: string, data: unknown) => {
        controller.enqueue(encoder.encode(encodeSseEvent(event, data)))
      }
      const activeTools = new Map<number, { id: string; name: string }>()
      let messageStarted = false
      const ensureMessageStart = () => {
        if (messageStarted) return
        messageStarted = true
        emit("message_start", {
          type: "message_start",
          message: {
            id: opts.messageId,
            type: "message",
            role: "assistant",
            model: opts.model,
            content: [],
            stop_reason: null,
            stop_sequence: null,
            usage: {
              input_tokens: 0,
              output_tokens: 0,
              cache_creation_input_tokens: 0,
              cache_read_input_tokens: 0,
            },
          },
        })
        emit("ping", { type: "ping" })
      }

      try {
        for await (const e of reduceUpstream(upstream, opts.log)) {
          switch (e.kind) {
            case "text-start":
              ensureMessageStart()
              emit("content_block_start", {
                type: "content_block_start",
                index: e.index,
                content_block: { type: "text", text: "" },
              })
              break
            case "text-delta":
              emit("content_block_delta", {
                type: "content_block_delta",
                index: e.index,
                delta: { type: "text_delta", text: e.text },
              })
              break
            case "text-stop":
              emit("content_block_stop", { type: "content_block_stop", index: e.index })
              break
            case "tool-start":
              activeTools.set(e.index, { id: e.id, name: e.name })
              ensureMessageStart()
              emit("content_block_start", {
                type: "content_block_start",
                index: e.index,
                content_block: {
                  type: "tool_use",
                  id: e.id,
                  name: e.name,
                  input: {},
                },
              })
              break
            case "tool-delta":
              emit("content_block_delta", {
                type: "content_block_delta",
                index: e.index,
                delta: { type: "input_json_delta", partial_json: e.partialJson },
              })
              break
            case "tool-stop":
              activeTools.delete(e.index)
              emit("content_block_stop", { type: "content_block_stop", index: e.index })
              break
            case "finish":
              ensureMessageStart()
              opts.onFinish?.({ stopReason: e.stopReason, usage: e.usage })
              emit("message_delta", {
                type: "message_delta",
                delta: { stop_reason: e.stopReason, stop_sequence: null },
                usage: mapUsageToAnthropic(e.usage),
              })
              emit("message_stop", { type: "message_stop" })
              break
          }
        }
      } catch (err) {
        const activeToolNames = Array.from(activeTools.values(), (tool) => tool.name)
        const activeToolCalls = Array.from(activeTools.values())
        if (err instanceof UpstreamStreamError) {
          opts.log.warn("upstream stream error", {
            kind: err.kind,
            message: err.message,
            activeToolNames,
            activeToolCalls,
          })
          // Mid-stream context-overflow path. Preflight tee (providers/codex/
          // index.ts) only consumes ONE upstream event. If overflow surfaces
          // AFTER preflight (e.g., upstream emits some content then a
          // `response.failed` with context-window error), HTTP 200 SSE has
          // already been committed and we cannot return HTTP 400. Emitting a
          // synthetic `end_turn` message_delta is wrong — Claude Code renders
          // the error text as model output and reactive auto-compact never
          // fires. Instead, emit an Anthropic-shaped `event: error` with
          // type:"invalid_request_error" and the literal "prompt is too long"
          // prefix, then `controller.error()` to terminate the stream as a
          // protocol error. Claude Code's SSE parser surfaces `event: error`
          // payloads through the same isApiErrorMessage path that HTTP 400
          // uses, so tryReactiveCompact fires.
          const isContextOverflow =
            err.kind === "failed" && CONTEXT_OVERFLOW_PATTERN.test(err.message)
          if (isContextOverflow) {
            const errorPayload = {
              type: "error" as const,
              error: {
                type: "invalid_request_error" as const,
                message: `prompt is too long: ${err.message}`,
              },
            }
            try {
              emit("error", errorPayload)
            } catch {
              // controller may already be in an error state — ignore.
            }
            // Terminate the underlying ReadableStream with an Error so the
            // HTTP response surfaces as a protocol error (not a clean close).
            controller.error(new Error(`prompt is too long: ${err.message}`))
            return
          }
          // Non-overflow upstream failure path. Surface a SYNTHETIC complete
          // assistant message containing the upstream error text. Emitting
          // only an Anthropic `error` event leaves Claude Code's usage
          // accumulator in an inconsistent state (it expects message_delta
          // with usage stats), causing a `_.input_tokens` crash on the next
          // /compact or context-aware operation.
          if (!messageStarted) {
            const errorText =
              err.kind === "rate_limit"
                ? `[upstream rate-limited] ${err.message}`
                : `[upstream error] ${err.message}`
            ensureMessageStart()
            emit("content_block_start", {
              type: "content_block_start",
              index: 0,
              content_block: { type: "text", text: "" },
            })
            emit("content_block_delta", {
              type: "content_block_delta",
              index: 0,
              delta: { type: "text_delta", text: errorText },
            })
            emit("content_block_stop", { type: "content_block_stop", index: 0 })
            emit("message_delta", {
              type: "message_delta",
              delta: { stop_reason: "end_turn", stop_sequence: null },
              usage: {
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
              },
            })
            emit("message_stop", { type: "message_stop" })
          } else {
            // Stream already started — close out the partial message gracefully.
            emit("message_delta", {
              type: "message_delta",
              delta: { stop_reason: "end_turn", stop_sequence: null },
              usage: {
                input_tokens: 0,
                output_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
              },
            })
            emit("message_stop", { type: "message_stop" })
          }
        } else {
          opts.log.error("stream translation error", {
            err: String(err),
            activeToolNames,
            activeToolCalls,
          })
          emit("error", {
            type: "error",
            error: { type: "api_error", message: String(err) },
          })
        }
      } finally {
        controller.close()
      }
    },
  })
}
