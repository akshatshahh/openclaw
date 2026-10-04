import { AssistantMessageEventStream, type Message, type Model } from "openclaw/plugin-sdk/llm";
import { Type } from "typebox";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import { createResponsesAssistantOutput } from "../../packages/ai/src/providers/openai-responses-shared.js";
import { processResponsesStream } from "../../packages/ai/src/transports/openai-responses-stream-internal.js";
import { markdownToIR } from "../../packages/markdown-core/src/ir.js";
import { createDeferred } from "../../test/helpers/promise.js";
import { resolveHeartbeatReplyPayload } from "../auto-reply/heartbeat-reply-payload.js";
import { buildReplyPayloads } from "../auto-reply/reply/agent-runner-payloads.js";
import { createBlockReplyPipeline } from "../auto-reply/reply/block-reply-pipeline.js";
import { createBlockReplyDeliveryHandler } from "../auto-reply/reply/reply-delivery.js";
import { createTypingSignaler } from "../auto-reply/reply/typing-mode.js";
import { createTypingController } from "../auto-reply/reply/typing.js";
import { applyAssistantDeliveryDirectives } from "../config/sessions/transcript-assistant-delivery.js";
import { runAgentLoop } from "../plugin-sdk/agent-core.js";
import { attachSessionTranscriptRunId } from "../sessions/transcript-events.js";
import { matchesTranscriptEvent } from "../sessions/transcript-visible-record.js";
import { createTestAdmittedRunContext } from "./admitted-run-context.test-support.js";
import { buildAgentRunTerminalReplySnapshot } from "./agent-run-terminal-reply.js";
import type { EmbeddedRunAttemptWithReceiptEvidence } from "./embedded-agent-runner/run/attempt-result.js";
import { createEmbeddedRunContextRecoveryState } from "./embedded-agent-runner/run/context-recovery-state.js";
import { prepareEmbeddedRunTerminal } from "./embedded-agent-runner/run/terminal-preparation.js";
import { createUsageAccumulator } from "./embedded-agent-runner/usage-accumulator.js";
import {
  createSubscribedSessionHarness,
  emitAssistantTextDelta,
  emitAssistantTextEnd,
  extractTextPayloads,
} from "./embedded-agent-subscribe.e2e-harness.js";
import {
  createOpenAiResponsesPartial,
  createOpenAiResponsesTextBlock,
  createOpenAiResponsesTextEvent,
  type OpenAiResponsesTextEventPhase,
} from "./embedded-agent-subscribe.openai-responses.test-helpers.js";
import { readSubagentRunAnnounceResultUsing } from "./subagents/announce/subagent-announce-result.js";
import type { SubagentRunRecord } from "./subagents/registry/subagent-registry.types.js";
import { textAssistant } from "./test-helpers/sparse-transcript.test-support.js";

type Options = Omit<Parameters<typeof createSubscribedSessionHarness>[0], "runId">;
function setup(options: Options = {}) {
  const onBlockReply = vi.fn();
  const harness = createSubscribedSessionHarness({
    runId: "run",
    onBlockReply,
    blockReplyBreak: "text_end",
    ...options,
  });
  onTestFinished(() => harness.subscription.unsubscribe());
  return { ...harness, onBlockReply, texts: () => extractTextPayloads(onBlockReply.mock.calls) };
}
type Harness = ReturnType<typeof setup>;
function expectSingle(h: Harness, text: string) {
  expect(h.onBlockReply).toHaveBeenCalledTimes(1);
  expect(h.texts()).toEqual([text]);
  expect(h.subscription.assistantTexts).toEqual([text]);
}
function responsePair(
  h: Harness,
  text: string,
  id: string,
  phase?: OpenAiResponsesTextEventPhase,
  delta?: string,
) {
  const event = { text, id, delta, signaturePhase: phase, partialPhase: phase };
  for (const type of ["text_delta", "text_end"] as const) {
    h.emit(createOpenAiResponsesTextEvent({ type, ...event }));
  }
}
function block(text: string, id: string, phase?: OpenAiResponsesTextEventPhase) {
  return createOpenAiResponsesTextBlock({ text, id, phase });
}

describe("Responses final delivery", () => {
  it("does not replay compact items when message_end becomes cumulative", async () => {
    const h = setup();
    const items = [
      { id: "item-a", text: "Alpha" },
      { id: "item-b", text: "Beta" },
    ];
    const base = createOpenAiResponsesPartial({
      text: "",
      id: "item-a",
      signaturePhase: "final_answer",
    });
    h.emit({ type: "message_start", message: base });
    for (const [contentIndex, item] of items.entries()) {
      const partial = createOpenAiResponsesPartial({ ...item, signaturePhase: "final_answer" });
      for (const type of ["text_delta", "text_end"] as const) {
        h.emit({
          type: "message_update",
          message: partial,
          assistantMessageEvent: {
            type,
            contentIndex,
            ...(type === "text_delta" ? { delta: item.text } : { content: item.text }),
            partial,
          },
        });
      }
      await h.subscription.waitForPendingEvents();
      expect(h.texts()).toEqual(items.slice(0, contentIndex + 1).map(({ text }) => text));
    }
    const message = {
      ...base,
      content: items.map(({ text, id }) => block(text, id, "final_answer")),
    };
    for (let repeat = 0; repeat < 2; repeat++) {
      h.emit({ type: "message_end", message });
      await h.subscription.waitForPendingEvents();
      expect(h.texts()).toEqual(["Alpha", "Beta"]);
      expect(h.onBlockReply).toHaveBeenCalledTimes(2);
    }
  });

  it.each([
    {
      name: "unfinished reasoning before a queued phase update",
      prefix: "<think>private",
      queuedDeltas: [" reasoning", " remains private"],
      recoveryPrefix: "</think>",
      audioAsVoice: false,
    },
    {
      name: "split unclosed inline tag examples",
      prefix: "Example: <thi",
      recoveryPrefix: "nk>literal.",
      expectedPrefix: "Example: <think>literal.",
      audioAsVoice: false,
    },
    {
      name: "split voice directives",
      prefix: "[[audio_as_",
      recoveryPrefix: "voice]]",
      audioAsVoice: true,
    },
  ])(
    "preserves $name across late Responses phase updates",
    async ({ prefix, audioAsVoice, expectedPrefix = "", recoveryPrefix, queuedDeltas = [] }) => {
      const model: Model<"openai-responses"> = {
        id: "gpt-5.5",
        name: "GPT-5.5",
        api: "openai-responses",
        provider: "openai",
        baseUrl: "https://api.openai.com/v1",
        reasoning: true,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 200000,
        maxTokens: 8192,
      };
      const answer = "Answer".repeat(32);
      const expected = expectedPrefix + answer;
      const queuedText = queuedDeltas.join("");
      let observedQueuedText = "";
      const rawProcessed = createDeferred();
      const reanchorProcessed = createDeferred();
      const phaseProcessed = createDeferred();
      const onPartialReply = vi.fn();
      const { emit, subscription, onBlockReply } = setup({
        onPartialReply,
        blockReplyChunking: { minChars: 64, maxChars: 128, breakPreference: "paragraph" },
      });
      async function* wireEvents() {
        yield {
          type: "response.output_item.added",
          output_index: 0,
          item: { type: "message", id: "msg_answer", role: "assistant", content: [] },
        };
        yield { type: "response.output_text.delta", output_index: 0, delta: prefix };
        await rawProcessed.promise;
        yield {
          type: "response.output_item.added",
          output_index: 1,
          item: { type: "reasoning", id: "rs_reanchor", summary: [] },
        };
        await reanchorProcessed.promise;
        const text = prefix + queuedDeltas.join("") + recoveryPrefix + answer;
        const finalItem = {
          type: "message",
          id: "msg_answer",
          role: "assistant",
          status: "completed",
          phase: "final_answer",
          content: [{ type: "output_text", text, annotations: [] }],
        };
        if (queuedDeltas.length > 0) {
          for (const delta of queuedDeltas) {
            yield { type: "response.output_text.delta", output_index: 0, delta };
          }
          yield { type: "response.output_item.done", output_index: 0, item: finalItem };
          phaseProcessed.resolve();
        }
        yield {
          type: "response.completed",
          response: {
            id: "resp_phase_recovery",
            status: "completed",
            output: [finalItem, { type: "reasoning", id: "rs_reanchor", summary: [] }],
          },
        };
      }
      const output = createResponsesAssistantOutput(model);
      const response = new AssistantMessageEventStream();
      response.push({ type: "start", partial: output });
      const producing = processResponsesStream(wireEvents(), output, response, model).then(
        () => {
          response.push({ type: "done", reason: "stop", message: output });
          response.end();
        },
        (error: unknown) => {
          response.end({ ...output, stopReason: "error", errorMessage: String(error) });
          throw error;
        },
      );
      const running = runAgentLoop(
        [{ role: "user", content: "Give the answer.", timestamp: 1 }],
        { systemPrompt: "", messages: [] },
        {
          model,
          convertToLlm: (messages) =>
            messages.filter(
              (message): message is Message =>
                message.role === "user" ||
                message.role === "assistant" ||
                message.role === "toolResult",
            ),
        },
        async (event) => {
          emit(event);
          await subscription.waitForPendingEvents();
          if (event.type !== "message_update") {
            return;
          }
          const update = event.assistantMessageEvent;
          if (update.type === "text_delta" && update.delta === prefix) {
            expect(onBlockReply).not.toHaveBeenCalled();
            rawProcessed.resolve();
          }
          if (update.type === "thinking_start") {
            reanchorProcessed.resolve();
            if (queuedDeltas.length > 0) {
              await phaseProcessed.promise;
            }
          }
          if (update.type === "text_delta" && queuedText && queuedText.includes(update.delta)) {
            observedQueuedText += update.delta;
            expect(onPartialReply).not.toHaveBeenCalled();
          }
        },
        undefined,
        () => response,
      );
      try {
        await Promise.all([producing, running]);
        await subscription.waitForPendingEvents();
        expect(observedQueuedText).toBe(queuedText);
        expect(extractTextPayloads(onBlockReply.mock.calls).join("")).toBe(expected);
        expect(subscription.assistantTexts.join("")).toBe(expected);
        expect(onBlockReply.mock.calls[0]?.[0].audioAsVoice ?? false).toBe(audioAsVoice);
      } finally {
        rawProcessed.resolve();
        reanchorProcessed.resolve();
        phaseProcessed.resolve();
        await Promise.allSettled([producing, running]);
        subscription.unsubscribe();
      }
    },
  );

  it.each([
    { name: "late completions phase", api: "openai-completions", suppressLiveStreamOutput: false },
    {
      name: "suppressed Responses stream",
      api: "openai-responses",
      suppressLiveStreamOutput: true,
    },
  ] as const)(
    "delivers all undelivered final blocks after $name",
    async ({ api, suppressLiveStreamOutput }) => {
      const onAgentEvent = vi.fn();
      const h = setup({ onAgentEvent, suppressLiveStreamOutput });
      const base = { ...createOpenAiResponsesPartial({ text: "", id: "answer-0" }), api };
      const texts = ["First", "Second"];
      h.emit({ type: "message_start", message: base });
      for (const [contentIndex, delta] of texts.entries()) {
        const partial = {
          ...base,
          content: texts
            .slice(0, contentIndex + 1)
            .map((text, index) =>
              block(text, `answer-${index}`, suppressLiveStreamOutput ? "final_answer" : undefined),
            ),
        };
        h.emit({
          type: "message_update",
          message: partial,
          assistantMessageEvent: { type: "text_delta", contentIndex, delta, partial },
        });
        await h.subscription.waitForPendingEvents();
        expect(h.onBlockReply).not.toHaveBeenCalled();
        expect(h.subscription.assistantTexts).toEqual([]);
      }
      if (suppressLiveStreamOutput) {
        expect(onAgentEvent).not.toHaveBeenCalled();
      }
      h.emit({
        type: "message_end",
        message: {
          ...base,
          content: texts.map((text, index) => block(text, `answer-${index}`, "final_answer")),
        },
      });
      await h.subscription.waitForPendingEvents();
      expectSingle(h, "First\nSecond");
      expect(onAgentEvent.mock.calls.at(-1)?.[0]).toMatchObject({
        stream: "assistant",
        data: { text: "First\nSecond" },
      });
    },
  );

  describe("Astra async response tails", () => {
    const model: Model<"openai-responses"> = {
      id: "gpt-6-astra",
      name: "GPT-6 Astra",
      api: "openai-responses",
      provider: "openai",
      baseUrl: "https://api.openai.com/v1",
      reasoning: true,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 200000,
      maxTokens: 8192,
    };
    const lookupCall = {
      type: "function_call" as const,
      id: "fc_lookup",
      call_id: "call_lookup",
      name: "lookup",
      arguments: "{}",
      status: "completed",
      async: true,
    };
    const finalAnswer = (id: string, text: string) => ({
      type: "message" as const,
      id,
      role: "assistant",
      status: "completed",
      phase: "final_answer",
      content: [{ type: "output_text", text, annotations: [] }],
    });
    const reasoning = {
      type: "reasoning" as const,
      id: "rs_answer",
      summary: [{ type: "summary_text", text: "Counter B is next to the entrance." }],
    };
    type WireItem = typeof lookupCall | typeof reasoning | ReturnType<typeof finalAnswer>;
    type Request =
      | string
      | readonly WireItem[]
      | { readonly items: readonly WireItem[]; readonly endTurn: false }
      | { readonly items: readonly WireItem[]; readonly incomplete: true };
    // Each model request is a real Responses wire stream through the shipped transport.
    function responsesStream(id: string, request: Request) {
      const items: readonly WireItem[] =
        typeof request === "string"
          ? [finalAnswer(`msg_${id}`, request)]
          : "items" in request
            ? request.items
            : request;
      const endTurn = typeof request === "object" && "endTurn" in request ? request.endTurn : true;
      const incomplete = typeof request === "object" && "incomplete" in request;
      async function* wire() {
        for (const [outputIndex, item] of items.entries()) {
          if (item.type === "message") {
            yield {
              type: "response.output_item.added",
              output_index: outputIndex,
              item: { ...item, status: "in_progress", content: [] },
            };
            yield {
              type: "response.output_text.delta",
              output_index: outputIndex,
              delta: item.content[0]?.text ?? "",
            };
          } else {
            yield {
              type: "response.output_item.added",
              output_index: outputIndex,
              item: { ...item, status: "in_progress", arguments: "" },
            };
          }
          yield { type: "response.output_item.done", output_index: outputIndex, item };
        }
        yield incomplete
          ? {
              type: "response.incomplete",
              response: {
                id,
                status: "incomplete",
                incomplete_details: { reason: "max_output_tokens" },
                output: items,
              },
            }
          : {
              type: "response.completed",
              response: { id, status: "completed", output: items, end_turn: endTurn },
            };
      }
      const output = createResponsesAssistantOutput(model);
      const response = new AssistantMessageEventStream();
      response.push({ type: "start", partial: output });
      void processResponsesStream(wire(), output, response, model, {
        asyncToolExecution: true,
      }).then(
        () => {
          response.push({
            type: "done",
            reason: output.stopReason === "toolUse" ? "toolUse" : "stop",
            message: output,
          });
          response.end();
        },
        (error: unknown) => {
          response.end({ ...output, stopReason: "error", errorMessage: String(error) });
        },
      );
      return response;
    }

    const answered = [lookupCall, finalAnswer("msg_answer", "Use counter B.")];
    const answeredTail = ["toolUse:toolCall", "stop:text", "stop:text"];
    const keptReply = { disposition: "visible", text: "Use counter B." } as const;
    it.each([
      {
        name: "a later NO_REPLY keeps the completed answer",
        delivery: "deferred",
        requests: [answered, "NO_REPLY"],
        transcript: answeredTail,
        delivered: ["Use counter B."],
        reply: keptReply,
      },
      {
        name: "a heartbeat turn without block streaming keeps the completed answer",
        delivery: "off",
        requests: [answered, "NO_REPLY"],
        transcript: answeredTail,
        delivered: ["Use counter B."],
        reply: keptReply,
        heartbeat: true,
      },
      {
        name: "live blocks of a two-item answer are not resent after NO_REPLY",
        delivery: "live",
        requests: [
          [
            lookupCall,
            finalAnswer("msg_first", "First part."),
            finalAnswer("msg_second", "Second part."),
          ],
          "NO_REPLY",
        ],
        transcript: ["toolUse:toolCall", "stop:text+text", "stop:text"],
        delivered: ["First part.", "Second part."],
        reply: { disposition: "visible", text: "First part.\nSecond part." },
      },
      {
        name: "live blocks of a two-item answer split by commentary are not resent after NO_REPLY",
        delivery: "live",
        requests: [
          [
            lookupCall,
            finalAnswer("msg_first", "First part."),
            { ...finalAnswer("msg_note", "Checking the second part."), phase: "commentary" },
            finalAnswer("msg_second", "Second part."),
          ],
          "NO_REPLY",
        ],
        transcript: ["toolUse:toolCall", "stop:text+text+text", "stop:text"],
        delivered: ["First part.", "Second part."],
        reply: { disposition: "visible", text: "First part.\nSecond part." },
      },
      {
        name: "a live answer followed by commentary is not resent after NO_REPLY",
        delivery: "live",
        requests: [
          [
            lookupCall,
            finalAnswer("msg_answer", "Alpha."),
            { ...finalAnswer("msg_note", "Wrapping up."), phase: "commentary" },
          ],
          "NO_REPLY",
        ],
        transcript: ["toolUse:toolCall", "stop:text+text", "stop:text"],
        delivered: ["Alpha."],
        reply: { disposition: "visible", text: "Alpha." },
      },
      {
        name: "a live answer followed by commentary is not resent",
        delivery: "live",
        requests: [
          [
            finalAnswer("msg_answer", "Alpha."),
            { ...finalAnswer("msg_note", "Wrapping up."), phase: "commentary" },
          ],
        ],
        transcript: ["stop:text+text"],
        delivered: ["Alpha."],
        reply: { disposition: "visible", text: "Alpha." },
      },
      {
        name: "live blocks of a two-item terminal answer are not resent",
        delivery: "live",
        requests: [
          [finalAnswer("msg_first", "First part."), finalAnswer("msg_second", "Second part.")],
        ],
        transcript: ["stop:text+text"],
        delivered: ["First part.", "Second part."],
        reply: { disposition: "visible", text: "First part.\nSecond part." },
      },
      {
        name: "a repeated answer is delivered once",
        delivery: "deferred",
        requests: [answered, "Use counter B."],
        transcript: answeredTail,
        delivered: ["Use counter B."],
        reply: keptReply,
      },
      {
        name: "a later answer supersedes the completed answer",
        delivery: "deferred",
        requests: [answered, "Correction: use counter C."],
        transcript: answeredTail,
        delivered: ["Correction: use counter C."],
        reply: { disposition: "visible", text: "Correction: use counter C." },
      },
      {
        name: "a later silent attachment supersedes the completed answer",
        delivery: "deferred",
        requests: [answered, "NO_REPLY\nMEDIA:/tmp/openclaw/tts-a/voice-a.opus"],
        transcript: answeredTail,
        delivered: ["/tmp/openclaw/tts-a/voice-a.opus"],
      },
      {
        name: "a NO_REPLY after a superseding silent attachment does not restore the earlier answer",
        delivery: "deferred",
        requests: [
          answered,
          [
            { ...lookupCall, id: "fc_lookup_2", call_id: "call_lookup_2" },
            finalAnswer("msg_media", "NO_REPLY\nMEDIA:/tmp/openclaw/tts-a/voice-a.opus"),
          ],
          "NO_REPLY",
        ],
        transcript: ["toolUse:toolCall", "stop:text", "toolUse:toolCall", "stop:text", "stop:text"],
        delivered: ["/tmp/openclaw/tts-a/voice-a.opus"],
        reply: { disposition: "silent" },
      },
      {
        name: "a NO_REPLY after a superseding cut-off attachment does not restore the earlier answer",
        delivery: "deferred",
        requests: [
          answered,
          {
            items: [
              { ...lookupCall, id: "fc_lookup_2", call_id: "call_lookup_2" },
              finalAnswer("msg_media", "NO_REPLY\nMEDIA:/tmp/openclaw/tts-a/voice-a.opus"),
            ],
            incomplete: true,
          },
          "NO_REPLY",
        ],
        transcript: [
          "toolUse:toolCall",
          "stop:text",
          "toolUse:toolCall",
          "length:text",
          "stop:text",
        ],
        delivered: ["/tmp/openclaw/tts-a/voice-a.opus"],
        reply: { disposition: "silent" },
      },
      {
        name: "a later voice NO_REPLY that persistence normalized first supersedes the completed answer",
        delivery: "deferred",
        requests: [answered, "NO_REPLY [[audio_as_voice]]"],
        transcript: answeredTail,
        delivered: [],
        reply: { disposition: "silent" },
        persistedFirst: true,
      },
      {
        name: "a later NO_REPLY keeps pre-tool progress silent",
        delivery: "deferred",
        requests: [[finalAnswer("msg_progress", "Checking counter B."), lookupCall], "NO_REPLY"],
        transcript: ["toolUse:text+toolCall", "stop:", "stop:text"],
        delivered: [],
        reply: { disposition: "silent" },
      },
      {
        name: "a later NO_REPLY keeps an interim stop that continued the turn silent",
        delivery: "deferred",
        requests: [
          { items: [finalAnswer("msg_interim", "Starting the export now.")], endTurn: false },
          "NO_REPLY",
        ],
        transcript: ["stop:text", "stop:text"],
        delivered: [],
        reply: { disposition: "silent" },
      },
      {
        name: "interim progress before a later NO_REPLY does not replace the completed answer",
        delivery: "deferred",
        requests: [
          answered,
          { items: [finalAnswer("msg_progress", "Exporting the rest now.")], endTurn: false },
          "NO_REPLY",
        ],
        transcript: [...answeredTail, "stop:text"],
        delivered: ["Use counter B."],
        reply: keptReply,
        announce: true,
      },
      {
        name: "quiet mode keeps the completed answer private",
        delivery: "deferred",
        requests: [answered, "NO_REPLY"],
        transcript: answeredTail,
        delivered: ["Sent with the message tool."],
        reply: keptReply,
        quiet: true,
      },
      {
        name: "reasoning sent with the completed answer is not repeated after NO_REPLY",
        delivery: "live",
        requests: [
          [lookupCall, reasoning, finalAnswer("msg_answer", "Use counter B.")],
          "NO_REPLY",
        ],
        transcript: answeredTail,
        delivered: ["Counter B is next to the entrance.", "Use counter B."],
        reply: keptReply,
        reasoning: true,
      },
    ] as const)("$name", async ({ delivery, requests, transcript, delivered, ...row }) => {
      const heartbeat = "heartbeat" in row;
      const quiet = "quiet" in row;
      const sent: string[] = [];
      const show = (payload: { text?: string; mediaUrl?: string }) =>
        payload.text ?? payload.mediaUrl ?? "";
      const blockStreamingEnabled = delivery !== "off";
      const pipeline = createBlockReplyPipeline({
        onBlockReply: (payload) => {
          sent.push(show(payload));
        },
        timeoutMs: 5000,
      });
      const handler = createBlockReplyDeliveryHandler({
        onBlockReply: (payload) => {
          sent.push(show(payload));
        },
        normalizeStreamingText: (payload) => ({ text: payload.text, skip: false }),
        applyReplyToMode: (payload) => payload,
        typingSignals: createTypingSignaler({
          typing: createTypingController({}),
          mode: "never",
          isHeartbeat: heartbeat,
        }),
        blockStreamingEnabled,
        reasoningPayloadsEnabled: "reasoning" in row,
        blockReplyPipeline: pipeline,
        directBlockDeliveries: [],
      });
      // Required user replies defer terminal delivery; optional turns stream blocks live.
      const h = setup({
        onBlockReply: handler,
        blockReplyBreak: "message_end",
        ...(delivery === "deferred" ? { onBeforeTerminalDelivery: async () => undefined } : {}),
        ...("reasoning" in row ? { reasoningMode: "on" as const } : {}),
      });
      const lookup = vi.fn(async () => ({
        content: [{ type: "text" as const, text: "Counter B is open." }],
        details: {},
      }));
      const pending = requests.map(
        (request, index) => () => responsesStream(`resp_${index}`, request),
      );
      const messages = await runAgentLoop(
        [{ role: "user", content: "Where do I store my bag?", timestamp: 1 }],
        {
          systemPrompt: "",
          messages: [],
          tools: [
            {
              name: "lookup",
              label: "lookup",
              description: "lookup",
              parameters: Type.Object({}),
              execute: lookup,
            },
          ],
        },
        {
          model,
          convertToLlm: (history) =>
            history.filter(
              (message): message is Message =>
                message.role === "user" ||
                message.role === "assistant" ||
                message.role === "toolResult",
            ),
        },
        async (event) => {
          if ("persistedFirst" in row && event.type === "message_end") {
            // A backpressured subscriber sees the message after persistence normalized it.
            applyAssistantDeliveryDirectives(event.message);
          }
          h.emit(event);
          await h.subscription.waitForPendingEvents();
        },
        undefined,
        () => {
          const next = pending.shift();
          if (!next) {
            throw new Error("unexpected model request");
          }
          return next();
        },
      );
      await h.subscription.waitForPendingEvents();
      await pipeline.flush({ force: true });
      expect(pending).toEqual([]);
      // The call-free tail ended the provider response; only the call fragment uses tools.
      expect(
        messages.flatMap((message) =>
          message.role === "assistant"
            ? [
                `${message.stopReason}:${message.content
                  .map((item) => item.type)
                  .filter((type) => type !== "thinking")
                  .join("+")}`,
              ]
            : [],
        ),
      ).toEqual(transcript);
      const completed = h.subscription.getCurrentAttemptAssistant();
      const attempt: EmbeddedRunAttemptWithReceiptEvidence = {
        terminal: { kind: "ok" },
        sessionIdUsed: "session",
        messagesSnapshot: messages,
        assistantTexts: h.subscription.assistantTexts,
        answerSegments: h.subscription.answerSegments,
        keptAnswer: h.subscription.getKeptAnswer(),
        lastAssistantTextMessageIndex: h.subscription.getLastAssistantTextMessageIndex(),
        toolMetas: [],
        lastAssistant: completed,
        currentAttemptAssistant: completed,
        currentAttemptCompletedAssistant: completed,
        didSendViaMessagingTool: quiet,
        messagingToolSentTexts: [],
        messagingToolSentMediaUrls: [],
        messagingToolSentTargets: [],
        ...(quiet
          ? { messagingToolSourceReplyPayloads: [{ text: "Sent with the message tool." }] }
          : {}),
        cloudCodeAssistFormatError: false,
        replayMetadata: { hadPotentialSideEffects: false, replaySafe: true },
        itemLifecycle: { startedCount: 0, completedCount: 0, activeCount: 0 },
      };
      const prepared = prepareEmbeddedRunTerminal({
        runParams: {
          admittedRunContext: createTestAdmittedRunContext("run"),
          sessionId: "session",
          sessionKey: "agent:main:telegram:direct:astra",
          runId: "run",
          workspaceDir: "/tmp/openclaw-test",
          prompt: "Where do I store my bag?",
          timeoutMs: 60_000,
          trigger: heartbeat ? "heartbeat" : "user",
          ...(quiet ? { sourceReplyDeliveryMode: "message_tool_only" as const } : {}),
          ...("reasoning" in row ? { reasoningLevel: "on" as const } : {}),
        },
        attempt,
        currentAttemptCompletedAssistant: completed,
        provider: "openai",
        model: model.id,
        activeErrorContext: { provider: "openai", model: model.id },
        authProfileStore: { version: 1, profiles: {} },
        sessionIdUsed: "session",
        outerContextTokenMeta: {},
        usageAccumulator: createUsageAccumulator(),
        contextRecoveryState: createEmbeddedRunContextRecoveryState(),
        resolvedToolResultFormat: "markdown",
        terminalState: {
          outcome: { reason: "completed", status: "ok", stopReason: "stop" },
          signalOwnedInterruption: false,
        },
      });
      if (heartbeat) {
        expect(resolveHeartbeatReplyPayload(prepared.payloads)?.text).toBe("Use counter B.");
      }
      const { replyPayloads } = await buildReplyPayloads({
        payloads: prepared.payloads,
        isHeartbeat: heartbeat,
        didLogHeartbeatStrip: false,
        blockStreamingEnabled,
        blockReplyPipeline: pipeline,
        replyToMode: "off",
      });
      expect([...sent, ...replyPayloads.map(show)]).toEqual(delivered);
      if ("reply" in row) {
        // Sub-agent completion and A2A forwarding read the terminal reply, not the payloads.
        expect(
          buildAgentRunTerminalReplySnapshot({
            visibleText: prepared.finalAssistantVisibleText,
            rawText: prepared.finalAssistantRawText,
          }),
        ).toEqual(row.reply);
      }
      if ("announce" in row) {
        // A parent's sub-agent announcement hydrates the answer from the persisted rows.
        const events = messages.map((message, index) => ({
          type: "message",
          id: `entry-${index}`,
          message: attachSessionTranscriptRunId(message, "run"),
        }));
        const child: SubagentRunRecord = {
          runId: "run",
          childSessionKey: "agent:main:subagent:astra",
          requesterSessionKey: "agent:main:main",
          requesterDisplayKey: "main",
          task: "Where do I store my bag?",
          cleanup: "keep",
          createdAt: 1,
          execution: {
            status: "terminal",
            outcome: { status: "ok" },
            transcriptTarget: {
              agentId: "main",
              sessionId: "session",
              sessionKey: "agent:main:subagent:astra",
              storePath: "/tmp/openclaw-test/sessions.json",
            },
          },
          completion: {
            required: true,
            terminalReply: buildAgentRunTerminalReplySnapshot({
              visibleText: prepared.finalAssistantVisibleText,
              rawText: prepared.finalAssistantRawText,
            }),
          },
        };
        const unexpected = () => {
          throw new Error("unexpected session fallback");
        };
        const announced = await readSubagentRunAnnounceResultUsing(child, {
          readSubagentRun: () => child,
          findTranscriptEvent: async (_target, match) => {
            const event = events.findLast((candidate) => matchesTranscriptEvent(candidate, match));
            return event === undefined ? undefined : { event };
          },
          findSessionTranscriptArchiveEventReadOnly: async () => undefined,
          getRuntimeConfig: unexpected,
          readSubagentSessionEntry: unexpected,
          resolveAgentIdFromSessionKey: unexpected,
          resolveSessionStorePathCore: unexpected,
        });
        expect(announced.text).toBe("Use counter B.");
      }
    });
  });
});

describe("terminal visible replies", () => {
  it.each([
    {
      name: "decoded reasoning and reply controls",
      body: "<think>hidden [[reply_to:example-id]]</think>Visible reply.",
      expected: "Visible reply.",
    },
    {
      name: "an entirely hidden decoded body",
      body: "<think>hidden [[reply_to:example-id]]</think>",
      expected: "",
    },
    {
      name: "decoded final prose after commentary",
      body: "Before <think>literal tag text after",
      mixedPhases: true,
      expected: "Before <think>literal tag text after",
    },
    {
      name: "an outer final envelope",
      body: "Visible reply.",
      finalEnvelope: true,
      expected: "Visible reply.",
    },
  ])("prepares $name from standalone message-tool JSON", async (scenario) => {
    const h = setup({ blockReplyBreak: "message_end", enforceFinalTag: scenario.finalEnvelope });
    const encoded = JSON.stringify({
      name: "message",
      arguments: { action: "send", target: "test-target", message: scenario.body },
    }).replaceAll("<", "\\u003c");
    const message = {
      ...textAssistant(scenario.finalEnvelope ? `<final>${encoded}</final>` : encoded),
      api: "openai-completions",
      ...(scenario.mixedPhases
        ? {
            content: [
              block("Working...", "commentary", "commentary"),
              block(encoded, "answer", "final_answer"),
            ],
          }
        : {}),
    };
    h.emit({ type: "message_start", message });
    h.emit({ type: "message_end", message });
    await h.subscription.waitForPendingEvents();
    expect(h.texts()).toEqual(scenario.expected ? [scenario.expected] : []);
    for (const [payload] of h.onBlockReply.mock.calls) {
      expect(payload.replyToId).toBeUndefined();
      expect(payload.replyToCurrent).toBeFalsy();
      expect(payload.replyToTag).toBeFalsy();
    }
  });

  it.each(["google", "responses"] as const)(
    "preserves indented code in %s replies",
    async (provider) => {
      const text = "    const value = 1;\n    use(value);";
      const code = "const value = 1;\nuse(value);\n";
      const onAgentEvent = vi.fn();
      const h = setup({
        onAgentEvent,
        blockReplyBreak: "message_end",
        blockReplyChunking: { minChars: 64, maxChars: 128, breakPreference: "paragraph" },
      });
      const message =
        provider === "responses"
          ? createOpenAiResponsesPartial({
              text,
              id: "item-final-code",
              signaturePhase: "final_answer",
            })
          : {
              ...textAssistant(text),
              api: "google-generative-ai",
              provider: "google",
              model: "gemini-2.5-flash",
              stopReason: "stop" as const,
            };
      h.emit({ type: "message_start", message: { ...message, content: [] } });
      if (provider === "responses") {
        let accumulatedText = "";
        for (const delta of ["    ", "const value = 1;\n", "    use(value);"]) {
          accumulatedText += delta;
          const partial = {
            ...message,
            content: message.content.map((part) => ({ ...part, text: accumulatedText })),
          };
          h.emit({
            type: "message_update",
            message: partial,
            assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta, partial },
          });
          await h.subscription.waitForPendingEvents();
        }
      }
      h.emit({ type: "message_end", message });
      await h.subscription.waitForPendingEvents();
      for (const texts of [h.texts(), h.subscription.assistantTexts]) {
        expect
          .soft(texts.map((payload) => markdownToIR(payload)))
          .toMatchObject([
            { text: code, styles: [{ start: 0, end: code.length, style: "code_block" }] },
          ]);
      }
      expect
        .soft(
          onAgentEvent.mock.calls
            .filter(([event]) => event.stream === "assistant")
            .map(([event]) => event.data.text),
        )
        .toEqual(provider === "responses" ? ["    const value = 1;", text] : [text]);
    },
  );

  it("retains silent terminal evidence with text_end block replies", async () => {
    const h = setup({
      blockReplyChunking: { minChars: 64, maxChars: 128, breakPreference: "paragraph" },
    });
    h.emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantTextDelta({ emit: h.emit, delta: "NO_REPLY" });
    emitAssistantTextEnd({ emit: h.emit, content: "NO_REPLY" });
    h.emit({ type: "message_end", message: textAssistant("NO_REPLY") });
    await h.subscription.waitForPendingEvents();
    expect(h.subscription.assistantTexts).toEqual(["NO_REPLY"]);
    expect(h.onBlockReply).not.toHaveBeenCalled();
  });

  it("recovers visible text when text_end delivered only silent NO_REPLY chunks", async () => {
    const h = setup();
    h.emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantTextEnd({ emit: h.emit, content: "NO_REPLY" });
    await Promise.resolve();
    expect(h.onBlockReply).not.toHaveBeenCalled();
    h.emit({ type: "message_end", message: textAssistant("Final visible reply.") });
    await h.subscription.waitForPendingEvents();
    expectSingle(h, "Final visible reply.");
  });

  it("does not replay a source range assembled from multiple streamed chunks", async () => {
    const h = setup({ blockReplyChunking: { minChars: 1, maxChars: 4 } });
    const text = "aaaaaaaaaaaa";
    h.emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantTextDelta({ emit: h.emit, delta: text });
    h.emit({ type: "message_end", message: textAssistant(text) });
    expect(h.texts()).toEqual(["aaaa", "aaaa", "aaaa"]);
    emitAssistantTextEnd({ emit: h.emit, content: text });
    await Promise.resolve();
    expect(h.texts()).toEqual(["aaaa", "aaaa", "aaaa"]);
  });

  it("delivers the full final text when it extends suppressed commentary", async () => {
    const h = setup();
    h.emit({ type: "message_start", message: { role: "assistant" } });
    responsePair(h, "Hello", "item_commentary", "commentary");
    await Promise.resolve();
    expect(h.onBlockReply).not.toHaveBeenCalled();
    responsePair(h, "Hello world", "item_final", "final_answer", " world");
    await Promise.resolve();
    expectSingle(h, "Hello world");
  });

  it("delivers the final answer at message_end after streamed commentary", async () => {
    const h = setup();
    h.emit({ type: "message_start", message: { role: "assistant" } });
    responsePair(h, "Working...", "item_commentary", "commentary");
    await Promise.resolve();
    h.emit({
      type: "message_end",
      message: {
        role: "assistant",
        content: [
          block("Working...", "item_commentary", "commentary"),
          block("Done.", "item_final", "final_answer"),
        ],
      },
    });
    expectSingle(h, "Done.");
  });
});
