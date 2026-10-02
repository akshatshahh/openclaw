import path from "node:path";
import { expect, test } from "vitest";
import { requireGit } from "../agents/worktrees/git.js";
import { loadSessionEntry, upsertSessionEntryCore } from "../config/sessions/session-accessor.js";
import type { SessionEntry } from "../config/sessions/types.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { registerProjectRegistry } from "../projects/project-registry.js";
import { ensureProfileForEmail } from "../state/user-profiles.js";
import {
  connectReq,
  CONTROL_UI_CLIENT,
  onceMessage,
  openWs,
  rpcReq,
  testState,
  withGatewayServer,
} from "./server.auth.test-helpers.js";
import { initializeRepository } from "./server.sessions.create.projects.test-support.js";
import { setupSessionCreateHandlerTestHarness } from "./server.sessions.create.test-support.js";
import { gatewayReplyMock } from "./test-helpers.js";
import { settleGatewaySessionStoreFixture } from "./test/server-sessions-resources.test-helpers.js";
import { getGatewayConfigModule } from "./test/server-sessions.test-helpers.js";

const { createSessionStoreDir, withSessionTestState } = setupSessionCreateHandlerTestHarness();

test("a contributor creates, reads, and runs a required workspace on a non-main agent over the Gateway", async () => {
  await withSessionTestState({ layout: "state-only" }, async (state) => {
    const workspace = await initializeRepository(state.root, "project");
    const main = await requireGit(workspace, ["rev-parse", "main"]);
    await requireGit(workspace, ["checkout", "-b", "unrelated-source"]);
    await requireGit(workspace, ["commit", "--allow-empty", "-m", "source ahead of main"]);
    testState.agentConfig = { workspace };
    testState.agentsConfig = {
      ownership: "explicit",
      entries: { main: {}, "contributor-agent": {} },
    };
    const { dir, storePath } = await createSessionStoreDir();
    const project = await registerProjectRegistry({ path: workspace });
    const profile = ensureProfileForEmail("workspace-contributor@example.test");
    const origin = "https://control.example.test";
    const scopes = ["operator.sessions.read", "operator.sessions.write"] as const;
    const auth = {
      mode: "trusted-proxy" as const,
      trustedProxy: { userHeader: "x-forwarded-user", allowLoopback: true },
    };
    testState.gatewayAuth = auth;
    testState.gatewayControlUi = { allowedOrigins: [origin] };
    const cfg: OpenClawConfig = {
      gateway: {
        auth,
        trustedProxies: ["127.0.0.1"],
        controlUi: { allowedOrigins: [origin] },
        roles: {
          default: "contributor",
          definitions: {
            contributor: {
              sessions: {
                others: "view",
                workspace: { projects: [project.id], worktreeBaseRef: "main" },
              },
              agents: ["contributor-agent"],
              scopes: [...scopes],
            },
          },
        },
      },
    };
    const config = await getGatewayConfigModule();
    await config.writeConfigFile(cfg);
    await withGatewayServer(async ({ port }) => {
      const ws = await openWs(port, {
        origin,
        "x-forwarded-for": "203.0.113.50",
        "x-forwarded-proto": "https",
        "x-forwarded-user": "workspace-contributor@example.test",
      });
      try {
        const connected = await connectReq(ws, {
          skipDefaultAuth: true,
          prePairDevice: true,
          client: CONTROL_UI_CLIENT,
          browserOrigin: origin,
          scopes: [...scopes],
          deviceIdentityPath: path.join(state.root, "contributor-device.sqlite"),
        });
        expect(connected.ok, JSON.stringify(connected.error)).toBe(true);
        const created = await rpcReq<{
          key: string;
          entry: SessionEntry;
          worktree: { id: string; path: string };
        }>(ws, "sessions.create", { agentId: "contributor-agent", projectId: project.id });
        expect(created.ok, JSON.stringify(created.error)).toBe(true);
        const payload = created.payload!;
        expect(payload.key).toMatch(/^agent:contributor-agent:dashboard:/u);
        expect(await requireGit(payload.worktree.path, ["rev-parse", "HEAD"])).toBe(main);
        expect(
          loadSessionEntry({ agentId: "contributor-agent", sessionKey: payload.key, storePath }),
        ).toMatchObject({
          createdActor: { type: "human", source: "profile", id: profile.id },
          requiredWorkspace: { projectId: project.id, worktreeBaseRef: "main" },
          sessionRoot: payload.worktree.path,
        });
        const read = await rpcReq(ws, "sessions.get", { key: payload.key });
        expect(read.ok, JSON.stringify(read.error)).toBe(true);
        const runId = "required-workspace-turn";
        const replyText = "The selected workspace is ready.";
        // Keep the real dispatcher and delivery owner; control only the model reply source.
        gatewayReplyMock.mockResolvedValueOnce({ text: replyText });
        const terminal = onceMessage(
          ws,
          (frame) =>
            frame.type === "event" &&
            frame.event === "chat" &&
            frame.payload?.runId === runId &&
            frame.payload?.sessionKey === payload.key &&
            (frame.payload?.state === "final" ||
              frame.payload?.state === "error" ||
              frame.payload?.state === "aborted"),
        );
        void terminal.catch(() => undefined);
        const accepted = await rpcReq(ws, "chat.send", {
          sessionKey: payload.key,
          message: "inspect the selected workspace",
          idempotencyKey: runId,
        });
        expect(accepted.ok, JSON.stringify(accepted.error)).toBe(true);
        expect(accepted.payload).toMatchObject({ runId, status: "started" });
        expect((await terminal).payload).toMatchObject({
          state: "final",
          message: {
            role: "assistant",
            content: expect.arrayContaining([{ type: "text", text: replyText }]),
          },
        });
        await settleGatewaySessionStoreFixture(dir);
        expect(gatewayReplyMock).toHaveBeenCalledOnce();
        expect(gatewayReplyMock.mock.calls[0]?.[0]).toMatchObject({ SessionKey: payload.key });
        expect(gatewayReplyMock.mock.calls[0]?.[1]).toMatchObject({ runId });

        const key = "agent:contributor-agent:dashboard:old-shared-thread";
        await upsertSessionEntryCore(
          { agentId: "contributor-agent", storePath, sessionKey: key },
          {
            sessionId: "old-shared-thread",
            updatedAt: 1,
            createdActor: { type: "human", source: "profile", id: profile.id },
            createdVia: "operator",
          },
        );
        const continued = await rpcReq(ws, "chat.send", {
          sessionKey: key,
          message: "must select a new workspace",
          idempotencyKey: "old-workspace-turn",
        });
        expect(continued).toMatchObject({ ok: false, error: { code: "FORBIDDEN" } });
        expect(gatewayReplyMock).toHaveBeenCalledOnce();
        cfg.gateway!.roles!.definitions.contributor.sessions.workspace!.projects = [];
        await config.writeConfigFile(cfg);
        const revoked = await rpcReq(ws, "chat.send", {
          sessionKey: payload.key,
          message: "project access revoked",
          idempotencyKey: "revoked-project-turn",
        });
        expect(revoked).toMatchObject({ ok: false, error: { code: "FORBIDDEN" } });
        expect(gatewayReplyMock).toHaveBeenCalledOnce();
      } finally {
        ws.close();
      }
    });
  });
});
