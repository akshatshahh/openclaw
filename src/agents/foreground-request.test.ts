import { describe, expect, it } from "vitest";
import { prepareChannelRunAdmission } from "../auto-reply/reply/channel-run-admission.js";
import {
  attachGatewayLocalUserIngress,
  bindGatewayForegroundUserRequest,
  prepareGatewayLocalUserIngress,
} from "../gateway/local-user-ingress.js";
import {
  createAdmittedRunOperatorAuthority,
  createOperationalRunInstanceRef,
  prepareAgentRunAdmission,
  prepareSystemAgentRunAdmission,
} from "./admitted-run-context.js";
import { combineForegroundUserRequests, getForegroundUserRequest } from "./foreground-request.js";
import { requireAdmittedRunForeground } from "./run-execution-policy.js";

function acceptedInput() {
  const client = {};
  attachGatewayLocalUserIngress(
    client,
    prepareGatewayLocalUserIngress({
      authMethod: "token",
      authenticatedUserExpected: true,
      isLocalClient: false,
      profile: { profileId: "source" },
    }),
  );
  const input = {};
  bindGatewayForegroundUserRequest(client, input, () => {});
  return getForegroundUserRequest({ ...input });
}

const restricted = () =>
  createAdmittedRunOperatorAuthority({
    profileId: "source",
    scopes: ["operator.write"],
    assertCurrent() {},
    rolePolicy: {
      sessionAccessCap: "none",
      sandboxRequired: true,
      agents: "*",
      execution: "foreground-only",
    },
  });

describe("accepted foreground input", () => {
  it.each(["heartbeat", "cron", "restart", "continuation"])(
    "does not grant execution from a live %s admission or retained operator",
    async (boundary) => {
      const prepared = prepareSystemAgentRunAdmission({}, boundary, "main", boundary);
      const delegated = prepareSystemAgentRunAdmission(
        {},
        `${boundary}-role`,
        "main",
        boundary,
        undefined,
        restricted(),
      );
      try {
        const context = await prepared.admit("embedded");
        expect(() => requireAdmittedRunForeground(context)).toThrow(
          "fresh authenticated user request",
        );
        await expect(delegated.admit("embedded")).rejects.toThrow(
          "fresh authenticated user request",
        );
      } finally {
        await prepared.close();
        await delegated.close();
      }
    },
  );

  it("retains one collected request across runtime retry but never issues a second incarnation", async () => {
    const sources = [acceptedInput(), acceptedInput()];
    const request = combineForegroundUserRequests(sources);
    const prepare = (runId: string) =>
      prepareChannelRunAdmission({
        cfg: {},
        runId,
        agentId: "main",
        ingressKind: "channel",
        boundary: "test",
        foregroundRequest: request,
        operatorAuthority: restricted(),
      });
    const first = prepare("first");
    const replay = prepare("replay");
    try {
      const context = await first.admit("embedded");
      requireAdmittedRunForeground(context);
      expect(await first.admit("embedded")).toBe(context);
      await first.close();
      await expect(replay.admit("embedded")).rejects.toThrow("another foreground request");
      expect(combineForegroundUserRequests([sources[0], undefined])).toBeUndefined();
    } finally {
      await first.close();
      await replay.close();
    }
  });

  it("does not derive foreground permission from copied audit facts or a recovery token", async () => {
    for (const recovery of [undefined, { retryOnly: true, consume: () => ({ accepted: true }) }]) {
      const prepared = prepareAgentRunAdmission({
        cfg: {},
        operationalRunInstance: createOperationalRunInstanceRef("audit-lookalike"),
        operatorAuthority: restricted(),
        recovery,
        foregroundRequest: recovery ? acceptedInput() : { kind: "foreground-user-request" },
        facts: {
          runId: "audit-lookalike",
          agentId: "main",
          ingress: { kind: "gateway-client", boundary: "authenticated", state: "present" },
          invoker: { state: "present", kind: "person", rawPrincipalRef: "source" },
        },
      });
      try {
        await expect(prepared.admit("embedded")).rejects.toThrow(
          "fresh authenticated user request",
        );
      } finally {
        await prepared.close();
      }
    }
  });
});
