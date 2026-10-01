// Install shared transport mocks before publication owners enter the module cache.
// oxfmt-ignore
import { createGitHubPublicationRequesterFixture, createRealPublicationWorkspace, createTestGitHubPublicationCoordinator, createTestGitHubPublicationRuntime, persistPublicationTestSession, githubPublicationTestMocks, installGitHubPublicationTestHarness } from "./github-publication.test-support.js";
import fs from "node:fs/promises";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { withGatewayToolCallerIdentity } from "../agents/tools/gateway-caller-context.js";
import { createGitHubPublishTool } from "../agents/tools/github-publish-tool.js";
import { loadSessionEntryReadOnly } from "../config/sessions/session-accessor.js";
import { withPluginRuntimeGatewayRequestScope } from "../plugins/runtime/gateway-request-scope.js";
import { preparePersonalGitHubSessionReceiptDeletion } from "../state/github-personal-publication-lifecycle.js";
import {
  openOpenClawStateDatabase,
  runOpenClawStateWriteTransaction,
} from "../state/openclaw-state-db.js";
import { GitHubPublicationRequesterUnavailableError } from "./github-publication-failure.js";
import {
  createRequesterPolicyFixture,
  createRequesterPublicationFixture,
  guestScopes,
  publisherScopes,
  holdWorkerTurn,
  prepareVisitorPublicationFixture,
} from "./github-publication-requester.test-support.js";
import {
  insertGitHubPublicationReview,
  readGitHubPublicationReview,
  readGitHubPublicationReviewCandidate,
  listGitHubPublicationReviews,
  listUnreportedGitHubPublicationReviews,
  markGitHubPublicationReviewStale,
} from "./github-publication-review-store.js";
import {
  prepareGitHubPublicationReviewConfirmation,
  projectGitHubPublicationReview,
  readGitHubPublicationReviewDiff,
} from "./github-publication-review.js";
import { captureGatewayOperatorRunAuthority } from "./operator-run-authority.js";
import { createContext } from "./server-plugin-in-process-dispatch.test-support.js";
import { createWorkerSessionPlacementStore } from "./worker-environments/placement-store.js";
const mocks = githubPublicationTestMocks();
const checkpoint = vi.hoisted(() => vi.fn());
vi.mock("./worker-environments/session-repository-checkpoints.js", () => ({
  withSessionRepositoryCheckpoint: (...args: unknown[]) => checkpoint(...args),
}));
const fixture = createRequesterPublicationFixture.bind(undefined, checkpoint);

describe("reviewed publication from restricted conversations", () => {
  installGitHubPublicationTestHarness({
    creatorEmail: "publication-guest@example.test",
    sandbox: "required",
    realWorktree: true,
  });
  it("refuses Incognito review before persisting intent or preparing a publisher", async () => {
    const f = await fixture("local");
    const session = {
      ...f.currentReviewSession(),
      sessionKey: "agent:main:dashboard:incognito-review",
    };
    const prepareIdentity = vi.fn();
    expect(() =>
      insertGitHubPublicationReview({
        session,
        profileId: f.guestProfile,
        idempotencyKey: "incognito",
        assertCurrent: () => {},
      }),
    ).toThrow("Incognito");
    await expect(
      f.coordinator.prepareReview({
        session,
        request: { action: "prepare", sessionKey: session.sessionKey, idempotencyKey: "incognito" },
        requester: f.maintainer,
        prepareIdentity,
        signal: new AbortController().signal,
      }),
    ).rejects.toThrow("Incognito");
    expect(prepareIdentity).not.toHaveBeenCalled();
    expect(listGitHubPublicationReviews(session)).toEqual([]);
    expect(f.coordinator.listUnreportedResults()).toEqual([]);
  });
  it("denies an active Visitor's publication without changing its restricted role", async () => {
    const f = await createRequesterPolicyFixture();
    const visitors = await prepareVisitorPublicationFixture(f);
    try {
      await visitors.start();
      await visitors.execute("visitor_invite", {
        email: "publication-guest@example.test",
        days: 1,
      });
      await expect(
        createGitHubPublicationRequesterFixture({
          profileId: f.guestProfile,
          scopes: guestScopes,
          ...f.guestSource.session,
        }),
      ).rejects.toThrow(GitHubPublicationRequesterUnavailableError);
      expect(f.externalWrites).toEqual([]);
    } finally {
      await visitors.close();
    }
  });

  it.each(["local", "repository"] as const)(
    "requires a distinct maintainer candidate for %s work",
    async (backend) => {
      const f = await fixture(backend);
      await expect(
        f.coordinator.requestForSession(f.request("unreviewed", f.publisher)),
      ).rejects.toThrow("reviewed publication candidate");
      const requested = insertGitHubPublicationReview({
        session: f.currentReviewSession(),
        idempotencyKey: "guest-review",
        profileId: f.guestProfile,
        assertCurrent: () => {},
      });
      expect(requested).toMatchObject({
        candidate_json: null,
        candidate_digest: null,
        requester_authority_json: null,
        publication_request_id: null,
      });
      expect(f.externalWrites).toEqual([]);
      const input = await f.reviewedRequest("reviewed", f.publisher);
      const candidate = readGitHubPublicationReview({ reviewId: input.preparedReview.id })!;
      expect(candidate.review_id).not.toBe(requested.review_id);
      expect(readGitHubPublicationReviewCandidate(candidate).diff).toContain(
        backend === "local" ? "accepted" : "accepted first",
      );
      expect(f.externalWrites).toEqual([]);
      expect(await f.coordinator.requestForSession(input)).toMatchObject({ status: "published" });
      expect(f.publishedTitles).toEqual(["reviewed"]);
      expect(
        f.readRequester(
          readGitHubPublicationReview({ reviewId: candidate.review_id })!.publication_request_id!,
        ),
      ).toEqual(f.publisher.snapshot);
      expect(readGitHubPublicationReview({ reviewId: requested.review_id })).toEqual(requested);
      expect(
        loadSessionEntryReadOnly({ agentId: "main", sessionKey: f.session.sessionKey }),
      ).toMatchObject({ sandbox: "required", createdActor: { id: f.guestProfile } });
      for (const cwd of f.repository?.reviewWorkspaces ?? [])
        await expect(fs.stat(cwd)).rejects.toMatchObject({ code: "ENOENT" });
    },
  );

  it.each(["local", "repository"] as const)(
    "publishes a fresh maintainer tool confirmation on its own active %s claim",
    async (backend) => {
      const f = await fixture(backend);
      const claim = await holdWorkerTurn(f);
      const context = {
        ...createContext(),
        ...f.maintainerSource.context,
        githubPublicationService: f.coordinator,
      };
      const client = f.maintainerSource.client;
      const source = await captureGatewayOperatorRunAuthority({ client, context });
      if (!source) throw new Error("Expected a captured maintainer source");
      let receiptId: string;
      try {
        receiptId = await withPluginRuntimeGatewayRequestScope(
          { context, client, isWebchatConnect: () => false },
          () =>
            withGatewayToolCallerIdentity(
              {
                agentId: "main",
                sessionKey: f.session.sessionKey,
                operatorAuthority: source.authority,
                operationalRunInstance: { instanceId: "review-tool-instance", runId: claim.runId },
                receiptAuthority: () => source.authority.assertCurrent(),
                gatewayContextResolver: () => context,
              },
              async () => {
                const tool = createGitHubPublishTool();
                const prepared = (
                  await tool.execute("active-prepare", {
                    action: "prepare",
                    title: "Active maintainer review",
                  })
                ).details as { reviewId: string; digest: string };
                const review = { reviewId: prepared.reviewId, digest: prepared.digest };
                let offset: number | null = 0;
                do {
                  const page = (
                    await tool.execute(`active-diff-${offset}`, { action: "diff", review, offset })
                  ).details as { nextOffset: number | null };
                  offset = page.nextOffset;
                } while (offset !== null);
                const accepted = (
                  await tool.execute("active-confirm", { action: "confirm", review })
                ).details as { requestId: string; status: string };
                expect(accepted.status).toBe("requested");
                return accepted.requestId;
              },
            ),
        );
      } finally {
        source.release();
      }
      expect(f.externalWrites).toEqual([]);
      f.placements.markWorkspaceResultPending(claim);
      await f.coordinator.prepareClaimWorkspace(claim);
      f.placements.acceptWorkspaceResult(claim);
      expect(await f.coordinator.processClaim(claim)).toContainEqual(
        expect.objectContaining({ requestId: receiptId!, status: "published" }),
      );
      expect(f.readRequester(receiptId!)?.actor).toEqual({
        kind: "operator",
        profileId: f.maintainerProfile,
      });
      expect(f.publishedTitles).toEqual(["Active maintainer review"]);
    },
  );

  it.each(["local", "repository"] as const)(
    "retains only exact reviewed %s publication across completion of its own invocation",
    async (backend) => {
      const f = await fixture(backend);
      const input = await f.reviewedRequest("own-confirmation", f.publisher);
      const claim = await holdWorkerTurn(f);
      const accepted = await f.coordinator.requestForClaim({ ...input, claim });
      const original = f.readRequester(accepted.requestId);
      f.publisherSource.release();
      expect(f.publisher.assertCurrent).toThrow();
      f.placements.markWorkspaceResultPending(claim);
      await f.coordinator.prepareClaimWorkspace(claim);
      if (f.repository) await f.repository.capture("accepted first\n", "same-reviewed-source");
      f.placements.acceptWorkspaceResult(claim);
      expect(await f.coordinator.processClaim(claim)).toContainEqual(
        expect.objectContaining({ requestId: accepted.requestId, status: "published" }),
      );
      expect(f.readRequester(accepted.requestId)).toEqual(original);
      expect(f.publishedTitles).toEqual(["own-confirmation"]);
    },
  );

  it.each(["local", "repository"] as const)(
    "does not automatically resume a reviewed %s candidate after restart",
    async (backend) => {
      const f = await fixture(backend);
      const input = await f.reviewedRequest("restart", f.publisher);
      const claim = await holdWorkerTurn(f);
      const accepted = await f.coordinator.requestForSession(input);
      f.publisherSource.release();
      await f.placements.releaseTurn(claim);
      const restarted = f.restart();
      await restarted.resumeSessionRequests();
      expect(f.externalWrites).toEqual([]);
      expect(
        (await restarted.sharedStatus(f.currentReviewSession(), accepted.requestId))?.result.status,
      ).toBe("needs_confirmation");
      const fresh = await createGitHubPublicationRequesterFixture({
        profileId: f.publisherProfile,
        scopes: publisherScopes,
        ...f.publisherSource.session,
      });
      const preparedReview = await prepareGitHubPublicationReviewConfirmation(
        { reviewId: input.preparedReview.id, digest: input.preparedReview.digest },
        f.currentReviewSession(),
        fresh.requester,
      );
      expect(
        await restarted.requestForSession({ ...input, requester: fresh.requester, preparedReview }),
      ).toMatchObject({ status: "published" });
    },
  );

  it.each(["local", "repository"] as const)(
    "rejects changed %s source rather than recapturing an approved candidate",
    async (backend) => {
      const f = await fixture(backend);
      const input = await f.reviewedRequest("source-stale", f.publisher);
      if (f.local) await fs.writeFile(path.join(f.local.cwd, "artifact.txt"), "unreviewed edit\n");
      else await f.repository!.capture("unreviewed edit\n", "changed");
      if (backend === "repository")
        await expect(f.coordinator.requestForSession(input)).rejects.toThrow(
          "reviewed workspace changed",
        );
      else
        expect(await f.coordinator.requestForSession(input)).toMatchObject({
          status: "failed",
          code: "workspace_changed",
        });
      expect(f.externalWrites).toEqual([]);
    },
  );

  it.each(["local", "repository"] as const)(
    "ends retained %s confirmation when the original publisher loses access",
    async (backend) => {
      const f = await fixture(backend);
      const input = await f.reviewedRequest("revoked-hold", f.publisher);
      const claim = await holdWorkerTurn(f);
      await f.coordinator.requestForClaim({ ...input, claim });
      f.publisherSource.release();
      await f.revoke();
      f.placements.markWorkspaceResultPending(claim);
      await f.coordinator.prepareClaimWorkspace(claim);
      f.placements.acceptWorkspaceResult(claim);
      await f.coordinator.processClaim(claim);
      expect(f.externalWrites).toEqual([]);
    },
  );

  it("keeps direct confirmation pinned even when a later checkpoint has identical bytes", async () => {
    const f = await fixture("repository");
    const input = await f.reviewedRequest("direct-checkpoint", f.publisher);
    await f.repository!.capture("accepted first\n", "same-bytes-new-checkpoint");
    expect(input.preparedReview.assertCurrent).toThrow("reviewed workspace changed");
    await expect(f.coordinator.requestForSession(input)).rejects.toThrow(
      "reviewed workspace changed",
    );
    expect(f.externalWrites).toEqual([]);
  });
  it("returns the same immutable candidate on a lost prepare response and binds it only once", async () => {
    const f = await fixture("local");
    const input = await f.reviewedRequest("idempotent", f.publisher);
    const row = readGitHubPublicationReview({ reviewId: input.preparedReview.id })!;
    await fs.writeFile(path.join(f.local!.cwd, "artifact.txt"), "new unreviewed bytes\n");
    const replay = await f.reviewedRequest("idempotent", f.publisher);
    expect(replay.preparedReview.id).toBe(input.preparedReview.id);
    expect(replay.preparedReview.candidate).toEqual(input.preparedReview.candidate);
    await expect(f.reviewedRequest("idempotent", f.publisher, "different title")).rejects.toThrow(
      "idempotency key",
    );
    runOpenClawStateWriteTransaction(({ db }) => input.preparedReview.bindRequest(db, "receipt-1"));
    expect(() =>
      runOpenClawStateWriteTransaction(({ db }) =>
        input.preparedReview.bindRequest(db, "receipt-2"),
      ),
    ).toThrow("already consumed");
    expect(readGitHubPublicationReview({ reviewId: row.review_id })?.publication_request_id).toBe(
      "receipt-1",
    );
  });

  it("keeps diff reads bounded, explicit and exactly bound to the candidate", async () => {
    const f = await fixture("local");
    await fs.writeFile(path.join(f.local!.cwd, "large.txt"), "review line\n".repeat(800));
    const input = await f.reviewedRequest("pages", f.publisher);
    const row = readGitHubPublicationReview({ reviewId: input.preparedReview.id })!;
    const ref = { reviewId: row.review_id, digest: row.candidate_digest! };
    const chunks: string[] = [];
    let offset = 0;
    for (;;) {
      const page = readGitHubPublicationReviewDiff(row, ref, offset);
      expect(page.text.length).toBeLessThanOrEqual(4096);
      expect(Object.keys(page).toSorted()).toEqual([
        "complete",
        "digest",
        "nextOffset",
        "offset",
        "reviewId",
        "text",
        "totalCharacters",
      ]);
      chunks.push(page.text);
      if (page.complete) break;
      offset = page.nextOffset!;
    }
    expect(chunks.join("")).toBe(input.preparedReview.candidate.diff);
    expect(projectGitHubPublicationReview(row)).not.toHaveProperty("diff");
    expect(() =>
      readGitHubPublicationReviewDiff(row, { ...ref, digest: "0".repeat(64) }, 0),
    ).toThrow("changed");
    expect(() =>
      readGitHubPublicationReviewDiff(row, ref, input.preparedReview.candidate.diff.length + 1),
    ).toThrow("outside");
    expect(() =>
      readGitHubPublicationReviewCandidate({
        ...row,
        candidate_json: row.candidate_json!.replace("review line", "altered line"),
      }),
    ).toThrow("intact");
    markGitHubPublicationReviewStale(row, "Changed reviewed source");
    expect(input.preparedReview.assertCurrent).toThrow("no longer current");
    expect(listGitHubPublicationReviews(f.currentReviewSession())).toHaveLength(1);
  });
  it("retires a full page of replaced-generation notifications without starving current review", async () => {
    const f = await fixture("local");
    const session = f.currentReviewSession();
    const oldIds: string[] = [];
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now++);
    let currentId: string;
    try {
      for (let index = 0; index < 100; index++)
        oldIds.push(
          insertGitHubPublicationReview({
            session: { ...session, lifecycleRevision: "previous-generation" },
            profileId: f.guestProfile,
            idempotencyKey: `old-${index}`,
            assertCurrent: () => {},
          }).review_id,
        );
      currentId = insertGitHubPublicationReview({
        session,
        profileId: f.guestProfile,
        idempotencyKey: "current-review",
        assertCurrent: () => {},
      }).review_id;
    } finally {
      clock.mockRestore();
    }
    const warn = vi.fn();
    const runtime = createTestGitHubPublicationRuntime({
      placements: f.placements,
      loadSessionRuntime: () => import("./session-utils.js"),
      warn,
    });
    await runtime.reconcilePublications();
    expect(warn).not.toHaveBeenCalled();
    for (const reviewId of oldIds)
      expect(readGitHubPublicationReview({ reviewId })).toMatchObject({
        reported_at_ms: expect.any(Number),
        stale_reason: expect.stringContaining("not delivered"),
      });
    expect(listUnreportedGitHubPublicationReviews().map((row) => row.review_id)).toEqual([
      currentId!,
    ]);
    await runtime.reconcilePublications();
    expect(readGitHubPublicationReview({ reviewId: currentId! })).toMatchObject({
      reported_at_ms: expect.any(Number),
      stale_reason: null,
    });
  });
  it("deletes the captured generation including late review intents, while retaining a replacement", async () => {
    const f = await fixture("local");
    const input = await f.reviewedRequest("delete", f.publisher);
    const generation = f.currentReviewSession();
    const remove = await preparePersonalGitHubSessionReceiptDeletion({
      agentId: "main",
      generations: [
        {
          sessionKey: generation.sessionKey,
          sessionId: generation.sessionId,
          lifecycleRevision: generation.lifecycleRevision,
        },
      ],
    });
    const late = insertGitHubPublicationReview({
      session: generation,
      profileId: f.guestProfile,
      idempotencyKey: "late",
      assertCurrent: () => {},
    });
    const replacement = insertGitHubPublicationReview({
      session: { ...generation, lifecycleRevision: "replacement-generation" },
      profileId: f.guestProfile,
      idempotencyKey: "replacement",
      assertCurrent: () => {},
    });
    await remove();
    expect(readGitHubPublicationReview({ reviewId: input.preparedReview.id })).toBeUndefined();
    expect(readGitHubPublicationReview({ reviewId: late.review_id })).toBeUndefined();
    expect(readGitHubPublicationReview({ reviewId: replacement.review_id })).toEqual(replacement);
    expect(input.preparedReview.assertCurrent).toThrow("no longer current");
  });
});

describe("unrestricted Incognito publication", () => {
  installGitHubPublicationTestHarness({ realWorktree: true });
  it("preserves explicit broad System publication without durable review", async () => {
    const sessionKey = "agent:main:dashboard:incognito-publication";
    const workspace = await createRealPublicationWorkspace(undefined, sessionKey);
    await persistPublicationTestSession(sessionKey);
    const placements = createWorkerSessionPlacementStore({ database: openOpenClawStateDatabase() });
    const coordinator = createTestGitHubPublicationCoordinator({ placements });
    expect(
      await coordinator.requestForSession({
        sessionKey,
        agentId: "main",
        idempotencyKey: "incognito-direct",
      }),
    ).toMatchObject({ status: "published" });
    expect(workspace.effects).toEqual(["push", "pull_request"]);
  });
});
