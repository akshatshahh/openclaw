import { createHash, randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "../infra/kysely-sync.js";
import { deferSqlitePostCommitPublication } from "../infra/sqlite-post-commit.js";
import { emitSessionLifecycleEvent } from "../sessions/session-lifecycle-events.js";
import {
  encodeGitHubPublicationRequester,
  type GitHubPublicationRequesterSnapshot,
} from "../state/github-publication-requester.js";
import { ensureGitHubPublicationReviewSchema } from "../state/openclaw-state-db-schema-additive.js";
import { tableExists } from "../state/openclaw-state-db-schema-helpers.js";
import type { DB } from "../state/openclaw-state-db.generated.js";
import {
  openOpenClawStateDatabase,
  runOpenClawStateWriteTransaction,
} from "../state/openclaw-state-db.js";
import type { PublicationSessionIdentity } from "./github-publication-availability.js";
import {
  gitHubPublicationReviewCandidateSchema,
  assertDurableGitHubPublicationReview,
  type GitHubPublicationReviewCandidate,
} from "./github-publication-review-contract.js";

const table = "github_publication_review_candidates";
const query = (db: DatabaseSync) => getNodeSqliteKysely<Pick<DB, typeof table>>(db);
export type GitHubPublicationReviewRow = DB[typeof table];

function changed(db: DatabaseSync, row: GitHubPublicationReviewRow): void {
  deferSqlitePostCommitPublication(db, () => {
    emitSessionLifecycleEvent({
      sessionKey: row.session_key,
      agentId: row.agent_id,
      reason: "github-publication",
    });
  });
}

export function readGitHubPublicationReview(
  selector: { reviewId: string } | { publicationRequestId: string },
  db: DatabaseSync = openOpenClawStateDatabase().db,
): GitHubPublicationReviewRow | undefined {
  if (!tableExists(db, table)) {
    return undefined;
  }
  return executeSqliteQueryTakeFirstSync(
    db,
    query(db)
      .selectFrom(table)
      .selectAll()
      .where(
        "reviewId" in selector ? "review_id" : "publication_request_id",
        "=",
        "reviewId" in selector ? selector.reviewId : selector.publicationRequestId,
      ),
  );
}

export function listGitHubPublicationReviews(session: PublicationSessionIdentity) {
  const db = openOpenClawStateDatabase().db;
  if (!tableExists(db, table)) {
    return [];
  }
  return executeSqliteQuerySync(
    db,
    query(db)
      .selectFrom(table)
      .selectAll()
      .where("session_id", "=", session.sessionId)
      .where("session_key", "=", session.sessionKey)
      .where("agent_id", "=", session.agentId)
      .orderBy("created_at_ms", "desc")
      .orderBy("review_id", "desc")
      .limit(20),
  ).rows;
}

export function findGitHubPublicationReview(input: {
  sessionId: string;
  profileId: string;
  idempotencyKey: string;
}) {
  const db = openOpenClawStateDatabase().db;
  if (!tableExists(db, table)) return undefined;
  return executeSqliteQueryTakeFirstSync(
    db,
    query(db)
      .selectFrom(table)
      .selectAll()
      .where("session_id", "=", input.sessionId)
      .where("requester_profile_id", "=", input.profileId)
      .where("idempotency_key", "=", input.idempotencyKey),
  );
}

export function listUnreportedGitHubPublicationReviews() {
  const db = openOpenClawStateDatabase().db;
  if (!tableExists(db, table)) return [];
  return executeSqliteQuerySync(
    db,
    query(db)
      .selectFrom(table)
      .selectAll()
      .where("reported_at_ms", "is", null)
      .where("publication_request_id", "is", null)
      .orderBy("created_at_ms")
      .limit(100),
  ).rows;
}

export function markGitHubPublicationReviewReported(reviewId: string): void {
  const db = openOpenClawStateDatabase().db;
  if (!tableExists(db, table)) return;
  runOpenClawStateWriteTransaction(
    ({ db }) => {
      executeSqliteQuerySync(
        db,
        query(db)
          .updateTable(table)
          .set({ reported_at_ms: Date.now() })
          .where("review_id", "=", reviewId)
          .where("reported_at_ms", "is", null),
      );
    },
    undefined,
    { operationLabel: "github-publication.review-reported" },
  );
}

export function retireGitHubPublicationReviewReport(
  input: PublicationSessionIdentity & { reviewId: string },
): void {
  const db = openOpenClawStateDatabase().db;
  if (!tableExists(db, table)) return;
  runOpenClawStateWriteTransaction(
    ({ db }) => {
      const row = executeSqliteQueryTakeFirstSync(
        db,
        query(db)
          .updateTable(table)
          .set({
            reported_at_ms: Date.now(),
            stale_reason:
              "This conversation generation ended. Its review notification was not delivered to the replacement conversation.",
          })
          .where("review_id", "=", input.reviewId)
          .where("session_id", "=", input.sessionId)
          .where("session_key", "=", input.sessionKey)
          .where("agent_id", "=", input.agentId)
          .where(
            "lifecycle_revision",
            input.lifecycleRevision == null ? "is" : "=",
            input.lifecycleRevision ?? null,
          )
          .where("publication_request_id", "is", null)
          .where("reported_at_ms", "is", null)
          .returningAll(),
      );
      if (row) changed(db, row);
    },
    undefined,
    { operationLabel: "github-publication.review-report-retire" },
  );
}

function digest(
  row: Pick<
    GitHubPublicationReviewRow,
    | "review_id"
    | "session_id"
    | "session_key"
    | "agent_id"
    | "lifecycle_revision"
    | "requester_profile_id"
    | "requester_authority_json"
    | "candidate_json"
  >,
): string {
  return createHash("sha256")
    .update(
      JSON.stringify([
        row.review_id,
        row.session_id,
        row.session_key,
        row.agent_id,
        row.lifecycle_revision,
        row.requester_profile_id,
        row.requester_authority_json,
        row.candidate_json,
      ]),
    )
    .digest("hex");
}

export function readGitHubPublicationReviewCandidate(row: GitHubPublicationReviewRow) {
  if (!row.candidate_json || !row.candidate_digest || digest(row) !== row.candidate_digest) {
    throw new Error("The publication review has no intact candidate; request a new review.");
  }
  return gitHubPublicationReviewCandidateSchema.parse(JSON.parse(row.candidate_json));
}

/** A guest's inert request and a maintainer's candidate always receive separate identities. */
export function insertGitHubPublicationReview(input: {
  session: PublicationSessionIdentity;
  idempotencyKey: string;
  profileId: string;
  requestedReviewId?: string;
  reviewed?: {
    candidate: GitHubPublicationReviewCandidate;
    requester: GitHubPublicationRequesterSnapshot;
  };
  assertCurrent: () => void;
}): GitHubPublicationReviewRow {
  assertDurableGitHubPublicationReview(input.session.sessionKey);
  input.assertCurrent();
  const candidateJson = input.reviewed
    ? JSON.stringify(gitHubPublicationReviewCandidateSchema.parse(input.reviewed.candidate))
    : null;
  const row: GitHubPublicationReviewRow = {
    review_id: randomUUID(),
    requested_review_id: input.requestedReviewId ?? null,
    idempotency_key: input.idempotencyKey,
    session_id: input.session.sessionId,
    session_key: input.session.sessionKey,
    agent_id: input.session.agentId,
    lifecycle_revision: input.session.lifecycleRevision ?? null,
    requester_profile_id: input.profileId,
    requester_authority_json: input.reviewed
      ? encodeGitHubPublicationRequester(input.reviewed.requester)
      : null,
    candidate_json: candidateJson,
    candidate_digest: null,
    publication_request_id: null,
    stale_reason: null,
    created_at_ms: Date.now(),
    reported_at_ms: null,
  };
  if (candidateJson) {
    row.candidate_digest = digest(row);
  }
  return runOpenClawStateWriteTransaction(
    ({ db }) => {
      ensureGitHubPublicationReviewSchema(db);
      input.assertCurrent();
      const existing = executeSqliteQueryTakeFirstSync(
        db,
        query(db)
          .selectFrom(table)
          .selectAll()
          .where("session_id", "=", row.session_id)
          .where("requester_profile_id", "=", row.requester_profile_id)
          .where("idempotency_key", "=", row.idempotency_key),
      );
      if (existing) {
        if (
          existing.candidate_json !== candidateJson ||
          existing.requested_review_id !== row.requested_review_id ||
          existing.requester_authority_json !== row.requester_authority_json ||
          existing.lifecycle_revision !== row.lifecycle_revision
        ) {
          throw new Error("Publication review idempotency key was reused; start a new review.");
        }
        return existing;
      }
      if (input.requestedReviewId) {
        const request = readGitHubPublicationReview({ reviewId: input.requestedReviewId }, db);
        if (
          !request ||
          request.candidate_json ||
          request.session_id !== row.session_id ||
          request.session_key !== row.session_key ||
          request.agent_id !== row.agent_id ||
          request.lifecycle_revision !== row.lifecycle_revision
        ) {
          throw new Error("The original review request no longer matches this session.");
        }
      }
      executeSqliteQuerySync(db, query(db).insertInto(table).values(row));
      input.assertCurrent();
      changed(db, row);
      return row;
    },
    undefined,
    { operationLabel: "github-publication.review" },
  );
}

/** Called inside the publication receipt transaction, before any execution can observe it. */
export function bindGitHubPublicationReviewRequest(
  db: DatabaseSync,
  row: GitHubPublicationReviewRow,
  requestId: string,
  assertCurrent: () => void,
): void {
  assertCurrent();
  const current = readGitHubPublicationReview({ reviewId: row.review_id }, db);
  if (
    current?.candidate_digest !== row.candidate_digest ||
    current.stale_reason ||
    !current.candidate_json ||
    (current.publication_request_id && current.publication_request_id !== requestId)
  ) {
    throw new Error("The reviewed candidate was already consumed or changed.");
  }
  if (current.publication_request_id === requestId) return;
  const updated = executeSqliteQueryTakeFirstSync(
    db,
    query(db)
      .updateTable(table)
      .set({ publication_request_id: requestId })
      .where("review_id", "=", row.review_id)
      .where("candidate_digest", "=", row.candidate_digest)
      .where("publication_request_id", "is", null)
      .where("stale_reason", "is", null)
      .returningAll(),
  );
  if (!updated) throw new Error("The reviewed candidate was already consumed or changed.");
  assertCurrent();
  changed(db, updated);
}

export function markGitHubPublicationReviewStale(
  row: GitHubPublicationReviewRow,
  reason: string,
): void {
  runOpenClawStateWriteTransaction(
    ({ db }) => {
      const updated = executeSqliteQueryTakeFirstSync(
        db,
        query(db)
          .updateTable(table)
          .set({ stale_reason: reason })
          .where("review_id", "=", row.review_id)
          .where(
            "candidate_digest",
            row.candidate_digest === null ? "is" : "=",
            row.candidate_digest,
          )
          .where("stale_reason", "is", null)
          .returningAll(),
      );
      if (updated) changed(db, updated);
    },
    undefined,
    { operationLabel: "github-publication.review-stale" },
  );
}
