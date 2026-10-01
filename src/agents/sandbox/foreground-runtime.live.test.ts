import fs from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { prepareForegroundTestAdmission } from "../run-execution-policy.test-support.js";
import { resolveSandboxConfigForAgent } from "./config.js";
import {
  bindNativeSandboxEngineTarget,
  captureNativeSandboxEngine,
  DOCKER_SANDBOX_ENGINE,
  PODMAN_SANDBOX_ENGINE,
  execContainer,
  resolveNativeDockerTarget,
  runNativeSandboxCleanup,
} from "./container-engine.js";
import type { NativeSandboxContainerCustody } from "./docker-native-custody.js";
import { ensureSandboxContainer } from "./docker.js";
import { acquireForegroundSandboxCustody } from "./foreground-owner.js";
import { resolvePodmanSandboxRuntimeInfo } from "./podman-runtime.js";
import { readRegistryEntry } from "./registry.js";

// Explicit fixture selection prevents ordinary live-provider runs from creating containers.
const engineId = process.env.OPENCLAW_TEST_SANDBOX_ENGINE;
const image = process.env.OPENCLAW_TEST_SANDBOX_IMAGE;
describe.runIf(
  process.platform === "linux" &&
    Boolean(image) &&
    (engineId === "podman" || engineId === "docker"),
)("foreground native runtime", () => {
  it.each(["normal", "stop"] as const)(
    "retires detached descendants on %s and preserves workspace data",
    async (ending) => {
      await withOpenClawTestState({ label: `foreground-live-${ending}` }, async (fixture) => {
        const prepared = prepareForegroundTestAdmission(`foreground-live-${ending}`);
        const context = await prepared.admit("embedded");
        const signal = new AbortController();
        const custody = acquireForegroundSandboxCustody(context, signal.signal);
        try {
          const selected = captureNativeSandboxEngine(
            engineId === "podman" ? PODMAN_SANDBOX_ENGINE : DOCKER_SANDBOX_ENGINE,
            custody,
          );
          const podman =
            engineId === "podman" ? await resolvePodmanSandboxRuntimeInfo() : undefined;
          if (podman && podman.target.key !== "local")
            throw new Error("This fixture requires local Linux Podman");
          const engine = bindNativeSandboxEngineTarget(
            selected,
            podman?.target ?? (await resolveNativeDockerTarget(selected)),
          );
          const native: NativeSandboxContainerCustody = { engine, custody };
          const defaults = resolveSandboxConfigForAgent();
          const cfg = {
            ...defaults,
            backend: engine.id,
            workspaceAccess: "rw" as const,
            docker: { ...defaults.docker, image: image! },
          };
          // A proof run must use a qualified pre-existing image; never pull an implicit one.
          await execContainer(engine, ["image", "inspect", image!]);
          const allocated = await custody.runProducer(
            () =>
              ensureSandboxContainer({
                native,
                nativePodmanRuntimeInfo: podman,
                engine,
                podmanTarget: podman?.target,
                scopeKey: "agent:main:fixture",
                workspaceDir: fixture.workspaceDir,
                agentWorkspaceDir: fixture.workspaceDir,
                cfg,
              }),
            { settleAfterAbort: true },
          );
          const child = await execContainer(engine, [
            "exec",
            allocated.containerId,
            "python3",
            "-c",
            "import subprocess; p=subprocess.Popen(['sleep','600'],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); print(p.pid)",
          ]);
          const childPid = child.stdout.trim();
          expect(childPid).toMatch(/^\d+$/);
          // The launching command returned, yet the detached descendant is still live.
          await execContainer(engine, [
            "exec",
            allocated.containerId,
            "/bin/sh",
            "-c",
            'kill -0 "$1"',
            "fixture",
            childPid,
          ]);
          await fs.writeFile(fixture.path("workspace", "draft.txt"), "retained draft");
          expect(await readRegistryEntry(allocated.containerName)).toMatchObject({
            foreground: { containerId: allocated.containerId },
            runtimeState: "ready",
          });
          if (ending === "stop") signal.abort();
          await prepared.close();
          expect(await readRegistryEntry(allocated.containerName)).toBeNull();
          expect(await fs.readFile(fixture.path("workspace", "draft.txt"), "utf8")).toBe(
            "retained draft",
          );
          await runNativeSandboxCleanup(engine, async (exec) => {
            const inspection = await exec(["inspect", allocated.containerId], true);
            expect(inspection.code).not.toBe(0);
            expect(inspection.stderr.toString("utf8")).toMatch(/no such|does not exist/i);
          });
        } finally {
          // Only the exact admitted owner may retire this fixture's native allocation.
          try {
            await prepared.close();
          } catch (cause) {
            throw Object.assign(
              new Error(
                "Native fixture cleanup is unconfirmed; retain its workspace and registry for recovery",
                { cause },
              ),
              { processTreeState: "indeterminate" },
            );
          }
        }
      });
    },
    120_000,
  );
});
