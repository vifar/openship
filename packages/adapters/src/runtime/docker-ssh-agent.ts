import net from "node:net";
import type { Duplex } from "node:stream";
import { spawn, type ChildProcess } from "node:child_process";
import { access, chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { ClientChannel } from "ssh2";

import {
  connectSshClient,
  execSshCommand,
  openSshUnixSocket,
  type StreamLocalCapableClient,
} from "../system/ssh-client";
import type { SshConfig, CommandExecutor } from "../types";
import type { DockerConnectionOptions } from "./docker-transport";
import { safeErrorMessage } from "@repo/core";

const DEFAULT_REMOTE_DOCKER_SOCKET_PATH = "/var/run/docker.sock";
const resolvedDockerSocketPathCache = new WeakMap<DockerConnectionOptions, Promise<string>>();

function toSshConfig(opts: DockerConnectionOptions): SshConfig {
  return {
    host: opts.host ?? "",
    port: opts.port ?? 22,
    username: opts.username,
    hostVerifier: opts.hostVerifier,
    password: opts.password,
    privateKey: opts.privateKey,
    privateKeyPassphrase: opts.privateKeyPassphrase,
    sshAgent: opts.sshAgent,
  };
}

function getConfiguredDockerSocketPath(opts: DockerConnectionOptions): string | null {
  const socketPath = opts.dockerSocketPath?.trim();
  return socketPath ? socketPath : null;
}

function getFallbackDockerSocketPath(opts: DockerConnectionOptions): string {
  return getConfiguredDockerSocketPath(opts) ?? DEFAULT_REMOTE_DOCKER_SOCKET_PATH;
}

function normalizeSocketPathLines(lines: string[]): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];

  for (const line of lines) {
    const socketPath = line.trim();
    if (!socketPath.startsWith("/")) {
      continue;
    }
    if (seen.has(socketPath)) {
      continue;
    }
    seen.add(socketPath);
    normalized.push(socketPath);
  }

  return normalized;
}

const DOCKER_SOCKET_DISCOVERY_SCRIPT = [
  "set -eu",
  'uid="$(id -u 2>/dev/null || printf 0)"',
  'printf "%s\\n" "/var/run/docker.sock" "/run/docker.sock" "/run/podman/podman.sock" "/run/user/$uid/docker.sock" "$HOME/.docker/run/docker.sock" | while IFS= read -r candidate; do if [ -S "$candidate" ]; then printf "%s\\n" "$candidate"; fi; done',
  'find /run/user -maxdepth 2 -type s \\( -name docker.sock -o -name podman.sock \\) -print 2>/dev/null || true',
  'for dir in /run /var/run "$HOME/.docker/run"; do',
  '  if [ -d "$dir" ]; then',
  '    find "$dir" -maxdepth 3 -type s \\( -name docker.sock -o -name podman.sock \\) -print 2>/dev/null || true',
  "  fi",
  "done",
].join("\n");

async function discoverRemoteDockerSocketPathsWithClient(
  client: StreamLocalCapableClient,
): Promise<string[]> {
  const result = await execSshCommand(client, DOCKER_SOCKET_DISCOVERY_SCRIPT);
  const lines = [result.stdout, result.stderr]
    .filter(Boolean)
    .flatMap((text) => text.split(/\r?\n/))
    .map((line) => line.trim())
    .filter(Boolean);

  return normalizeSocketPathLines(lines);
}

async function discoverRemoteDockerSocketPathsWithExecutor(
  executor: CommandExecutor,
): Promise<string[]> {
  try {
    const output = await executor.exec(DOCKER_SOCKET_DISCOVERY_SCRIPT, { timeout: 10_000 });
    return normalizeSocketPathLines(output.split(/\r?\n/));
  } catch {
    return [];
  }
}

async function discoverRemoteDockerSocketPaths(
  opts: DockerConnectionOptions,
): Promise<string[]> {
  // Use pooled executor when available - no extra SSH connection needed
  if (opts.executor) {
    return discoverRemoteDockerSocketPathsWithExecutor(opts.executor);
  }

  let conn: StreamLocalCapableClient | null = null;

  try {
    conn = await connectSshClient(toSshConfig(opts));
    return await discoverRemoteDockerSocketPathsWithClient(conn);
  } finally {
    conn?.end();
  }
}

async function resolveRemoteDockerSocketPath(
  opts: DockerConnectionOptions,
): Promise<string> {
  const configuredSocketPath = getConfiguredDockerSocketPath(opts);
  if (configuredSocketPath) {
    return configuredSocketPath;
  }

  const cachedPath = resolvedDockerSocketPathCache.get(opts);
  if (cachedPath) {
    return cachedPath;
  }

  const pendingPath = discoverRemoteDockerSocketPaths(opts)
    .then((paths) => paths[0] ?? DEFAULT_REMOTE_DOCKER_SOCKET_PATH)
    .catch(() => DEFAULT_REMOTE_DOCKER_SOCKET_PATH);

  resolvedDockerSocketPathCache.set(opts, pendingPath);
  return pendingPath;
}

function shouldCollectSocketDiagnostics(error: unknown): boolean {
  const message = safeErrorMessage(error);
  return /channel open failure|open failed/i.test(message);
}

function formatSocketDiagnostics(lines: string[]): string {
  if (lines.length === 0) {
    return "";
  }

  return ` Remote diagnostics: ${lines.join("; ")}.`;
}

async function collectDockerSocketDiagnostics(
  opts: DockerConnectionOptions,
  socketPath: string,
): Promise<string[]> {
  let conn: StreamLocalCapableClient | null = null;

  try {
    conn = await connectSshClient(toSshConfig(opts));

    const escapedPath = JSON.stringify(socketPath);
    const command = [
      "set -eu",
      'printf "user=%s\\n" "$(whoami)"',
      'printf "groups=%s\\n" "$(id -Gn 2>/dev/null || true)"',
      `if [ -S ${escapedPath} ]; then`,
      `  printf 'socket=yes path=%s\\n' ${escapedPath}`,
      `  ls -ld ${escapedPath}`,
      "else",
      `  printf 'socket=no path=%s\\n' ${escapedPath}`,
      `  if [ -e ${escapedPath} ]; then ls -ld ${escapedPath}; fi`,
      "fi",
    ].join("\n");

    const result = await execSshCommand(conn, command);
    const lines = [result.stdout, result.stderr]
      .filter(Boolean)
      .flatMap((text) => text.split(/\r?\n/))
      .map((line) => line.trim())
      .filter(Boolean);

    if (result.code !== 0 && lines.length === 0) {
      return [`remote diagnostic exited with code ${result.code}`];
    }

    if (!getConfiguredDockerSocketPath(opts)) {
      const discoveredPaths = await discoverRemoteDockerSocketPathsWithClient(conn).catch(() => []);
      lines.push(
        discoveredPaths.length > 0
          ? `discovered_sockets=${discoveredPaths.join(",")}`
          : "discovered_sockets=none",
      );
    }

    return lines;
  } catch (error) {
    return [
      `remote diagnostic failed: ${safeErrorMessage(error)}`,
    ];
  } finally {
    conn?.end();
  }
}

export async function probeDockerSshBridge(opts: DockerConnectionOptions): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let conn: StreamLocalCapableClient | null = null;

    resolveRemoteDockerSocketPath(opts)
      .then((socketPath) =>
        connectSshClient(toSshConfig(opts)).then((client) => ({ client, socketPath })),
      )
      .then(async ({ client, socketPath }) => {
        conn = client;
        let stream: ClientChannel;

        try {
          stream = await openSshUnixSocket(client, socketPath);
        } catch (error) {
          throw new Error(
            `SSH session established, but opening a streamlocal channel to ${socketPath} failed: ${safeErrorMessage(error)}`,
          );
        }

        stream.once("close", () => {
          client.end();
        });
        stream.end();
        resolve();
      })
      .catch((error) => {
        conn?.end();
        reject(error instanceof Error ? error : new Error(String(error)));
      });
  });
}

export async function verifyDockerSshBridge(opts: DockerConnectionOptions): Promise<void> {
  const socketPath = await resolveRemoteDockerSocketPath(opts).catch(() => getFallbackDockerSocketPath(opts));

  // Fast path: use pooled executor’s streamlocal to verify
  if (opts.executor?.forwardUnixSocket) {
    try {
      const stream = await opts.executor.forwardUnixSocket(socketPath);
      stream.destroy();
      return;
    } catch (error) {
      const diagnostics = shouldCollectSocketDiagnostics(error)
        ? formatSocketDiagnostics(await collectDockerSocketDiagnostics(opts, socketPath))
        : "";

      throw new Error(
        `Cannot reach Docker daemon: ${safeErrorMessage(error)}. ` +
          `Current failure: streamlocal tunnel could not be opened for ${socketPath}. ` +
          "Check that the remote Docker-compatible socket exists, the SSH server allows streamlocal forwarding, and the SSH user can access that socket." +
          diagnostics,
      );
    }
  }

  try {
    await probeDockerSshBridge(opts);
  } catch (error) {
    const diagnostics = shouldCollectSocketDiagnostics(error)
      ? formatSocketDiagnostics(await collectDockerSocketDiagnostics(opts, socketPath))
      : "";

    throw new Error(
      `Cannot reach Docker daemon: ${safeErrorMessage(error)}. ` +
        `Preflight steps: SSH login -> resolve remote Docker socket path -> open streamlocal tunnel -> Docker API ping. ` +
        `Current failure: streamlocal tunnel could not be opened for ${socketPath}. ` +
        "Check that the remote Docker-compatible socket exists, the SSH server allows streamlocal forwarding, and the SSH user can access that socket." +
        diagnostics,
    );
  }
}

/** A loopback TCP listener that tunnels Docker API traffic to the remote socket. */
export interface DockerSshBridge {
  /** Bind the bridge and return the Docker endpoint dockerode should target. */
  start(): Promise<{ socketPath: string } | { host: string; port: number }>;
  /** Tear down the listener and any live connections. */
  close(): void;
}

/**
 * `ssh2` streamlocal forwarding is not reliable on every OpenSSH/Bun
 * combination: a channel can open but never move bytes. Use the operating
 * system's SSH implementation when its auth model can be represented without
 * weakening a caller-supplied host verifier. The ssh2 bridge remains the
 * fallback for password auth and custom verifier configurations.
 */
export function shouldUseNativeSshBridge(opts: DockerConnectionOptions): boolean {
  return Boolean(
    opts.host &&
      !opts.hostVerifier &&
      !opts.password &&
      (opts.privateKey || opts.sshAgent),
  );
}

export function buildNativeSshBridgeCommand(
  opts: DockerConnectionOptions,
  localSocketPath: string,
  remoteSocketPath: string,
  privateKeyPath?: string,
): { command: string; args: string[] } {
  const args = [
    "-N",
    "-o", "BatchMode=yes",
    "-o", "ExitOnForwardFailure=yes",
    // The existing ssh2 transport also accepts an unpinned host when callers
    // did not provide hostVerifier. Preserve that contract here; callers that
    // require pinning stay on the ssh2 path above.
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-p", String(opts.port ?? 22),
  ];
  if (privateKeyPath) args.push("-i", privateKeyPath);
  args.push(
    "-L", `${localSocketPath}:${remoteSocketPath}`,
    `${opts.username ?? "root"}@${opts.host}`,
  );
  return { command: "ssh", args };
}

function createNativeDockerSshBridge(opts: DockerConnectionOptions): DockerSshBridge {
  let child: ChildProcess | null = null;
  let bridgeDir: string | null = null;

  const cleanup = () => {
    child?.kill();
    child = null;
    if (bridgeDir) {
      void rm(bridgeDir, { recursive: true, force: true });
      bridgeDir = null;
    }
  };

  return {
    start: async () => {
      if (child && bridgeDir) return { socketPath: join(bridgeDir, "docker.sock") };

      bridgeDir = await mkdtemp(join(tmpdir(), "openship-docker-ssh-"));
      const socketPath = join(bridgeDir, "docker.sock");
      let keyPath: string | undefined;
      if (opts.privateKey) {
        keyPath = join(bridgeDir, "identity");
        await writeFile(keyPath, opts.privateKey, { mode: 0o600 });
        await chmod(keyPath, 0o600);
      }

      const { command, args } = buildNativeSshBridgeCommand(
        opts,
        socketPath,
        await resolveRemoteDockerSocketPath(opts),
        keyPath,
      );
      child = spawn(command, args, {
        stdio: ["ignore", "ignore", "pipe"],
        env: opts.sshAgent ? { ...process.env, SSH_AUTH_SOCK: opts.sshAgent } : process.env,
      });

      let stderr = "";
      child.stderr?.on("data", (chunk: Buffer) => {
        stderr += chunk.toString("utf8");
      });

      try {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error(`Native SSH Docker bridge timed out starting: ${stderr.trim() || "no diagnostic"}`)),
            10_000,
          );
          const poll = setInterval(() => {
            access(socketPath)
              .then(() => {
                clearTimeout(timer);
                clearInterval(poll);
                resolve();
              })
              .catch(() => {});
          }, 50);
          child!.once("error", (error) => {
            clearTimeout(timer);
            clearInterval(poll);
            reject(error);
          });
          child!.once("exit", (code) => {
            clearTimeout(timer);
            clearInterval(poll);
            reject(new Error(`Native SSH Docker bridge exited with code ${code}: ${stderr.trim() || "no diagnostic"}`));
          });
        });
      } catch (error) {
        cleanup();
        throw error;
      }

      return { socketPath };
    },
    close: cleanup,
  };
}

/**
 * Open a duplex stream to the remote Docker socket over SSH streamlocal
 * forwarding. Pooled path reuses the executor's persistent SSH connection
 * (channel multiplexing, no new TCP connection); ephemeral path opens a
 * fresh SSH connection whose lifetime is tied to the channel.
 */
async function openDockerUpstream(opts: DockerConnectionOptions): Promise<Duplex> {
  const socketPath = await resolveRemoteDockerSocketPath(opts);

  if (opts.executor?.forwardUnixSocket) {
    return opts.executor.forwardUnixSocket(socketPath);
  }

  const client: StreamLocalCapableClient = await connectSshClient(toSshConfig(opts));
  try {
    const channel = await openSshUnixSocket(client, socketPath);
    channel.once("close", () => client.end());
    channel.on("error", () => client.end());
    return channel;
  } catch (error) {
    client.end();
    throw error;
  }
}

/**
 * Build a Docker transport bridge for the SSH connection.
 *
 * dockerode talks plain HTTP to a loopback TCP port; each accepted
 * connection is piped to a fresh streamlocal channel to the remote Docker
 * socket. This is deliberately a real TCP listener rather than a custom
 * `http.Agent.createConnection` (the previous approach): Bun's HTTP client
 * ignores `Agent.createConnection` and dials the placeholder host instead,
 * which broke every SSH-transport Docker call under the Bun-hosted API. A
 * loopback bridge is honored identically by Node and Bun.
 */
export function createDockerSshBridge(opts: DockerConnectionOptions): DockerSshBridge {
  if (shouldUseNativeSshBridge(opts)) {
    return createNativeDockerSshBridge(opts);
  }

  const clients = new Set<net.Socket>();

  const server = net.createServer((client) => {
    clients.add(client);
    client.setNoDelay(true);
    client.once("close", () => clients.delete(client));

    openDockerUpstream(opts)
      .then((upstream) => {
        const teardown = () => {
          client.destroy();
          upstream.destroy();
        };
        client.on("error", teardown);
        upstream.on("error", teardown);
        client.once("close", () => upstream.destroy());
        upstream.once("close", () => client.destroy());
        client.pipe(upstream);
        upstream.pipe(client);
      })
      .catch((error) => {
        client.destroy(error instanceof Error ? error : new Error(String(error)));
      });
  });

  return {
    start: () =>
      new Promise((resolve, reject) => {
        const onError = (error: Error) => reject(error);
        server.once("error", onError);
        // Loopback only — never expose the remote Docker socket on the network.
        server.listen(0, "127.0.0.1", () => {
          server.removeListener("error", onError);
          const address = server.address();
          if (address === null || typeof address === "string") {
            reject(new Error("Docker SSH bridge failed to bind a loopback TCP port."));
            return;
          }
          resolve({ host: "127.0.0.1", port: address.port });
        });
      }),
    close: () => {
      for (const client of clients) {
        client.destroy();
      }
      clients.clear();
      server.close();
    },
  };
}
