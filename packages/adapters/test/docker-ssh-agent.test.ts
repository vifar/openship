import { describe, expect, test } from "vitest";

import {
  buildNativeSshBridgeCommand,
  shouldUseNativeSshBridge,
} from "../src/runtime/docker-ssh-agent";

describe("native Docker SSH bridge", () => {
  test("uses native SSH for private-key streamlocal forwarding without a host verifier", () => {
    expect(
      shouldUseNativeSshBridge({
        transport: "ssh",
        host: "deploy.example",
        username: "deploy",
        privateKey: "private-key-material",
      }),
    ).toBe(true);
  });

  test("keeps the ssh2 bridge when native SSH cannot preserve the configured verifier", () => {
    expect(
      shouldUseNativeSshBridge({
        transport: "ssh",
        host: "deploy.example",
        username: "deploy",
        privateKey: "private-key-material",
        hostVerifier: () => true,
      }),
    ).toBe(false);
  });

  test("builds a loopback-only Unix socket forward with a temporary private-key file", () => {
    expect(
      buildNativeSshBridgeCommand(
        {
          transport: "ssh",
          host: "deploy.example",
          port: 2222,
          username: "deploy",
          privateKey: "private-key-material",
        },
        "/tmp/openship-bridge/docker.sock",
        "/var/run/docker.sock",
        "/tmp/openship-bridge/key",
      ),
    ).toEqual({
      command: "ssh",
      args: [
        "-N",
        "-o", "BatchMode=yes",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-p", "2222",
        "-i", "/tmp/openship-bridge/key",
        "-L", "/tmp/openship-bridge/docker.sock:/var/run/docker.sock",
        "deploy@deploy.example",
      ],
    });
  });
});
