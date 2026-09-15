import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  let observedCancelToken: string | undefined;
  const base = process.env.PWC_FIXTURE_URL!;
  const report = async (kind: string, data: unknown) => {
    const response = await fetch(`${base}/fixture/${kind}`, {
      method: "POST", body: JSON.stringify(data),
    });
    if (!response.ok) throw new Error(`fixture barrier failed: ${response.status}`);
  };

  pi.on("session_start", async (_event, ctx) => {
    await report("isolation", {
      agentDir: process.env.PI_CODING_AGENT_DIR,
      bridgeDir: process.env.PI_BRIDGE_DIR,
      home: process.env.HOME,
      autoName: process.env.PI_AUTO_NAME,
      credentialEnvAbsent: !Object.keys(process.env).some((key) => /API_KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|^AWS_|^AZURE_|^GOOGLE_/i.test(key)),
      sessionId: ctx.sessionManager.getSessionId(),
    });
  });

  pi.on("input", async (event, ctx) => {
    const match = event.text.match(/^Controller dispatch ([a-f0-9]{32})\ncase:([a-z-]+)/);
    if (!match) return;
    const [, token, action] = match;
    await report("input", { token, action });
    if (action === "handled") return { action: "handled" };
    if (action === "transform") return { action: "transform", text: "transformed without dispatch nonce" };
    if (action === "hold-input") await report("hold-input", { token });
    if (action === "select" || action === "confirm" || action === "input" || action === "editor") {
      if (action === "select") await ctx.ui.select("bundled select", ["deny", "allow"]);
      if (action === "confirm") await ctx.ui.confirm("bundled confirm", "test only");
      if (action === "input") await ctx.ui.input("bundled input", "test only");
      if (action === "editor") await ctx.ui.editor("bundled editor", "test only");
      return { action: "handled" };
    }
  });

  pi.on("message_start", async (event) => {
    if (event.message.role !== "user") return;
    const text = typeof event.message.content === "string" ? event.message.content :
      event.message.content.filter((part) => part.type === "text").map((part) => part.text).join("\n");
    const match = text.match(/^Controller dispatch ([a-f0-9]{32})\ncase:compact-cancel/);
    if (match) {
      observedCancelToken = match[1];
      await report("observed-user", { token: observedCancelToken });
    }
  });

  pi.on("session_before_compact", async (event) => {
    await report("before-compact", { reason: event.reason, willRetry: event.willRetry, observedCancelToken });
    // 派送前的 preflight compaction 不能冒充本次已觀察 user 的 cancellation。
    if (event.reason !== "manual" && observedCancelToken) {
      observedCancelToken = undefined;
      return { cancel: true };
    }
  });
}
