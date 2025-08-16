import express from "express";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

const { GITHUB_TOKEN, OWNER, REPO, WORKFLOW_FILENAME = "automation.yml", REF = "main" } = process.env;
if (!GITHUB_TOKEN || !OWNER || !REPO) { console.error("Missing env: GITHUB_TOKEN, OWNER, REPO"); process.exit(1); }

let lastFire = 0;
const COOLDOWN_MS = 5000;

app.get("/", (_req, res) => res.status(200).send("OK"));

app.post("/drive-webhook", async (req, res) => {
  try {
    const now = Date.now();
    if (now - lastFire < COOLDOWN_MS) return res.sendStatus(200);
    lastFire = now;

    const state = req.header("X-Goog-Resource-State") || "unknown";
    if (!["update", "change", "add", "modify"].includes(state)) return res.sendStatus(200);

    const url = `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW_FILENAME}/dispatches`;
    const r = await fetch(url, {
      method: "POST",
      headers: { "Authorization": `Bearer ${GITHUB_TOKEN}`, "Accept": "application/vnd.github+json" },
      body: JSON.stringify({ ref: REF })
    });

    if (!r.ok) { const t = await r.text(); console.error("workflow_dispatch failed:", r.status, t); }
    res.sendStatus(200);
  } catch (e) {
    console.error("webhook error:", e?.message || e);
    res.sendStatus(200);
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`Webhook listening on :${port}`));
