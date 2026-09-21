# Deploying your private Orbit dashboard on Railway

When you're done you'll have a private web address (like `orbit-watcher-production.up.railway.app`) with a login page. Behind it, the watcher runs around the clock. No coding is needed: every step happens in a web browser.

Use **personal accounts** for GitHub and Railway, not work ones.

**Cost:** Railway's Hobby plan is about $5 a month, and this small app normally fits inside it. Check Railway's pricing page for current numbers.

---

## 1. Put the code on GitHub (about 5 minutes)

1. Sign in at **github.com** (create a free account if needed).
2. Click **+ → New repository**. Name it `orbit-watcher`, choose **Private**, then click **Create repository**.
3. On the new repo page, click **uploading an existing file**.
4. Unzip `orbit-watcher-saas.zip` on your computer. Drag **all the files inside the folder** into the upload area: `server.py`, `watcher.py`, `dashboard.html`, `Dockerfile`, `railway.json`, the tests and the docs.
5. Click **Commit changes**.

## 2. Create the Railway service (about 5 minutes)

1. Sign in at **railway.com** with your GitHub account.
2. Click **New Project → Deploy from GitHub repo**, then choose `orbit-watcher`. If Railway asks for access to the repo, allow it for that repo only.
3. Railway sees the `Dockerfile` and starts building. The first deploy may **fail**, because the password isn't set yet. That's expected.

## 3. Set the password and optional RPC

Open the service, go to **Variables**, and add:

| Name | Value |
|---|---|
| `ADMIN_PASSWORD` | A long password you don't use anywhere else (at least 12 characters). This protects the dashboard. |
| `SOLANA_RPC_HTTP` | *(optional)* Your private RPC link, starting `https://`. |
| `SOLANA_RPC_WS` | *(optional)* The same RPC's WebSocket link, starting `wss://`. |

The public Solana RPC often limits cloud servers. A free Helius account gives you both links, and they're the ones to use if the dashboard keeps showing "Reconnecting".

## 4. Add storage so data survives restarts

1. In the project, add a **Volume** to the service. Depending on the Railway layout, that's the **+ New → Volume** button, or right-clicking the service and choosing **Attach volume**.
2. Set the **mount path** to `/data`.

Without the volume, your watches and recorded data are wiped every time Railway restarts the app.

## 5. Get your web address

Go to **Settings → Networking → Generate Domain**. Railway gives you a public `https://...up.railway.app` address. The dashboard itself stays private behind your password.

Railway redeploys automatically after these changes. When the deploy log shows `Orbit dashboard on port ...`, it's live.

## 6. Use it

1. Open your address and sign in.
2. Under **Add a token**, paste a token mint and click **Find pools**. You need a token with two or more Raydium AMM v4, Raydium CPMM or PumpSwap pools against SOL or USDC.
3. Click **Start watching**.
4. Leave it running. Check back over the next few days: the tiles, the gap and shock tables, and the **Report** fill in over time.
5. Download the CSV files whenever you like.

---

## If something goes wrong

| What you see | What to do |
|---|---|
| Deploy log: `Set ADMIN_PASSWORD to at least 12 characters` | Add or lengthen the `ADMIN_PASSWORD` variable. |
| Dashboard keeps showing **Reconnecting** in Activity | Add your own RPC variables (step 3). |
| "Find pools" returns an error | Usually a network hiccup or the RPC limiting requests. Try again, or add your own RPC. |
| Watches vanished after a restart | The volume isn't attached at `/data` (step 4). |
| Too many wrong password attempts | Wait 15 minutes. Five failures lock that address out temporarily. |

## Security notes

- The login uses a signed session cookie (HttpOnly, SameSite=Strict, and Secure over HTTPS). Sessions expire after 12 hours.
- Dashboard actions only work from the dashboard page itself, so other sites can't trigger them.
- There's no wallet, key or trading code anywhere in this app. A test checks for that.
- To change the password, edit `ADMIN_PASSWORD` in Railway. Existing sessions stay valid until they expire. To log everyone out immediately, delete the file `/data/.session_secret` (with the volume's file browser, or by redeploying with a fresh volume).

---

## Faster data: a free Helius RPC (recommended)

1. Sign up at **helius.dev** (free plan) and open **Endpoints**.
2. Copy the **HTTPS** URL (`https://mainnet.helius-rpc.com/?api-key=...`) and the **WebSocket** URL (`wss://mainnet.helius-rpc.com/?api-key=...`).
3. In Railway → orbit-watcher → **Variables**, add `SOLANA_RPC_HTTP` and `SOLANA_RPC_WS` with those two URLs, then click **Deploy**.
4. The header pill changes to **private RPC**. Compare the **Behind chain tip** tile before and after: lower is faster.

## Phone alerts with Telegram (optional)

1. In Telegram, message **@BotFather** → `/newbot` → pick any name. Copy the **token** it gives you.
2. Open your new bot and send it any message (for example "hi").
3. In a browser, open `https://api.telegram.org/bot<TOKEN>/getUpdates` (put your token in). Find `"chat":{"id":123456789` and copy that number.
4. In Railway **Variables**, add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`, then **Deploy**.
5. On the dashboard, click **Alerts → Send test**. You should get a message within seconds.

You get alerts for: gaps that pass the depth check, single-update moves of 3% or more, and one daily summary. Nothing ever trades.

## New-pool hunter

Click **New-pool hunter → Turn on**. Every 5 minutes it checks GeckoTerminal's newest Solana pools. When a new token already has 2+ usable pools above $10k, it is watched automatically for 30 minutes (max 3 at once), then removed. Watches it adds show a yellow "hunter" badge.
