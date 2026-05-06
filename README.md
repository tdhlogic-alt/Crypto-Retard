# Crypto Bot Starter

A conservative Kotlin + Spring Boot starter for a Coinbase Advanced Trade API bot.

## What it does

- Runs on a schedule, default every 15 minutes.
- Reads Coinbase product data and USD balances.
- Applies a simple dip-buy rule.
- Defaults to `bot.enabled=false` and `bot.dry-run=true`.
- Places a market buy only when both `bot.enabled=true` and `bot.dry-run=false`.

## Safety defaults

Do not enable withdrawal/transfer permission on the Coinbase API key.
Use a key with `view` and, only when ready, `trade` permission.
Start with dry-run.

## Environment variables

```bash
export COINBASE_API_KEY_NAME='organizations/{org_id}/apiKeys/{key_id}'
export COINBASE_PRIVATE_KEY_PEM='-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n'
```

Coinbase App APIs require ECDSA / ES256 keys, not Ed25519.

## Run locally

```bash
./gradlew bootRun
```

## Make it actually evaluate strategy

In `src/main/resources/application.yml`:

```yaml
bot:
  enabled: true
  dry-run: true
```

## Enable live trades

Only after dry-run logs look correct:

```yaml
bot:
  enabled: true
  dry-run: false
```

## Deploy later

Good first deployment targets:

- Google Cloud Run Job + Cloud Scheduler
- AWS Lambda + EventBridge Scheduler
- A tiny VPS if you want a persistent process

For Cloud Run/Lambda, prefer a one-shot process instead of a Spring `@Scheduled` long-running service. This starter is intentionally written so the core strategy/client code can be moved into a one-shot command easily.
