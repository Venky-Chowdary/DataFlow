# Pilot

Pilot is the assistant inside Datawrap. You ask it questions in plain language
and it answers by running tools against your workspace. It can also stage
actions for you to confirm.

## What Pilot can do

- **Answer questions about your workspace**: connectors, jobs, schedules,
  contracts, why a job failed, whether a destination matches its source.
- **Look at data through a saved connector**: list tables, read a schema,
  sample rows, run a read-only query, compute an aggregate.
- **Plan**: propose a transfer route, a mapping, a sync mode, or a fix for a
  Validate finding.
- **Stage actions for you to confirm**: start a transfer, create a connector
  or schedule, cancel, retry or resume a job, replay quarantined rows, and
  similar.
- **Explain the product**: how mapping, Validate, sync modes and connectors
  work.

The full tool list, with the permission each tool needs, is in
[mcp.md](mcp.md#tools). Pilot and the MCP server share one tool registry.

## What Pilot cannot do

- **It cannot act without your confirmation.** A tool that would change
  something stages the action and stops. Nothing happens until you press
  Confirm. An approval expires after 15 minutes and is used once.
- **It cannot do more than your role allows.** Each tool is checked against
  the same permission as the matching API route. A viewer can ask questions
  but cannot reach a tool that changes anything. Confirm checks the
  permission for that specific action again.
- **It does not decide a mapping.** The mapping engine decides which source
  column goes to which target. A model does not.
- **It does not decide whether a transfer is verified.** Verdicts come from
  the deterministic checks described in [verification.md](verification.md).
  Pilot can word a result; it cannot change one.
- **It cannot change data with a query.** Queries are read-only.
- **It cannot change workspace settings, members, roles or API keys**, sign a
  Trust Report, or approve a Trust Report fix. No tool exists for those.

## Answers come from tool results

Pilot works in a loop: read the question, choose tools, run them, and write
an answer from what the tools returned. A turn runs at most six rounds of
tool calls.

Each response carries:

| Field | Meaning |
|---|---|
| `answer` | The text shown to you. |
| `tools_used` | Which tools ran and whether each succeeded. |
| `sources` | Citations the tools retrieved. |
| `grounded` | True only when the answer rests on something that was read: a citation, or a successful tool call that read live state. Written product prose and refusals are not reported as grounded. |
| `pending_actions` | Actions staged and waiting for your confirmation. |
| `needs_clarification` | A question back to you, when the request was ambiguous. |

How to use this:

- Treat an answer with `grounded: false` as general guidance, not as a fact
  about your workspace.
- For a number that matters, look at the tool result or open the job, table
  or report it came from.
- Pilot can be wrong in how it words a correct tool result. The tool result
  is the evidence.

## Use of AI models

Pilot runs in one of three engine settings. The choice is made in this order:
the `DATAWRAP_PILOT_ENGINE` setting, then the preference saved in Settings,
then automatic.

| Engine | What runs where |
|---|---|
| `local` | A built-in planner chooses and runs the tools and writes the answer. No model provider is called. This is the automatic choice when no usable provider key is saved. |
| `hybrid` | Tools, gates and facts still run inside your deployment. The saved provider only words the answer. This is the automatic choice once a provider key is saved. |
| `cloud` | A provider model drives the tool loop and writes the answer. |

The code supports OpenAI and Anthropic as providers, and a local Ollama
endpoint.

With `hybrid` or `cloud`, the question, the conversation and the tool results
needed for the answer are sent to the provider. When you ask Pilot to sample
rows or run a query, the rows it reads are tool results and can be part of
what is sent. If that is not acceptable, pin the engine to `local`.

Related controls. Note what each one covers:

| Control | Default | What it covers |
|---|---|---|
| `DATAWRAP_PILOT_ENGINE=local` | unset | Keeps Pilot from calling any provider. |
| `DATAWRAP_AI_METADATA_ONLY` | on | The **mapping assistant** sends column names, declared types and aggregate profiles, not cell values. Each outbound mapping request is recorded with a hash of what was sent, and cloud providers are flagged as having left your boundary. |
| `DATAWRAP_PII_MASKING` | on | Sample values that look like personal data are masked in **mapping** prompts. |
| `DATAWRAP_LLM_ENABLED` | on | Switches off the model for **mapping suggestions** and for **Trust Report explanations**. It was not verified to stop Pilot; use the engine setting for that. |

Whenever the engine is `hybrid` or `cloud`, Pilot's answers are worded or
produced by an AI model. They can contain mistakes. The previous section
tells you how to check them.

## Access and limits

- Talking to Pilot needs the `ai.use` permission, which every role holds.
- Chat: `POST /api/v1/copilot/chat`. Confirm a staged action:
  `POST /api/v1/copilot/confirm`. Status and available models:
  `GET /api/v1/copilot/status` and `/models`.
- Training Pilot's shared knowledge (`POST /api/v1/copilot/train`) and adding
  documents to the shared search corpus (`POST /api/v1/ai/rag/ingest`) need
  `workspace.manage`, because the knowledge is shared by every workspace in
  the deployment.
- Requests are rate limited per caller (`DATAFLOW_PILOT_RATE_*`).
- Pilot keeps conversation memory and stored query results in files on the
  API host.

## Not covered

- How the web app labels AI-generated text. This page describes the API
  fields; the on-screen wording was not reviewed.
- Answer quality. No accuracy figures are stated here.
- Which model versions are used by default, and provider data-retention
  terms. Those are set by your provider account.
- Exactly which tool results are included in a cloud prompt for each kind of
  question.

## Verified against

- `apps/api/src/ai/copilot/pilot_agent.py` (tool loop, `carries_evidence`)
- `apps/api/src/ai/copilot/agent.py` (`CopilotResponse` fields)
- `apps/api/src/ai/copilot/tool_permissions.py`
- `apps/api/src/ai/copilot/confirm_ack.py`, `apps/api/src/ai/copilot/ack_ledger.py`
- `apps/api/src/ai/llm/provider.py` (engine selection, providers)
- `apps/api/services/llm_policy.py`, `apps/api/services/ai_egress.py`
- `apps/api/src/routers/copilot_router.py`
- `apps/api/services/rbac.py`
