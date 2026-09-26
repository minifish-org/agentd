# Automatic behavior learning

agentd can periodically propose and evaluate an instruction supplement for each
foreground agent in a tenant. An independent model judge compares the current
instructions with the proposal. A passing proposal becomes active automatically;
there is no human approval step.

This works with a standard OpenAI-compatible chat-completions API, including
DeepSeek. It optimizes instructions using AI feedback. It does not train model
weights, implement a policy-gradient algorithm, or collect human feedback for
RLHF. The measured result is the quality of the next assistant decision on
historical inputs, not end-to-end success on a newly executed task.

## Install and enable

Create the tenant and foreground agents first, then install the optional preset:

```sh
curl -X POST http://127.0.0.1:8080/v1/tenants/demo/presets/behavior-learning \
  -H 'content-type: application/json' \
  -d '{"target_agent":"*","judge_model":"deepseek-chat"}'
```

An empty request body uses defaults. Add your configured authorization header
when the server requires an API token. Installation creates:

- `system/behavior-learner`, with no tool families and no rolling context;
- `system/behavior-learning`, a disabled Sunday 04:00 schedule in
  `Asia/Singapore`, with no delivery destination.

The default target `*` expands when the schedule fires into separate runs for
the tenant's foreground agents. Agents named `system/...` or labelled
`agentd.system=true` are excluded. Each target receives a separate learning
scope. A newly registered foreground agent is included at the next trigger.

Enable the existing schedule through the normal schedule API:

```sh
curl -s http://127.0.0.1:8080/v1/tenants/demo/schedules/system%2Fbehavior-learning \
  | jq '.spec | .enabled = true' > /tmp/agentd-behavior-schedule.json
curl -X PUT http://127.0.0.1:8080/v1/tenants/demo/schedules/system%2Fbehavior-learning \
  -H 'content-type: application/json' \
  --data-binary @/tmp/agentd-behavior-schedule.json
```

Once enabled, sampling, proposing, judging, and promotion run automatically.
Reapplying the preset preserves an existing compatible schedule, including its
payload and enabled state. It repairs the learner's context window to zero,
preserves its other compatible settings, and rejects a reserved learner with
tool capabilities or a reserved schedule pointing to another agent with `409`.
Use schedule PUT to change installed options.

## Options and budgets

The preset's optional JSON body becomes the schedule payload. Unknown fields
and invalid values are rejected.

| Field | Default | Meaning |
| --- | --- | --- |
| `target_agent` | `"*"` | All foreground agents for a schedule, or one existing foreground agent |
| `proposer_model` | omitted | Target agent's model, then the host default |
| `judge_model` | omitted | Target agent's model, then the host default |
| `min_samples` | `8` | Minimum usable source runs before a cycle evaluates a proposal |
| `max_samples` | `12` | Maximum usable source runs sampled for a cycle |
| `max_model_calls` | `64` | Total model-call budget per target cycle |
| `max_tokens` | `2048` | Completion-token cap per model call |
| `min_improvement` | `0.1` | Minimum mean conservative gain on the judge's 0–1 score scale |
| `rubric` | omitted | Additional evaluation criteria alongside the fixed reliability rubric |

The preset's run timeout is 900,000 ms, and its `max_steps` is 64. The effective
model-call budget is the smaller of `max_model_calls` and the learner's
`max_steps`. One proposal plus four calls per held-out decision are required:
baseline, candidate, and two judge calls. A cycle that cannot fit every selected
decision within the budget skips promotion. Increasing the sample count may
therefore require increasing both budget settings. Actual provider cost also
depends on input size, model pricing, and token usage; `max_tokens` limits output
tokens rather than the complete request.

All models use the host's configured endpoint and credentials. An independent
judge means a separate request and context, not necessarily a different model
or provider. Set `judge_model` to a different model ID if the same endpoint
serves one. The baseline and candidate always use the target agent's model.

## What a cycle evaluates

The coordinator selects new usable terminal foreground runs after the last
consumed source cursor, examining up to four times `max_samples` recent source
runs to find eligible inputs. It uses text-only captured model requests bounded
to 32 KiB and excludes historical tool schemas that are no longer exposed by the
target. From each source run it takes at most two decision points: the earliest
tool-call decision and the latest final-answer decision. These include the
conversation prefix and tool schemas that were visible at that point.

This samples recent history rather than processing a complete training queue:
after an evaluated cycle, the cursor advances to the newest selected source,
and older unselected runs are no longer considered.

Source runs are partitioned by conversation scope into development and held-out
groups before the proposal call. All sampled runs from one scope stay in the
same group, because a later request can contain earlier turns from that scope.
The groups balance run counts without splitting scopes. A cycle needs at least
two distinct scopes among its selected usable runs as well as `min_samples`
total runs; otherwise it skips without calling a model. The proposer sees only
development examples, the current learned supplement, and the owner's
instructions. Development examples include bounded excerpts of both selected
decision points and their recorded outcomes. Its output is limited to a
4096-byte supplement.

For every held-out prefix, agentd independently asks the current and proposed
policies for one next assistant decision. The decision may be a final answer or
a set of native tool calls. Validation checks the assistant message envelope,
available tool names, unique call IDs, JSON-object arguments, and the runtime's
basic required-field and non-empty-string checks. It does not implement full
JSON Schema validation or prove that arguments are semantically correct.
**No proposed tool call is executed.**
Recorded tool observations are evidence already present in a frozen prefix;
agentd neither invents results for new calls nor replays them against production.

The judge sees the same task evidence and the two anonymized decisions in a
fresh context. It evaluates both A/B orders to reduce position bias. The fixed
rubric covers appropriate tool selection, valid arguments, use of available
evidence, and answer quality; an optional operator rubric adds criteria. Scores
must be structured values from 0 to 1 with supporting evidence.

Every held-out decision must be evaluated. Promotion requires no per-decision
regression using the worse score difference across the two judge orders and a
mean conservative improvement at least `min_improvement`. Invalid decisions,
malformed judge output, insufficient samples, or exhausted budgets cannot
produce a promoted supplement. A judge's favorable score is still an estimate
of quality, not proof of factual correctness or successful downstream actions.

## State and isolation

Learned instructions have their own tenant/agent-scoped policy and revision
tables. They are separate from owner-authored persona, ordinary conversation
context, business memory, and artifacts. Memory maintenance does not scan them.
The learner and judge receive only explicitly prepared request contexts and
have no production tools. Trial messages are not added to foreground context.

The active supplement is captured when a foreground run is claimed and used
alongside the owner's prompt. It does not alter the agent's allowed tools or
other configuration. Promotion checks that both the target specification and
the parent learned revision still match the cycle's starting snapshot. An
overlapping human edit or policy change makes the proposal stale.
Deleting and recreating an agent clears its active policy and sampling cursor;
the new agent cannot inherit the old agent's runs or in-flight proposals.

Successful promotion, its revision, and the coordinator's terminal result are
committed together. Failed, cancelled, timed-out, or interrupted cycles do not
activate a proposal. Restart marks in-flight runs failed; later scheduled runs
can attempt learning again. Earlier revisions remain available in history.

## Inspect, reset, or run once

`GET /v1/tenants/:tenant/learning/:agent` returns `tenant`, `agent`,
`active_revision` (or `null`), and at most 20 recent entries in `history`.
`DELETE` at the same path clears the active supplement and retains revision
history. It returns `cleared`; repeated clearing is safe. Both endpoints return
`404` for an unknown tenant or agent. Clearing does not disable the schedule, so
future successful cycles may activate a new supplement.

The learner's ordinary run and trace endpoints expose the selected source IDs,
development/held-out run and scope split, proposal, model requests/responses, comparisons,
budget use, and promotion outcome. Model events identify the `propose`,
`baseline`, `candidate`, or `judge` stage. Reports distinguish promoted,
rejected, stale, and skipped cycles. Normal provider errors remain failed runs.

For an immediate cycle, submit an ordinary turn to the installed learner with
one concrete target:

```sh
curl -X POST http://127.0.0.1:8080/v1/tenants/demo/turns \
  -H 'content-type: application/json' \
  -d '{
    "agent":"system/behavior-learner",
    "scope":"behavior-learning/simple-bot",
    "payload":{"target_agent":"simple-bot","judge_model":"deepseek-chat"}
  }'
```

`target_agent="*"` is schedule-only. A manual turn uses its own payload defaults,
not options copied from the installed schedule. Follow the returned run ID with
the normal wait or trace endpoint.

## Interpretation limits

This is offline next-decision evaluation. It can assess a tool choice and its
arguments against available evidence without producing side effects. It cannot
prove that the tool will succeed, that a different tool sequence reaches the
goal, or that a future conversation benefits from the supplement. Historical
examples from distinct scopes can still be correlated through similar tasks or
shared external information.

The proposer and judge may share model biases, and repeated AI judgments can
optimize for the judge's preferences. Held-out conversation scopes, swapped comparison
orders, deterministic decision checks, revision history, and conservative
promotion make the process inspectable; they do not establish end-to-end task
quality or remove prompt-injection risk. Live model-quality evidence must be
measured separately from deterministic tests of this protocol.
