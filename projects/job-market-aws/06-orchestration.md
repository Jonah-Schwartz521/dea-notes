# 06 - Orchestration

Step Functions state machine coordinating the pipeline, plus the
EventBridge Scheduler that starts it.

**Files:** `infra/stepfunctions.tf`, `infra/statemachine.asl.json`,
`infra/scheduler.tf`

```
Scheduler (6:34 UTC weekdays)
  -> DeriveIngestDate
  -> IngestAll (Distributed Map, 43 boards, concurrency 3)
  -> AnySuccess?
       no  -> AlertNoIngest -> Fail
       yes -> NormalizeAll (Parallel: Greenhouse | Lever)
              -> ExtractSkills
              -> CrawlCurated
              -> Done
```

---

## What Step Functions is

**A managed workflow engine.** States defined in JSON (Amazon States
Language), executed in order, with built-in retries, branching, parallelism,
and a full record of every transition.

The alternative is a Lambda calling other Lambdas and tracking its own
progress. Works until something fails halfway and you have no idea what
already ran.

| | Standard | Express |
|---|---|---|
| Max duration | 1 year | 5 minutes |
| Execution model | exactly-once | at-least-once |
| Billing | per state transition | per request + duration |
| History | full, 90 days, in console | CloudWatch Logs only |
| Use | long ETL, approvals, anything durable | high-volume event processing |

`STANDARD` is right here: runs for minutes, needs exactly-once so a Glue job
isn't started twice, and the visual execution history matters when something
breaks at 6am. Express would fit thousands of IoT events per second.

---

## Config resolved at plan time

```hcl
active_companies = [
  for c in local.companies_file.companies : {
    ats         = c.ats
    company     = c.board_token
    board_token = c.board_token
  } if c.active && contains(["greenhouse", "lever"], c.ats)
]
```

The filter predicted back in `03-ingest`. Lives here rather than in
`scheduler.tf` because the list is now passed as one array into the state
machine, not turned into one schedule per company.

**`company = c.board_token` is load-bearing:**

> the existing S3 partitions are company=komodohealth / company=spotify and
> renaming them would orphan every snapshot already written.

Answers the open question from `03-ingest`. Partition values are lowercase
slugs, so no spaces and no Hive parsing problems. And it's now locked in by
history: normalize globs on `company=`, so changing it would strand every
existing partition.

**Partition keys are effectively permanent once you have data.**

### Why Ashby is excluded, quantitatively

> 19 guaranteed failures out of 44 would sit just under
> ToleratedFailurePercentage 50, leaving three real failures between a normal
> morning and a dead pipeline.

Sharp. The Map tolerates 50% failures. Including Ashby means 19/44 ≈ 43%
failing every day by design, so 3 more tips the pipeline over.

**Known-bad inputs eat your failure budget.** Filter them out or your
alerting threshold becomes meaningless.

---

## Breaking a dependency cycle

```hcl
state_machine_name = "${var.name_prefix}-daily"
state_machine_arn  = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.state_machine_name}"
```

> Using the resource attribute would be a cycle: role -> policy -> state
> machine -> role.

The Distributed Map means the state machine needs permission to start
executions **of itself**. So the policy needs its ARN, but the state machine
needs the role, which needs the policy.

**Fix: construct the ARN by hand.** ARN format is deterministic, so you can
build it from name, region, and account without the resource existing.

Standard escape hatch for self-referential IAM. Also answers the "is
`aws_region` dead code?" gap from `04-transform`. It's used here.

---

## Vended logs

```hcl
name = "/aws/vendedlogs/states/${local.state_machine_name}"
```

**"Vended logs" are logs a service delivers on your behalf**, as opposed to
logs your own code writes. Step Functions and API Gateway both use this, and
it requires the `/aws/vendedlogs/` prefix. Wrong prefix means log delivery
silently fails.

```hcl
logging_configuration {
  log_destination        = "${aws_cloudwatch_log_group.states.arn}:*"
  include_execution_data = true
  level                  = "ALL"
}
```

Levels are `ALL`, `ERROR`, `FATAL`, `OFF`. `include_execution_data` logs the
actual input and output payloads of every state, which is what makes
debugging tractable. **Note that means payloads land in CloudWatch, so turn
it off for sensitive data.**

---

## The .sync integration pattern

One of the most important Step Functions concepts, and definitely tested.

| Pattern | ASL suffix | Behavior |
|---|---|---|
| **Request Response** | none | fire and forget, next state immediately |
| **Run a Job** | `.sync` | start it, **wait for completion**, then continue |
| **Wait for Callback** | `.waitForTaskToken` | pause until `SendTaskSuccess` is called |

```json
"Resource": "arn:aws:states:::glue:startJobRun.sync"
```

Without `.sync`, `StartJobRun` returns instantly and Step Functions moves on
while Glue is still running. Your crawler would fire before normalize
finished writing.

### The managed EventBridge rule

```hcl
statement {
  sid    = "ManagedSyncRules"
  actions = ["events:PutRule", "events:PutTargets", "events:DescribeRule"]
  resources = [".../rule/StepFunctionsGetEventsForGlueJobRule", ...]
}
```

> The `.sync` integrations do not poll. Step Functions creates a managed
> EventBridge rule per integration type and waits to be called back.

**Genuinely non-obvious, and the thing that breaks people's first `.sync`
state machine.** You'd assume polling. Instead it registers an EventBridge
rule that catches the Glue completion event and routes it back.

Miss `events:PutRule` and you get an AccessDenied mentioning EventBridge from
a state machine you never configured to touch EventBridge.

Rule names are AWS-managed and fixed, which is why they're scoped by ARN
rather than wildcarded. One per integration type, shared account-wide.

The `.sync` polling also explains the extra Glue permissions:

> GetJobRun is what the .sync integration polls; BatchStopJobRun is how it
> cancels the Glue run if the execution is stopped.

---

## Distributed Map

```json
"IngestAll": {
  "Type": "Map",
  "ItemsPath": "$.companies",
  "MaxConcurrency": 3,
  "ToleratedFailurePercentage": 50,
  "ItemProcessor": {
    "ProcessorConfig": { "Mode": "DISTRIBUTED", "ExecutionType": "STANDARD" }
  }
}
```

**Two Map modes, and the difference is heavily tested:**

| | Inline Map | Distributed Map |
|---|---|---|
| Max concurrency | 40 | 10,000 |
| Items | from state input only | input, or S3 objects / CSV / JSON directly |
| Execution | within the parent | **child execution per item** |
| Failure tolerance | none, one failure kills it | `ToleratedFailureCount` / `Percentage` |
| History | counts against parent's 25k limit | separate per child |

> ToleratedFailurePercentage is not available on an inline Map.

That's the whole reason for DISTRIBUTED here. You need one dead board not to
kill the run, and only Distributed Map offers a failure budget.

**The child-execution model is why the IAM looks strange:**

```hcl
statement {
  sid       = "DistributedMapChildren"
  actions   = ["states:StartExecution"]
  resources = [local.state_machine_arn]      # itself
}
statement {
  sid       = "DistributedMapSupervision"
  actions   = ["states:DescribeExecution", "states:StopExecution"]
  resources = [".../execution:${local.state_machine_name}/*"]
}
```

Each item runs as a **separate child execution of the same state machine**.
The parent starts them, watches them, and can stop them. Note the two
different ARN formats: `stateMachine:` for starting, `execution:` for
supervising.

**`MaxConcurrency: 3`** is politeness to the ATS hosts, not a technical
limit. 43 simultaneous requests to Greenhouse would look like an attack.
Rate limiting your own scraper is basic etiquette and keeps you unblocked.

**Failures are deliberately not caught per item:**

> a failed board fails its child execution, stays visible in the Map Run, and
> is counted against the tolerance. Only successful items appear in the
> result array.

Catching per item would hide failures and let them look like successes. This
way a dead board is visible in the console, counted against the budget, and
absent from the results array, which is exactly what the next state checks.

---

## Data flow: Parameters, ItemSelector, ResultPath

The hardest part of ASL, and where most bugs live.

```json
"DeriveIngestDate": {
  "Type": "Pass",
  "Parameters": {
    "scheduled_time.$": "$.scheduled_time",
    "ingest_date.$": "States.ArrayGetItem(States.StringSplit($.ingest_date, 'T'), 0)",
    "companies.$": "$.companies"
  }
}
```

**The `.$` suffix means "this value is a JSONPath expression, evaluate it"**
rather than a literal string. `"foo": "$.bar"` passes the literal string
`$.bar`. `"foo.$": "$.bar"` passes the value at `$.bar`. Missing that suffix
is the single most common ASL mistake.

`States.StringSplit` and `States.ArrayGetItem` are **intrinsic functions**,
built-in ASL operations that avoid needing a Lambda for trivial data
manipulation. Others: `States.Format`, `States.JsonToString`,
`States.ArrayLength`, `States.MathAdd`, `States.UUID`.

> The scheduler context attribute is a full timestamp. The Lambda needs it
> whole for the S3 key; the Glue jobs need just the date. Split once here so
> both downstream shapes are fixed.

One transformation at the top rather than repeated JSONPath surgery in every
downstream state.

### ResultPath, and why null appears everywhere

```json
"ResultPath": "$.ingest_results"   // put the result HERE, keep everything else
"ResultPath": null                 // discard the result, keep input unchanged
// omitted (default "$")           // result REPLACES the entire state
```

> ResultPath is null because AlertFailure still needs `$.ingest_date` and the
> Glue run result would otherwise replace the whole state.

**This is the bug that bites everyone.** Default behavior replaces your
entire state object with the task's output. So after `ExtractSkills`, `$`
would be the Glue job run details, and `$.ingest_date` would be gone. Then
`AlertFailure` tries to format it into a message and fails, meaning your
error handler errors while handling an error.

`ResultPath: null` keeps the input flowing through untouched.

`ItemSelector` reshapes what each Map item receives:

```json
"ItemSelector": {
  "ats.$": "$$.Map.Item.Value.ats",
  "scheduled_time.$": "$.scheduled_time"
}
```

**`$$` is the context object**, runtime metadata rather than your data.
`$$.Map.Item.Value` is the current item, `$$.Execution.Name` is the execution
ID, `$$.State.EnteredTime` is the timestamp. Used here to combine per-item
data with parent-level state.

---

## Retry vs Catch

Two distinct mechanisms, both present here.

```json
"Retry": [{
  "ErrorEquals": ["Lambda.ServiceException", "Lambda.AWSLambdaException",
                  "Lambda.SdkClientException", "Lambda.TooManyRequestsException"],
  "IntervalSeconds": 5,
  "MaxAttempts": 2,
  "BackoffRate": 2
}]
```

**Retry re-runs the same state. Catch routes to a different state.** Retry
happens first; if all attempts are exhausted, Catch fires.

`BackoffRate: 2` gives exponential backoff: 5s, then 10s.

> Retry only Lambda-side transients. A handler error (bad board token,
> unknown ats, non-200) is deterministic, retrying it just burns invocations.

**This is the right instinct and it's tested.** Retry transient
infrastructure failures. Don't retry deterministic logic failures. A 404 from
a wrong board token will be a 404 every time.

This also connects to `max_retries = 0` on the Glue jobs: retry logic lives
in exactly one layer, here.

```json
"Catch": [{
  "ErrorEquals": ["States.ALL"],
  "ResultPath": "$.error",
  "Next": "AlertFailure"
}]
```

`States.ALL` catches everything. Other built-ins: `States.TaskFailed`,
`States.Timeout`, `States.Permissions`, `States.DataLimitExceeded`.

`ResultPath: "$.error"` **adds** the error to the state under `$.error`
rather than replacing it, which is what lets `AlertFailure` access both
`$.error.Cause` and `$.ingest_date`.

### The selective catch

```json
"CrawlCurated": {
  "Catch": [
    {
      "ErrorEquals": ["Glue.CrawlerRunningException"],
      "Comment": "Already running from a previous execution. It will pick up the new partition anyway - not worth an alert.",
      "Next": "Done"
    },
    {
      "ErrorEquals": ["States.ALL"],
      "Next": "AlertFailure"
    }
  ]
}
```

**Catch blocks evaluate in order**, so the specific case is listed first.
A crawler already running isn't a failure, it's a benign race, so it routes
to success. Everything else alerts.

This is what separates an alerting system people trust from one they mute.
Every known-benign condition you don't special-case is a false alarm, and
enough false alarms means real ones get ignored.

---

## Choice: missing is not empty, again

```json
"AnySuccess": {
  "Type": "Choice",
  "Choices": [{ "Variable": "$.ingest_results[0]", "IsPresent": true,
                "Next": "NormalizeAll" }],
  "Default": "AlertNoIngest"
}
```

> An empty result array means nothing ingested. Normalizing then would read a
> raw partition that does not exist, and downstream must never infer a
> delisting from an absent snapshot.

**Same principle as `discover_snapshots()` raising in the normalize scripts
and `require_postings_partition()` in extract_skills.** Three independent
layers all refusing to treat missing data as empty data.

`IsPresent: true` on `[0]` is the idiom for "array is non-empty."

Note the failure path alerts **then** fails:

```json
"AlertNoIngest" -> "NoIngestSucceeded" (Type: Fail)
```

The `Fail` state matters: it marks the execution as failed, which is what
`ExecutionsFailed` in your CloudWatch alarm counts. Alerting and then
succeeding would make the metric lie.

---

## Parallel

```json
"NormalizeAll": {
  "Type": "Parallel",
  "Branches": [ { Greenhouse }, { Lever } ]
}
```

**Parallel vs Map:**

| | Parallel | Map |
|---|---|---|
| Branches | fixed, defined in the ASL | dynamic, one per input item |
| Use | different work concurrently | same work over a collection |

Two different Glue jobs, so Parallel. Both write `postings`, and they can run
concurrently **only because their partition keys are disjoint** by `company`,
which is the `partitionBy("ingest_date", "company")` decision from
`04-transform` paying off.

A Parallel state fails if any branch fails, and outputs an array of branch
results in order.

---

## No .sync for the crawler

```json
"CrawlCurated": {
  "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler"
}
```

> startCrawler is fire-and-forget; there is no .sync integration for crawlers.

**`arn:aws:states:::aws-sdk:` is the AWS SDK integration**, which exposes
over 200 AWS services and 9,000+ API actions directly from Step Functions.
Anything without a purpose-built optimized integration goes through this.

The tradeoff: SDK integrations are always Request Response. No `.sync`. So
`CrawlCurated` returns immediately and `Done` fires while the crawler is
still running. Fine here since nothing downstream waits on the catalog, but
worth knowing the execution "succeeds" before the work finishes.

If you needed to wait, you'd poll with a Wait + Choice loop calling
`GetCrawler`.

---

## The scheduler

```hcl
resource "aws_scheduler_schedule" "daily" {
  name = local.state_machine_name

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  flexible_time_window { mode = "OFF" }
```

**EventBridge Scheduler vs EventBridge Rules**, the comparison flagged back
in `01-foundation`:

| | EventBridge Rules | EventBridge Scheduler |
|---|---|---|
| Purpose | event routing + schedules | schedules only |
| Targets | ~20 services | 270+ services, 6,000+ API actions |
| Timezone | UTC only | any IANA timezone, DST-aware |
| One-time | no | yes (`at()` expressions) |
| Scale | ~300 rules per bus | 1M+ schedules |
| Flexible window | no | yes |
| Retries / DLQ | limited | built-in |

**Scheduler is the modern answer for anything schedule-based.** Rules are for
event-driven routing.

`schedule_expression_timezone` is set explicitly to UTC. Could have been
`America/Denver` with automatic DST handling, which is the main reason to
choose Scheduler. UTC is the right call for a batch pipeline, since you want
a stable interval, not a stable wall-clock time.

`flexible_time_window { mode = "OFF" }` means fire exactly at the scheduled
time. Setting `FLEXIBLE` with a window lets AWS spread invocations to smooth
load, useful at scale, unnecessary for one daily run.

```hcl
retry_policy { maximum_retry_attempts = 2 }
```

A **fourth** retry layer, above Step Functions' Retry blocks. This one covers
failure to *start* the execution at all, which is distinct from failures
inside it.

---

## The literal token, and why jsonencode would break it

```hcl
input = <<-JSON
  {"scheduled_time":"<aws.scheduler.scheduled-time>","ingest_date":"<aws.scheduler.scheduled-time>","companies":${jsonencode(local.active_companies)}}
JSON
```

> It must reach the target LITERALLY so the S3 key comes from the schedule's
> execution time rather than wall clock, which is what makes a retry overwrite
> itself. A heredoc keeps it verbatim; jsonencode() on the whole object would
> \u-escape the angle brackets and break the substitution.

**Two things happening here.**

**Context attribute substitution.** `<aws.scheduler.scheduled-time>` is
replaced by Scheduler at invocation time with the schedule's intended
execution time. Others: `<aws.scheduler.schedule-arn>`,
`<aws.scheduler.execution-id>`, `<aws.scheduler.attempt-number>`.

**Why scheduled time beats wall clock.** If the 6:34 invocation fails and
retries at 6:39, the scheduled time is still 6:34. So the retry writes to the
same S3 key and **overwrites itself** rather than creating a duplicate. That's
the idempotency fix for the problem flagged in `03-ingest`.

**Why the heredoc.** `jsonencode()` on the whole object would escape `<` and
`>` as `\u003c` and `\u003e`, and Scheduler's substitution looks for literal
angle brackets. The token would arrive unsubstituted. So the outer JSON is
raw text and only the safe inner array gets `jsonencode`.

That's a real, non-obvious footgun, correctly identified.

**Note this changes the bug analysis from `03-ingest`.** The handler's
`stamp = scheduled_time.replace("-", "").replace(":", "")` receives whatever
format Scheduler emits. Whether it includes microseconds determines whether
that string-replace produces a clean filename. Still worth verifying against
a real S3 listing.

---

## The architecture change this file documents

> The per-company schedules (jobmarket-ingest-komodohealth,
> jobmarket-ingest-spotify) are gone: fan-out is now the Step Functions Map.

**Before:** 13 EventBridge schedules, each invoking Lambda directly. No
coordination, no way to know when all ingestion finished, no way to trigger
normalize afterward.

**After:** one schedule, one state machine, fan-out inside the workflow.

**The pattern: schedule the workflow, not the work.** One entry point, and
concurrency, ordering, failure tolerance, and downstream dependencies all
become properties of the workflow rather than things you hope work out.

Also note this supersedes the `for_each` prediction from `03-ingest`. The
company list is now a runtime array in the payload rather than plan-time
resource generation. Adding a company is still a YAML edit and an apply, but
what changes is the scheduler's `input` string, not the resource count.

---

## The alarm

```hcl
resource "aws_cloudwatch_metric_alarm" "daily_failed" {
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.daily.arn }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

Belt and braces on top of the SNS publishes inside the state machine. The ASL
alerts on errors it anticipated; this alarm catches failures that bypass the
Catch blocks entirely, like the state machine failing to start or an IAM
error before any state runs.

**`treat_missing_data = "notBreaching"` is the important setting.** Weekends
have no executions, so no data points. Without this the alarm would go to
INSUFFICIENT_DATA every Saturday. Options are `notBreaching`, `breaching`,
`ignore`, and `missing`.

More on this in `07-monitoring.md`.

---

# Exam angles

**Domain 1, Ingestion and Transformation (34%)**
- Step Functions as the orchestrator for multi-step ETL
- **`.sync` vs Request Response vs `.waitForTaskToken`**
- **Distributed Map vs Inline Map**, and the concurrency and tolerance limits
- Parallel vs Map
- AWS SDK integrations for services without optimized ones

**Domain 3, Operations and Support (22%)**
- **Standard vs Express**, duration, semantics, billing
- Retry vs Catch, and retrying only transient errors
- Retry ownership in exactly one layer
- `ResultPath` semantics and payload shaping
- **EventBridge Scheduler vs EventBridge Rules**
- Scheduler context attributes and idempotent retries
- `treat_missing_data` on alarms for intermittent workloads
- Failure tolerance budgets, and not spending them on known-bad inputs

**Domain 4, Security (18%)**
- Per-service scoped IAM in one execution role
- `.sync` requiring EventBridge write permissions
- Distributed Map requiring self-invoke permissions
- Constructing ARNs manually to break IAM dependency cycles
- Execution data in logs as a sensitive-data consideration

---

# Decision rules

```
Multi-step workflow, needs coordination  -> Step Functions
Long-running / exactly-once              -> Standard
High-volume events, under 5 min          -> Express
Wait for a job to finish                 -> .sync
External system must call back           -> .waitForTaskToken
Fan out over a list, need failure budget -> Distributed Map
Fan out under 40 items, all must succeed -> Inline Map
Different work concurrently              -> Parallel
Service has no optimized integration     -> aws-sdk: integration
Retry transient infra errors             -> Retry with BackoffRate
Route deterministic failures elsewhere   -> Catch
Keep input while adding a result         -> ResultPath: "$.field"
Discard a result entirely                -> ResultPath: null
Anything schedule-based                  -> EventBridge Scheduler
Event-driven routing                     -> EventBridge Rules
Timezone-aware / DST-aware schedule      -> Scheduler
Idempotent retries                       -> scheduled-time, not wall clock
Alarm on an intermittent workload        -> treat_missing_data = notBreaching
Self-referential IAM                     -> construct the ARN manually
```

---

# Gaps

- [ ] `.waitForTaskToken` in practice, what actually calls SendTaskSuccess
- [ ] Step Functions pricing: state transitions, and how Distributed Map
      child executions are counted
- [ ] Polling a crawler to completion with Wait + Choice + GetCrawler
- [ ] Express workflows and their CloudWatch-only observability
- [ ] Verify the scheduled-time format against a real S3 key listing
- [ ] EventBridge Scheduler DLQ configuration
- [ ] The 25,000 event history limit and how Distributed Map avoids it