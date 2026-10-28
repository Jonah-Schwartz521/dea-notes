# 07 - Monitoring and Security

Alerting via SNS and CloudWatch alarms, plus the two remaining IAM roles.

**Files:** `infra/monitoring.tf`, `infra/iam.tf`

---

## SNS

```hcl
resource "aws_sns_topic" "alerts" {
  name = "jobmarket-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = "..."
}
```

**SNS is pub/sub.** Publishers send to a **topic**, and every **subscriber**
gets a copy. The publisher knows nothing about who's listening.

That decoupling is why both the state machine's `sns:publish` states and the
CloudWatch alarms point at the same topic. Neither knows an email is on the
other end. Add Slack tomorrow and nothing upstream changes.

**Protocols:** `email`, `email-json`, `sms`, `https`, `sqs`, `lambda`,
`application` (mobile push), `firehose`.

**Email subscriptions require confirmation.** AWS sends a link, and until
someone clicks it the subscription sits in `PendingConfirmation` and silently
delivers nothing. Terraform reports success either way, which is a classic
"why am I not getting alerts" cause.

`aws_sns_topic_subscription` for email can't be fully managed by Terraform for
this reason. Worth confirming yours is actually `Confirmed` in the console.

**Note this topic hardcodes `jobmarket`** rather than using
`var.name_prefix`, same inconsistency as the Athena bucket had.

### SNS vs SQS vs EventBridge

Tested comparison, worth knowing cold:

| | SNS | SQS | EventBridge |
|---|---|---|---|
| Model | pub/sub, push | queue, pull | event bus, rules |
| Consumers | many, all get a copy | one per message | many, filtered by rule |
| Ordering | FIFO topics available | FIFO queues available | no |
| Retention | none, push and forget | up to 14 days | none |
| Filtering | message attributes | none | rich content-based patterns |
| Use | fanout, notifications | decoupling, buffering, retry | routing between services |

**The fanout pattern** is SNS to multiple SQS queues, giving you both
broadcast and durable buffering. Common exam answer.

Rough rule: **SNS pushes, SQS buffers, EventBridge routes.**

---

## Anatomy of a CloudWatch alarm

```hcl
resource "aws_cloudwatch_metric_alarm" "ingest_errors" {
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.ingest.function_name }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
```

**Namespace** groups metrics by service. `AWS/Lambda`, `AWS/States`,
`AWS/Glue`, `AWS/Athena`, `AWS/S3`. Custom metrics get your own namespace.

**Dimensions** are the filter. Without `FunctionName`, this would alarm on
errors from *every* Lambda in the account. Dimensions are what scope a metric
to one resource.

**Statistic** is the aggregation over the period: `Sum`, `Average`, `Minimum`,
`Maximum`, `SampleCount`, or a percentile like `p99`.

`Sum` is correct for error counts. `Average` would be nonsense here, and for
latency you'd want `p99` rather than `Average`, since averages hide tail
behavior.

**Period** is the aggregation window in seconds. 3600 means one data point per
hour.

**`evaluation_periods = 1`** means one bad data point triggers the alarm. Set
it to 3 and you'd need three consecutive breaching periods, which suppresses
transient spikes at the cost of slower detection.

There's also `datapoints_to_alarm` for M-out-of-N logic, like "3 of the last 5
periods."

**`threshold = 0` with `GreaterThanThreshold`** means any error at all fires.
Correct for a daily batch job where one failure matters. A high-traffic
service would need a rate-based threshold instead.

### The three alarm states

```
OK                 metric is within threshold
ALARM              threshold breached
INSUFFICIENT_DATA  not enough data points to judge
```

**`treat_missing_data` decides how gaps are handled**, and it's the setting
that matters most for batch workloads:

| Value | Behavior |
|---|---|
| `notBreaching` | missing data is treated as good |
| `breaching` | missing data is treated as a failure |
| `ignore` | keep the current state, don't evaluate |
| `missing` | go to INSUFFICIENT_DATA (the default) |

**`notBreaching` is right here** because the pipeline runs weekdays only.
Lambda publishes no `Errors` data point when it doesn't run, so weekends have
gaps. Without this the alarm flaps to INSUFFICIENT_DATA every Saturday and
you learn to ignore it.

**The tradeoff worth understanding:** `notBreaching` also means **a job that
never runs at all is silent.** If the scheduler breaks, or the Lambda is
deleted, there are no errors, so no alarm. That's the gap section below.

`breaching` on missing data would catch that, at the cost of weekend noise.

**`alarm_actions`** fire on transition into ALARM. There's also `ok_actions`
for recovery notifications and `insufficient_data_actions`.

---

## What's actually monitored

Two alarms across the whole pipeline:

| Alarm | Watches | Catches |
|---|---|---|
| `ingest_errors` | Lambda `Errors` | handler crashes |
| `daily_failed` (in `stepfunctions.tf`) | `ExecutionsFailed` | workflow failures |

Plus the SNS publishes inside the state machine, which alert on anticipated
conditions like "nothing ingested."

**Three layers, and they catch different things.** The ASL alerts on business
logic problems, the Step Functions alarm catches workflow failures including
ones that bypass the Catch blocks, and the Lambda alarm catches handler
errors directly.

Note the Lambda alarm is somewhat redundant now, since a Lambda error inside
the Map fails a child execution, which counts against the failure tolerance,
which may or may not fail the parent. Actually a useful distinction: the
Lambda alarm fires on **any** error, while the Step Functions alarm only fires
if enough errors breach the 50% tolerance. So a single dead board is visible
in one and not the other.

---

## Monitoring gaps

Honest list of what's not covered.

**Nothing watches the Glue jobs directly.** A Glue failure fails the `.sync`
state, which fails the workflow, which trips `daily_failed`. So it's covered
transitively, but there's no `AWS/Glue` alarm on job duration, DPU usage, or
`glue.driver.aggregate.numFailedTasks`.

**Nothing detects the pipeline not running.** The biggest gap. `notBreaching`
on both alarms means silence looks identical to success. If the scheduler is
disabled, everything stays green.

The standard fix is an alarm on `ExecutionsStarted` or `ExecutionsSucceeded`
with `LessThanThreshold 1`, evaluated over ~26 hours, with
`treat_missing_data = "breaching"`. That's a **dead man's switch**, and it's
worth adding.

**No data quality alarms.** The pipeline can succeed while writing zero
postings. `handler.py` logs `records` as structured JSON, so a metric filter
could turn that into a CloudWatch metric and alarm on it. That's the payoff
for structured logging that hasn't been collected yet.

**No cost alarms.** No AWS Budget, no Athena data usage control, no alarm on
`ProcessedBytes` trending up.

**No Athena alarms** despite `publish_cloudwatch_metrics_enabled = true` in
the workgroup. The metrics exist and nothing reads them.

---

## Metric filters, the missing piece

Not in this repo but directly relevant, since your logging was built for it.

```
Log line: {"company": "stripe", "records": 0, ...}
```

A **metric filter** on a log group extracts values from log events into a
CloudWatch metric:

```
{ $.records = 0 }
```

That becomes a countable metric you can alarm on. This is the bridge from
"logs" to "monitoring," and the reason `handler.py` emits JSON instead of
prose.

Related: **CloudWatch Logs Insights** for ad-hoc querying,
**Contributor Insights** for finding top-N patterns.

---

## The Lambda execution role

```hcl
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
```

**`AWSLambdaBasicExecutionRole` is the minimum any Lambda needs:**
`logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`.

Without it the function runs but writes nothing to CloudWatch, and debugging
becomes impossible.

Related managed policies worth knowing:
- `AWSLambdaVPCAccessExecutionRole` for VPC-attached functions (ENI
  management)
- `AWSLambdaSQSQueueExecutionRole` for SQS event sources
- `AWSLambdaDynamoDBExecutionRole` for DynamoDB Streams

```hcl
data "aws_iam_policy_document" "lambda_s3_put" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/raw/*"]
  }
}
```

**This is the tightest policy in the repo and it's a good model.**

One action. Not `s3:*`, not `GetObject`, not `DeleteObject`. The handler only
writes, so it can only write.

Scoped to `/raw/*`, not the whole bucket. It cannot touch anything outside
that prefix.

Object ARN with `/*` and no bare bucket ARN, correctly, because `PutObject` is
object-level and the handler never lists.

**Compare to the Glue normalize role**, which has `curated.arn/*` for the
whole bucket. That's the looser one flagged in `04-transform`. This is what
tight looks like.

---

## The scheduler role

```hcl
identifiers = ["scheduler.amazonaws.com"]
```

Note the principal: `scheduler.amazonaws.com`, not `events.amazonaws.com`.
**EventBridge Scheduler is a distinct service from EventBridge**, with its own
service principal. Using the wrong one produces a trust policy error that
looks like a permissions problem.

```hcl
# The scheduler now starts the state machine instead of invoking the Lambda
# directly. The Lambda is reached only through the IngestAll Map, using the
# state machine's own role.
data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.daily.arn]
  }
}
```

**One action, one resource.** The scheduler can start this specific state
machine and do nothing else. It cannot invoke the Lambda, cannot read S3,
cannot stop executions.

The comment documents the architecture change from `06-orchestration`. Under
the old design the scheduler needed `lambda:InvokeFunction`. Now it doesn't,
and that permission was correctly removed rather than left behind.

**Stale permissions accumulating after refactors is one of the most common
real-world IAM problems.** Removing this one was the right instinct.

---

## The permission chain

Worth tracing end to end, because it shows how role separation actually works:

```
Scheduler role      -> states:StartExecution on the state machine only
State machine role  -> lambda:InvokeFunction on the ingest function
                    -> glue:StartJobRun on three specific jobs
                    -> glue:StartCrawler on one crawler
                    -> sns:Publish on one topic
                    -> states:StartExecution on itself (Distributed Map)
Lambda role         -> s3:PutObject on raw/* only
Glue normalize role -> s3 read on raw, read/write on curated
Glue skills role    -> s3 read on postings, write on posting_skills only
Glue crawler role   -> s3 read on both buckets
```

**Six roles, each holding exactly what its component needs.** No role can do
another's job. A compromised or buggy scheduler cannot write to S3. A buggy
skills job cannot corrupt postings.

That's least privilege implemented rather than described, and it's the thing
the exam is actually testing when it asks about IAM design.

---

# Exam angles

**Domain 3, Operations and Support (22%)**
- **CloudWatch alarm anatomy:** namespace, dimensions, statistic, period,
  evaluation periods, threshold, comparison operator
- **`treat_missing_data` for intermittent workloads**, and the silence
  tradeoff
- Alarm states: OK, ALARM, INSUFFICIENT_DATA
- `datapoints_to_alarm` for M-of-N evaluation
- Metric filters turning log content into alarmable metrics
- Structured logging and CloudWatch Logs Insights
- **SNS vs SQS vs EventBridge**, and the SNS-to-SQS fanout pattern
- Email subscriptions requiring confirmation
- Dead man's switch alarms for detecting absence

**Domain 4, Security and Governance (18%)**
- Service principals: `lambda.` vs `states.` vs `scheduler.` vs `glue.`
- **EventBridge Scheduler is a separate principal from EventBridge**
- Single-action, single-resource policies as the least-privilege model
- `AWSLambdaBasicExecutionRole` and the VPC / SQS / DynamoDB variants
- Removing permissions after a refactor
- Role separation so no component can perform another's actions

---

# Decision rules

```
Fanout to many consumers          -> SNS
Durable buffering, one consumer   -> SQS
Both broadcast and durability     -> SNS -> multiple SQS queues
Content-based routing             -> EventBridge
Error counts                      -> Sum
Latency                           -> p99, not Average
Batch job, gaps expected          -> treat_missing_data = notBreaching
Detect a job that never ran       -> LessThanThreshold + breaching
Suppress transient spikes         -> evaluation_periods > 1, or M-of-N
Alarm on log contents             -> metric filter -> metric -> alarm
Lambda writes only                -> s3:PutObject, prefix-scoped, no bucket ARN
Scheduler starting a workflow     -> scheduler.amazonaws.com principal
After a refactor                  -> delete permissions the component no longer needs
```

---

# Gaps

- [ ] **Dead man's switch: alarm on ExecutionsStarted < 1 over ~26h with
      treat_missing_data = breaching**
- [ ] Metric filter on `records = 0` from the ingest log group
- [ ] Glue job alarms: duration, failed tasks, DPU consumption
- [ ] AWS Budget or Athena data usage control for cost alerting
- [ ] Nothing consumes the Athena workgroup metrics that are being published
- [ ] Confirm the SNS email subscription is actually Confirmed, not Pending
- [ ] SNS topic hardcodes "jobmarket" instead of var.name_prefix
- [ ] Email address is in plaintext in `iam.tf`, scrub before publishing
- [ ] Composite alarms for reducing alert noise
- [ ] CloudWatch dashboards for the pipeline