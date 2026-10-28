# 05 - Query

Athena workgroup and its results bucket. The SQL layer over the curated
Parquet.

**Files:** `infra/athena.tf`

---

## What Athena is

**Serverless SQL over S3.** No cluster, no provisioning, no scaling. You
write SQL, it reads files directly from S3 and returns rows.

It uses the Glue Data Catalog for table definitions, so everything the
crawler registered in `04-transform` is queryable with no further setup.

Under the hood it's Presto/Trino. Athena engine v3 is the current version.

**The billing model is the entire story: $5.00 per TB of data scanned.**
No charge for the query itself, for idle time, or for DDL. You pay for
bytes read from S3.

That single fact drives every design decision in this file and most of the
optimization advice on the exam:

| Technique | Why it saves money |
|---|---|
| Partitioning | skip whole prefixes, never read them |
| Columnar (Parquet) | read only the columns named in SELECT |
| Compression | fewer bytes on disk means fewer bytes scanned |
| Predicate pushdown | row-group stats let readers skip blocks |
| `SELECT *` avoidance | reads every column, defeats columnar |

**Minimum 10 MB per query is billed**, so thousands of tiny queries aren't
free. Failed queries are not charged. Cancelled queries are charged for
data scanned up to cancellation.

---

## The results bucket

```hcl
resource "aws_s3_bucket" "athena_results" {
  bucket = "jobmarket-athena-results-${data.aws_caller_identity.current.account_id}"
}
```

**Athena requires an S3 location to write query results.** Every query,
including `SELECT 1`, writes a CSV of its results plus a `.metadata` file.
This isn't optional and there's no "return results only" mode.

A third bucket, and correctly so:

```
raw/             source fidelity        never deleted
curated/         query-ready Parquet    rewritten daily
athena-results/  query output           disposable
```

Different lifecycle, different retention, different sensitivity. Results
are the only zone where "delete everything after a week" is the right
policy.

**Note this one hardcodes `jobmarket`** instead of using `var.name_prefix`
like every other bucket in the repo. Inconsistent. A parallel stack with a
different prefix would collide on this name. Small bug, worth fixing.

---

## Why results need their own lifecycle rule

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration { days = 7 }
  }
}
```

**This is the first lifecycle rule in the whole repo**, and it's here
because results are the fastest-growing garbage in the stack. Every
dashboard refresh, every ad-hoc query, every failed attempt writes a file
that nobody reads twice.

Without expiry the bucket grows forever and you pay storage on query output
from months ago.

### Lifecycle rule anatomy

```hcl
rule {
  id     = "expire-results"   # required, must be unique in the config
  status = "Enabled"          # or "Disabled" to keep it defined but inert
  filter {}                   # empty = applies to every object
  expiration { days = 7 }     # delete 7 days after creation
}
```

**`filter {}` empty means the whole bucket.** Non-empty options:

```hcl
filter { prefix = "results/" }
filter { tag { key = "temp", value = "true" } }
filter {
  and {
    prefix                   = "logs/"
    object_size_greater_than = 131072
  }
}
```

Note `filter {}` must be present even when empty. Omitting it entirely is a
different (deprecated) syntax.

**The actions a lifecycle rule can take:**

| Action | Effect |
|---|---|
| `expiration` | delete current version after N days |
| `transition` | move to another storage class |
| `noncurrent_version_expiration` | delete old versions |
| `noncurrent_version_transition` | tier old versions |
| `abort_incomplete_multipart_upload` | clean up failed uploads |

That last one is worth knowing. Failed multipart uploads leave orphaned
parts that **you pay for and cannot see in the console listing.** Adding a
7-day abort rule to every bucket is standard hygiene and a real exam
answer.

**Transitions vs expiration order:** you can chain them. Standard for 30
days, then Standard-IA, then Glacier at 90, then delete at 365. Minimum
storage durations still apply, so transitioning to IA and deleting at day
20 still bills the full 30.

### The gap this exposes

The raw and curated buckets have **versioning enabled and no lifecycle
rule.** That combination accumulates noncurrent versions forever, and
curated is rewritten every single day by dynamic partition overwrite.

Every daily overwrite of `postings/ingest_date=X/company=Y/` leaves the
previous Parquet file as a noncurrent version, billed indefinitely. That's
the exact problem flagged in `02-storage.md` and it's still open.

---

## Workgroups

```hcl
resource "aws_athena_workgroup" "jobmarket" {
  name = "jobmarket"
```

**A workgroup is Athena's isolation and governance boundary.** Every query
runs in exactly one. There's a `primary` workgroup by default.

What a workgroup controls:

- Where results are written
- Result encryption
- Per-query and per-workgroup data scan limits
- Whether those settings can be overridden by the client
- CloudWatch metrics publication
- Which engine version
- Cost allocation tags

**The exam framing is almost always separation.** Multiple teams sharing an
account, each needing their own cost tracking, their own result location,
and their own scan ceiling. Workgroups do all three, and you attach IAM
policies to them so a team can only run queries in its own workgroup.

---

## Enforcing configuration

```hcl
enforce_workgroup_configuration = true
```

**This is the switch that makes the workgroup a control rather than a
default.**

| Value | Behavior |
|---|---|
| `false` | client settings win, workgroup config is a fallback |
| `true` | workgroup config wins, client settings are ignored |

With `false`, anyone connecting via JDBC or the SDK can specify their own
output location and bypass the scan limit entirely. The governance is
advisory.

With `true`, the limits are actually enforced. If a scenario says "ensure
all teams write results to an approved encrypted location regardless of
client settings," this is the answer.

---

## The scan cutoff

```hcl
bytes_scanned_cutoff_per_query = 1073741824  # 1 GB
```

**A per-query kill switch.** Any single query projected to scan more than
1 GB is cancelled automatically.

At $5/TB, 1 GB is half a cent. So this isn't really about the money at this
scale, it's about **catching the query that forgot a partition filter.**

```sql
-- scans one partition, a few KB
SELECT * FROM postings WHERE ingest_date = '2026-08-27';

-- scans every partition ever written
SELECT * FROM postings;
```

The second one is a typo away from the first. On a small dataset it's
pennies. On a real lake it's a five-figure surprise, and this is genuinely
how people run up enormous Athena bills.

**Two limit types, and they're distinct:**

| Control | Scope | Effect |
|---|---|---|
| `bytes_scanned_cutoff_per_query` | one query | cancels that query |
| **Data usage controls** | workgroup, per time period | alerts or disables the workgroup |

The second is configured outside this resource and can trigger an SNS
notification or shut the workgroup down when a daily or monthly threshold
is hit. If a question mentions alerting a team when aggregate spend crosses
a line, that's a data usage control, not the per-query cutoff.

---

## Results encryption

```hcl
result_configuration {
  output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
  encryption_configuration {
    encryption_option = "SSE_S3"
  }
}
```

**Query results are real data.** A `SELECT` against a sensitive table
writes those rows in plaintext CSV to S3. Encrypting the source and not the
results is a common gap, and a favorite exam trap.

Options are the same shape as S3 itself:

| Option | Notes |
|---|---|
| `SSE_S3` | AWS-managed keys, free |
| `SSE_KMS` | your key, audit trail via CloudTrail |
| `CSE_KMS` | client-side, Athena encrypts before writing |

`SSE_S3` is consistent with the rest of the stack, and the data is public
job postings, so KMS would buy nothing.

**One thing worth noting:** the results bucket has no
`aws_s3_bucket_server_side_encryption_configuration` resource like raw and
curated do. It relies on this Athena setting plus S3's default encryption
since January 2023. Functionally fine, structurally inconsistent.

---

## CloudWatch metrics

```hcl
publish_cloudwatch_metrics_enabled = true
```

Publishes per-workgroup metrics to the `AWS/Athena` namespace:

- `ProcessedBytes`
- `TotalExecutionTime`
- `QueryPlanningTime`
- `QueryQueueTime`
- `EngineExecutionTime`
- `ServiceProcessingTime`

You can alarm on any of these. `ProcessedBytes` trending up week over week
is the early warning that a query lost its partition filter or the data
grew past what your design assumed.

---

## What's not configured

**No `engine_version` block.** Defaults to whatever AWS considers current
(engine v3). Pinning is worth considering, since engine upgrades have
occasionally changed function behavior.

**No `requester_pays_enabled`.** Only relevant for cross-account data
sharing.

**No Athena named queries.** `aws_athena_named_query` saves SQL in the
console. The dashboard likely holds its queries in `data.py` instead.

**No data usage controls.** The aggregate spend limit described above. The
per-query cutoff is set but not the workgroup-level one.

**No CTAS or views defined in Terraform.** See below.

---

## CTAS, and why it matters

Not in this file, but heavily tested.

```sql
CREATE TABLE postings_parquet
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  partitioned_by = ARRAY['ingest_date'],
  external_location = 's3://bucket/optimized/'
) AS SELECT * FROM postings_raw;
```

**CREATE TABLE AS SELECT runs a query and writes the results as a new
table** in whatever format you specify.

The classic exam scenario: a team has JSON or CSV in S3, queries are slow
and expensive, and they want to convert to Parquet without building an ETL
job. CTAS does it in one statement, no Glue, no Spark.

Limits worth knowing: max 100 partitions per CTAS statement, and the target
location must be empty.

**`INSERT INTO`** appends to an existing table, which is how you'd add to a
CTAS-created table incrementally.

**Views** are stored in the Glue Catalog and are just saved SQL. They don't
reduce scan cost, since the underlying query still runs. That's a
distinction people get wrong: a view is not a materialization.

---

## Federated query

Also not here, also tested.

**Athena Federated Query** uses Lambda connectors to query non-S3 sources:
DynamoDB, RDS, Redshift, DocumentDB, CloudWatch Logs, HBase. You write one
SQL statement that joins S3 data against an operational database.

If a scenario says "query data across S3 and DynamoDB without moving it,"
that's federated query.

---

## Athena vs the alternatives

The comparison the exam actually cares about:

| | Athena | Redshift | Redshift Spectrum | EMR |
|---|---|---|---|---|
| Model | serverless | provisioned or serverless cluster | cluster + S3 | managed Hadoop/Spark |
| Billing | per TB scanned | per hour or RPU | per TB scanned | per instance-hour |
| Best for | ad-hoc, intermittent | frequent, complex, concurrent | joining warehouse to lake | custom frameworks |
| Setup | none | provisioning, tuning | needs a cluster | significant |

**Decision rules:**

- **Intermittent or unpredictable queries** → Athena. No idle cost.
- **Constant heavy querying with many concurrent users** → Redshift. At high
  volume, per-TB scanning costs more than an always-on cluster.
- **Data already in Redshift, need to join to S3** → Spectrum.
- **Need Spark, Hive, HBase, or custom libraries** → EMR.
- **Sub-second dashboard latency** → neither. That's a warehouse or a cache.

Your usage is a handful of dashboard queries a day over a few MB. Athena is
unambiguously right, and a Redshift cluster would be absurd.