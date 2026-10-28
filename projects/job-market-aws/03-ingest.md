# 03 - Ingest

Pulls job postings from Greenhouse and Lever, lands raw gzipped JSON in
S3. Entry point of the pipeline.

**Files:** `infra/lambda.tf`, `src/ingest/handler.py`, `config/companies.yml`

---
---

# Part 1: lambda.tf

## Packaging the code

```hcl
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../src/ingest"
  output_path = "${path.module}/../build/ingest.zip"
}
```

The archive provider declared in `main.tf` finally doing its job. Lambda
will not accept a directory, only a zip deployment package.

`path.module` is a built-in reference to the directory of the current
module, here `infra/`. Using it instead of a bare relative path means the
config works regardless of what directory you run `terraform` from.

```hcl
path.module  # directory of this module
path.root    # directory of the root module
path.cwd     # where you invoked terraform
```

It's a `data` block, not a `resource`, because it creates nothing in AWS.
Local computation, runs on every plan.

---

## The hash mechanism

```hcl
filename         = data.archive_file.ingest.output_path
source_code_hash = data.archive_file.ingest.output_base64sha256
```

`filename` tells Lambda which zip to upload.

`source_code_hash` is the redeploy trigger. Terraform stores this hash in
state. On the next plan the archive provider rezips the source and
computes a fresh hash. Changed Python means changed hash means Terraform
sees drift and redeploys.

**Without `source_code_hash`,** Terraform compares only the `.tf` files.
Edit `handler.py`, run apply, Terraform sees no diff and deploys nothing.
Your bug stays live and you lose an hour wondering why.

Most common Lambda-in-Terraform mistake.

---

## Log group created explicitly

```hcl
resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${var.name_prefix}-ingest"
  retention_in_days = 14
}
```

If the log group doesn't exist, Lambda creates it on first invocation
**with retention set to Never Expire.** Logs accumulate and bill forever.

Declaring it buys three things:

- Retention control, 14 days here
- Terraform owns it, so `terraform destroy` removes it. An auto-created
  group survives and lingers as orphaned cost.
- It exists before the first invocation

The name is not arbitrary. Lambda writes to `/aws/lambda/<function-name>`
and that path is fixed by AWS. Get it wrong and you get two log groups:
your empty one and the auto-created one Lambda actually uses.

Retention is a fixed set: 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365,
400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653 days, or 0 for
never expire.

---

## Why depends_on is here

```hcl
depends_on = [aws_cloudwatch_log_group.ingest]
```

The Lambda resource never references the log group. No
`aws_cloudwatch_log_group.ingest.name` anywhere in the block. So Terraform
has no implicit dependency to infer and would create them in parallel.

The race: Lambda gets created and invoked, sees no log group, creates one
with Never Expire retention. Terraform then hits a name collision, or
silently ends up managing a group with the wrong settings.

`depends_on` forces order without a data reference. This is the
legitimate use case. Most of the time needing it means you should
reference an attribute instead, but here there genuinely is no attribute
to reference.

---

## Runtime and handler

```hcl
runtime = "python3.12"
handler = "handler.handler"
```

`handler` reads as `<filename>.<function_name>`. Python looks for
`handler.py` and calls `def handler(event, context)`. If the file were
`main.py` with `def lambda_handler(...)`, this would be
`main.lambda_handler`.

Runtimes get deprecated on a schedule. AWS stops security patches, then
blocks updates, then blocks invocations. Pinning means you choose when to
migrate.

---

## Memory and timeout

```hcl
memory_size = 512
timeout     = 120
```

**Memory is badly named.** It's the only performance dial Lambda has, and
CPU is allocated proportionally. At 1,769 MB you get one full vCPU. Below
that, a fraction.

So more memory can make a function *cheaper*. Billing is GB-seconds,
memory times duration. Double the memory and if it finishes in under half
the time, you paid less.

Range is 128 MB to 10,240 MB.

512 fits this workload: HTTP calls, JSON parsing, S3 writes. IO-bound, not
CPU-bound, so extra memory wouldn't help.

**Timeout hard ceiling is 900 seconds, 15 minutes.** One of the most
tested Lambda facts. 120 gives headroom while still failing fast if an API
hangs.

The pattern: **if a workload might exceed 15 minutes, Lambda is the wrong
answer.** That's the exam's favorite elimination. Alternatives are Glue,
Fargate, Batch, or Step Functions splitting the work.

---

## Environment variables

```hcl
environment {
  variables = {
    RAW_BUCKET = aws_s3_bucket.raw.bucket
  }
}
```

Passes the bucket name in so `handler.py` reads `os.environ["RAW_BUCKET"]`
instead of hardcoding it. Also creates the implicit dependency on S3, so
Terraform builds the bucket first.

**Security note:** Lambda env vars are encrypted at rest by default but
**visible in plaintext in the console** to anyone with
`lambda:GetFunction`. Fine for a bucket name. Never an API key. Those go
in Secrets Manager or SSM Parameter Store, fetched at runtime.

`AWS_REGION` and `AWS_LAMBDA_FUNCTION_NAME` are injected automatically.

---

## The IAM role

```hcl
role = aws_iam_role.lambda.arn
```

Defined in `iam.tf`, covered in `08-security.md`. Lambda assumes this role
and gets temporary credentials injected into the execution environment.
boto3 picks them up with no configuration.

Needs at minimum:
- `logs:CreateLogStream` and `logs:PutLogEvents`
- `s3:PutObject` scoped to the raw bucket

No long-lived access keys anywhere. The exam's preferred answer over
embedded credentials, always.

---

## What's not configured

**No VPC config.** Runs in AWS-managed networking with direct internet
access, which it needs to reach the ATS APIs. Putting a Lambda in a VPC
removes internet access unless you add a NAT gateway. Common exam
scenario: "Lambda in a VPC can't reach the internet," answer is NAT
gateway, or a VPC endpoint if the target is an AWS service.

**No reserved or provisioned concurrency.** Reserved caps how many run at
once and protects downstream systems. Provisioned keeps instances warm to
kill cold starts, and costs money. Neither matters for one daily run.

**No dead letter queue.** Failure handling lives in Step Functions
instead, see `06-orchestration.md`.

**No layers.** Dependencies must be in the zip or in the runtime.

**No architecture set.** Defaults x86_64. Graviton (`arm64`) is ~20%
cheaper for compatible workloads.

---
---

# Part 2: handler.py

## No dependencies

```python
import gzip, json, os, urllib.error, urllib.request
from datetime import datetime, timezone
import boto3
```

Everything except boto3 is stdlib, and boto3 is preinstalled in the Lambda
Python runtime. So the zip is just source files. No `pip install`, no
vendored packages.

That's why `urllib.request` instead of `requests`, which is nicer to write
but would need bundling. Zip stays tiny, cold starts stay fast, nothing to
patch.

**The tradeoff:** if you did need a third-party library, options are
vendoring into the zip, a Lambda layer, or a container image (up to 10 GB,
versus the 250 MB unzipped zip limit).

**Caveat:** the runtime's boto3 version is whatever AWS ships and changes
without warning. Depending on a new API means pinning your own.

---

## Module-level vs handler-level

```python
BUCKET = os.environ["RAW_BUCKET"]
s3 = boto3.client("s3")     # module scope, outside handler()
```

Runs once per **execution environment**, not once per invocation.

```
cold start  -> download zip, start runtime, run module code, call handler()
warm invoke -> call handler() only
```

Module scope means the client, its connection pool, and its credential
resolution are reused across warm invocations. Building it inside
`handler()` redoes that work every time. Standard Lambda optimization,
shows up on the exam.

`os.environ["RAW_BUCKET"]` with brackets is deliberate. Missing variable
means immediate KeyError at init, failing loudly at deploy rather than
silently writing nowhere.

**Flip side:** module scope persists between invocations. Never cache
request-specific state there or you leak data between runs.

---

## Two env vars, one pattern each

```python
BUCKET = os.environ["RAW_BUCKET"]                    # required, crash if missing
TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "30")) # optional, has a default
```

Required uses brackets. Optional uses `.get()` with a fallback. Same
required-vs-optional idea as Terraform's `default`.

`FETCH_TIMEOUT` is never set in `lambda.tf`, so it's always 30. It's a
knob you can turn without redeploying code.

---

## Timeout layering

```python
TIMEOUT = 30          # per HTTP request
timeout = 120         # Lambda function total, from lambda.tf
```

Nested timeouts, correctly ordered. A hung ATS endpoint dies at 30s with a
clean Python exception you can read. If the inner timeout were larger,
Lambda would kill the whole function and you'd get a useless "Task timed
out" with no context.

**Rule: inner timeout always shorter than outer.** Same applies to Step
Functions state timeouts wrapping Lambda timeouts.

---

## User-Agent

```python
UA = "job-market-pipeline/0.1 (+https://github.com/Jonah-Schwartz521)"
```

Identifying yourself to third-party APIs is basic etiquette. Some
endpoints block default Python UA strings, and a contact URL means someone
can reach you rather than just blocking.

---

## Endpoint dispatch

```python
ENDPOINTS = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
}

def build_url(ats, token):
    try:
        return ENDPOINTS[ats].format(token=token)
    except KeyError:
        raise ValueError(f"unknown ats: {ats}")
```

Adding a third ATS is one dict entry plus a validator branch, not a new
if-chain. KeyError to ValueError turns a cryptic lookup failure into a
message naming the bad input.

---

## Validate before write

```python
def validate(raw_bytes, ats):
    """Parse only to gate the write. The parsed object is discarded."""
    doc = json.loads(raw_bytes)
    if ats == "greenhouse":
        if not isinstance(doc, dict) or "jobs" not in doc:
            raise ValueError("greenhouse response missing 'jobs' key")
        ...
        return len(doc["jobs"])
```

Best decision in the file.

**The parsed object is thrown away.** What gets written is the original
bytes, not `json.dumps(doc)`. That preserves the response exactly as sent:
key order, whitespace, fields you didn't know existed. That's what makes
it a genuine raw zone. If your parsing logic turns out wrong in six
months, the original is there to reprocess.

**Parsing is only a gate.** If the API returns HTML, an error page, or
truncated JSON, the parse fails and nothing is written. Bad data never
enters the lake, which is far cheaper than finding it three stages
downstream in Athena.

The validators encode a real shape difference: Greenhouse wraps postings
in `{"jobs": [...]}`, Lever returns a bare array. Same reason
`04-transform` needs two separate normalize scripts.

---

## Partitioning, and the small files problem

```python
def build_key(ats, company, scheduled_time):
    day = scheduled_time[:10]
    stamp = scheduled_time.replace("-", "").replace(":", "")
    return (
        f"raw/ats={ats}/company={company}/ingest_date={day}/"
        f"{stamp}.json.gz"
    )
```

Produces:

```
raw/ats=greenhouse/company=stripe/ingest_date=2026-08-27/20260827T063400Z.json.gz
```

`key=value` directory naming is **Hive-style partitioning**. A convention,
not an S3 feature, but Glue crawlers and Athena both recognize it and turn
each segment into a queryable column. Query `WHERE ats = 'greenhouse'` and
Athena skips every other prefix, scanning less and billing less.

Three partition keys: `ats`, `company`, `ingest_date`. Order matters,
filtering works left to right.

Timestamp filename means daily runs never overwrite. Combined with bucket
versioning, two independent layers of history.

**The problem this creates.** One small gzipped file per company per day.
50 companies is 50 tiny files a day, 18,000 a year, growing linearly.

- Every S3 object read is a separate GET with its own latency
- Athena and Spark both degrade badly on many small files
- `company=` is high cardinality: more companies, more partitions, and
  partition metadata becomes its own overhead

Rule of thumb is 128 MB or larger per file. These are kilobytes.

**Why it's acceptable here:** this is the raw zone, queried rarely. The
curated zone Glue writes is where file size matters, because that's what
Athena hits. Compaction in the transform step is the standard fix.

---

## Compression

```python
Body=gzip.compress(raw_bytes),
ContentType="application/json",
ContentEncoding="gzip",
```

Gzip on JSON typically hits 70 to 90% reduction, cutting both storage and
Athena scan cost. Setting both headers means S3 and downstream tools know
what they're holding. Athena decompresses `.gz` transparently.

**Gzip's real limitation:** it is **not splittable**. Spark cannot break a
gzip file into chunks for parallel processing, so one file gets one worker
regardless of size. Fine for small files. Wrong for large ones.

| Codec | Splittable | Ratio | Speed |
|---|---|---|---|
| gzip | no | high | slow |
| bzip2 | yes | highest | slowest |
| snappy | no* | low | fastest |
| lz4 | no* | low | fastest |
| zstd | no* | high | fast |

*Snappy inside Parquet is splittable, because Parquet's block structure
handles the splitting. That's why Parquet plus Snappy is the default
recommendation for the curated zone.

---

## Object metadata

```python
Metadata={
    "record-count": str(record_count),
    "source-url": url,
    "scheduled-time": scheduled_time,
}
```

Custom metadata retrievable via a HEAD request without downloading the
body. So you can check whether a file has 0 or 500 postings without
pulling it.

Limits: 2 KB total, string values only, keys lowercased by S3 and prefixed
`x-amz-meta-` on the wire.

---

## Structured logging

```python
print(json.dumps({
    "company": company, "ats": ats, "records": record_count,
    "bytes_raw": len(raw_bytes), "key": key,
}))
```

`print` goes to CloudWatch Logs. Emitting JSON means Logs Insights can
query it as fields:

```sql
fields @timestamp, company, records
| filter records = 0
| sort @timestamp desc
```

The difference between grepping logs and querying them. `monitoring.tf`
builds on this.

---

## The return value

```python
return {"key": key, "record_count": record_count}
```

Goes back to whatever invoked the function. Step Functions passes it into
the next state, so downstream states can branch on `record_count` without
touching S3.

---

## The bug

```python
stamp = scheduled_time.replace("-", "").replace(":", "")
```

Intent is `2026-08-27T06:34:00Z` becoming `20260827T063400Z`. That works.

But `scheduled_time` comes from EventBridge Scheduler, and Scheduler's
`<aws.scheduler.scheduled-time>` format includes microseconds:

```
2026-08-27T06:34:00.000Z  ->  20260827T06340.000Z
```

Dots survive, producing a malformed filename. The local fallback formats
correctly, so this only breaks in the deployed path, which is worse.

`day = scheduled_time[:10]` also silently produces garbage if the input
isn't ISO-8601. A slicing assumption with no validation.

Fix is to parse rather than string-replace:

```python
dt = datetime.fromisoformat(scheduled_time.replace("Z", "+00:00"))
day = dt.strftime("%Y-%m-%d")
stamp = dt.strftime("%Y%m%dT%H%M%SZ")
```

Verify against an actual S3 listing before assuming either way.

---

## Idempotency

Rerun the same schedule twice and you get two files, since the timestamp
differs. Not idempotent, so downstream reads double-count.

The alternative is keying on `ingest_date` only, so a rerun overwrites.
Idempotent, but loses the record of what the API returned each time, which
matters when postings vanish as they're filled.

The exam favors idempotency, since retries are assumed. Worth knowing this
is a deliberate tradeoff and being able to say why.

---
---

# Part 3: companies.yml

## What this file is

A registry of every company to scrape. 43 entries, 13 active.

**Nothing in the pipeline hardcodes a company.** `handler.py` takes `ats`,
`company`, and `board_token` from the event payload. Terraform reads this
YAML and generates one scheduler target per active row. Adding a company
is a YAML edit and an apply, not a code change.

That's config-as-data. The alternative is 13 near-identical resource
blocks in `scheduler.tf`, which drift the moment someone edits one and not
the others.

---

## Terraform reading YAML

```hcl
locals {
  companies = yamldecode(file("${path.module}/../config/companies.yml")).companies

  scheduled = [
    for c in local.companies :
    c if c.active && contains(["greenhouse", "lever"], c.ats)
  ]
}
```

`yamldecode()` turns YAML into a Terraform data structure. `file()` reads
it from disk. Then a `for` expression with `if` filters the list:

```
active == true  AND  ats in (greenhouse, lever)
```

Two conditions meaning different things:

- `active: false` = deliberately parked, ingestion works, cost is the
  reason
- `ats: ashby` = no ingestion support exists, code would fail if scheduled

Ashby rows are also `active: false`, so either alone would exclude them.
Encoding both makes the intent explicit rather than accidental.

```hcl
resource "aws_scheduler_schedule" "ingest" {
  for_each = { for c in local.scheduled : c.board_token => c }
  ...
}
```

`for_each` creates one resource per map entry. Flip `active` to true on
Brex and the next apply creates exactly one schedule. Flip it back and
that one is destroyed. Everything else untouched.

**Why `for_each` and not `count`:** `count` indexes by position, so
removing the third entry shifts everything after it and Terraform destroys
and recreates them all. `for_each` keys by a stable string, so each
resource has a permanent identity. **Always `for_each` for lists that
change.** Real Terraform gotcha.

---

## The industry field

```yaml
industry: healthcare-it
```

The header comment calls it out: **there is no ATS field for this.** It's
a label you assign.

This is the analytical spine of the project. Without it the data is "here
are job postings." With it you can ask whether healthcare IT demands
different skills than fintech, which is the actual question the dashboard
answers.

**The warning in the comment is the real lesson:**

> industry lives on the company, not the posting, so changing a company's
> industry retroactively relabels its entire history

This is a **slowly changing dimension** problem, and it's tested.

Current design is **SCD Type 1**: overwrite in place, no history. Relabel
Doximity from healthcare-it to consumer-tech and every posting ever
ingested is now consumer-tech, including ones from before the change. Any
trend analysis across that boundary is silently wrong.

| Type | Behavior | Cost |
|---|---|---|
| **Type 0** | never changes | rigid |
| **Type 1** | overwrite, no history | history is lost |
| **Type 2** | new row per change, with valid_from and valid_to | correct, complex |
| **Type 3** | keep a previous_value column | one level of history only |

Type 2 is what warehouses use for exactly this. The exam signal is "track
historical changes to an attribute over time," which always means Type 2.

Type 1 is defensible here since industry rarely changes and the project is
analytical rather than transactional. What matters is being able to say
*why*.

**How it enters the data:** `industry` isn't in the ATS response, so it
gets joined in during transformation. That's a **dimension table joined to
a fact table**, which is star schema thinking. Confirm where in
`04-transform`.

---

## board_token vs name

```yaml
- {name: Komodo Health, board_token: komodohealth, ...}
```

Two identifiers, different jobs. `board_token` is the ATS slug from the
careers URL, the API key, must match exactly. `name` is human-readable.

`handler.py` uses `company` in the S3 key:

```python
f"raw/ats={ats}/company={company}/ingest_date={day}/"
```

**Check which one gets passed.** If `name` is used, "Komodo Health" lands
in an S3 prefix with a space. Legal in S3, but it breaks Hive partition
parsing in Glue and Athena, since values with spaces need URL encoding and
behave inconsistently. `board_token` gives `company=komodohealth` and
everything is clean.

---

## Cost control by config

13 active out of 43. Deliberate throttling.

What scales with company count:

- **Lambda invocations.** 13 vs 43 a day. Negligible, well inside free
  tier.
- **S3 objects.** More files, more small files, more partitions.
- **Glue DPU-hours.** Where real cost lives. More partitions to crawl,
  more data to transform.
- **Athena scan volume.** More data per query, billed per TB.

Parked entries also document intent, and flipping one is a one-word
change.

---

## Why YAML

Comments, mostly. JSON has no comment syntax, and the header block here is
load-bearing documentation about the filter logic and the SCD warning.

YAML also supports flow style (`{name: X, ats: Y}`), keeping each company
on one line and the file scannable as a table.

Terraform reads both natively via `yamldecode()` and `jsondecode()`.

**Tradeoff:** YAML is whitespace-sensitive with genuinely strange edge
cases. The classic is the Norway problem, where unquoted `no` parses as
boolean false.

---

## What's missing

**No validation.** A typo in `board_token` produces a 404 at runtime.
`yamldecode` parses anything YAML-shaped.

**No schema.** Nothing enforces that every entry has all five keys. A
missing `active` would fail at the filter with a confusing error.

**Industry values are free text.** Nothing constrains them to a known set,
so `healthcare-it` and `healthcare_it` would silently become two
categories.

---
---

# Exam angles

**Domain 1, Ingestion and Transformation (34%)**
- Lambda for event-driven and short scheduled ingestion
- **The 15-minute ceiling as the elimination criterion**
- Hive-style partitioning and how Athena and Glue consume it
- Partition key selection, cardinality, left-to-right filtering
- Small files problem and compaction
- Compression codecs, splittability, gzip vs snappy vs zstd
- Raw zone preserves original bytes, transformation happens downstream
- Config-driven ingestion, one code path for many sources

**Domain 2, Data Store Management (26%)**
- **Slowly changing dimensions, especially Type 1 vs Type 2**
- Dimension tables joined to facts, star schema basics
- Partition value hygiene: lowercase, no spaces, stable

**Domain 3, Operations (22%)**
- CloudWatch Logs retention as a cost control
- Memory as the CPU dial, GB-seconds billing
- Reserved vs provisioned concurrency
- Structured JSON logging and Logs Insights
- Module-scope client reuse across warm invocations
- Nested timeouts, inner shorter than outer
- Idempotency and retry safety

**Domain 4, Security (18%)**
- Execution roles over embedded credentials
- Env vars encrypted at rest but readable in console, so secrets go to
  Secrets Manager or SSM
- Lambda in a VPC loses internet unless NAT or VPC endpoint

---

# Decision rules

```
Over 15 min                      -> not Lambda. Glue, Fargate, or Batch.
Under 15 min, event-driven       -> Lambda
Spark / big data                 -> Glue
Long-running container           -> Fargate
Cold starts hurt                 -> provisioned concurrency
Protect downstream               -> reserved concurrency
Secrets                          -> Secrets Manager or SSM, never env vars
Preserve source fidelity         -> write original bytes, validate separately
Many small files                 -> compact in transform, not at ingest
Spark must parallelize a file    -> splittable codec, or Parquet+Snappy
Partition key                    -> low cardinality, matches WHERE clauses
Reused client / connection       -> module scope, not inside the handler
Track attribute history          -> SCD Type 2
Current value only               -> SCD Type 1
Many similar resources from list -> for_each, never count
Config that changes often        -> external file, not hardcoded
```

---

# Gaps

- [ ] Lambda concurrency: account limit, burst, reserved vs provisioned
- [ ] Cold start mechanics and what actually affects them
- [ ] When a Lambda layer is worth it
- [ ] Graviton / arm64 tradeoffs
- [ ] Lambda in a VPC: ENIs, NAT, VPC endpoints
- [ ] Verify the microsecond bug against a real S3 listing
- [ ] Confirm the Glue job compacts, or add compaction
- [ ] No retry logic in fetch(), relying on Step Functions instead
- [ ] Partition projection as an alternative to crawler-registered partitions
- [ ] Confirm whether `name` or `board_token` reaches the S3 key
- [ ] Verify `scheduler.tf` uses for_each and not count
- [ ] Where does industry get joined in? Which Glue script?
- [ ] SCD Type 2 mechanics: valid_from, valid_to, surrogate keys