# 04 - Transform

Raw gzipped JSON becomes typed, partitioned, query-ready Parquet. Two
normalize jobs write one `postings` table, a third extracts skills into
`posting_skills`, one crawler catalogs both.

**Files:** `infra/glue.tf`, `infra/curated.tf`, `infra/glue_job.tf`,
`src/glue/normalize_greenhouse.py`, `src/glue/normalize_lever.py`,
`src/glue/extract_skills.py`, `src/glue/roles.py`

**Pipeline order:**
```
Ingest -> Normalize (Greenhouse + Lever) -> ExtractSkills -> CrawlCurated
```

---

## The catalog database

```hcl
resource "aws_glue_catalog_database" "jobmarket" {
  name = "jobmarket"
}
```

The Glue Data Catalog is a **metadata store**, not a data store. Table
definitions only: column names, types, S3 location, format, partition keys.
Zero rows.

Data lives in S3 as files. The catalog makes those files look like SQL
tables. Athena, Redshift Spectrum, EMR, and Glue jobs all read the same
catalog, so one definition serves every engine.

**Hive Metastore compatible**, which is why Hive-style partitioning from the
ingest layer works with no configuration.

A `database` is a namespace grouping tables. No compute, no storage, free.

**Regional.** A catalog in us-east-1 is invisible to Athena in us-west-2.
This is the coupling reason for keeping everything in one region.

---

## Trust policy vs permissions policy

```hcl
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}
```

| Policy type | Question it answers |
|---|---|
| **Trust policy** | WHO can become this role |
| **Permissions policy** | What can this role DO |

This is the trust policy. Without it Glue physically cannot use the role no
matter what permissions you attach.

`sts:AssumeRole` returns temporary credentials, ~1 hour, auto-rotated.
That's the mechanism behind "no long-lived keys anywhere."

`type = "Service"` means an AWS service rather than a user or account.
Identifier is always `<service>.amazonaws.com`.

`aws_iam_policy_document` over heredoc JSON: Terraform validates structure,
interpolation works without escaping hell, errors surface at plan time.
`iam.tf` uses raw `.json` files instead, so the repo does both. Equivalent
output.

---

## AWS managed vs customer managed vs inline

```hcl
policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
```

Note the ARN has **no account ID**. That's how you spot an AWS managed
policy: AWS owns it, identical in every account.

| Type | Owner | Reusable | Exam framing |
|---|---|---|---|
| AWS managed | AWS | yes | fast, often over-permissive |
| Customer managed | you | yes | best practice for least privilege |
| Inline | you | no, tied to one identity | one-off, dies with the role |

---

## Bucket ARN vs object ARN

```hcl
resources = [
  aws_s3_bucket.raw.arn,                     # the bucket
  "${aws_s3_bucket.raw.arn}/raw/*",          # the objects
  aws_s3_bucket.curated.arn,
  "${aws_s3_bucket.curated.arn}/postings/*",
]
```

```
arn:aws:s3:::my-bucket        <- the bucket itself
arn:aws:s3:::my-bucket/*      <- the objects inside it
```

Different resources, different actions:

- `s3:ListBucket` is **bucket-level**. Needs the bare bucket ARN.
- `s3:GetObject` is **object-level**. Needs the object ARN with `/*`.

Get it wrong and listings fail while individual gets work, or vice versa.
Very common exam question.

**S3 ARNs have no region or account:**
```
arn:aws:s3:::bucket-name
arn:aws:lambda:us-east-1:123456789012:function:name
```
Bucket names are globally unique, so no disambiguation needed. Almost
everything else needs it, which is why `aws_caller_identity` matters
elsewhere but not here.

---

## Why two crawlers were removed

### No raw crawlers

> Keeping a crawler would have put it on the critical path between ingest
> and normalize for no benefit.

**A crawler exists to make files queryable by SQL.** Nothing queries raw
with SQL, so a raw crawler produces metadata nobody reads.

```
with:     ingest -> crawl raw -> normalize -> crawl curated
without:  ingest -> normalize -> crawl curated
```

A crawler bills a **10-minute minimum** regardless of duration, and it's a
serial step everything downstream waits on.

Glue jobs read S3 directly with `spark.read.json("s3://...")`. No catalog
table needed. The catalog is for **query engines**, not for Spark reading a
known path.

> The orphaned `jobmarket.ats_greenhouse` table is not dropped by removing
> this resource.

Removing the resource that *creates* a table doesn't remove what that
resource *produced*. Same class of problem as the Lambda auto-created log
group.

### No Lever crawler

> Lever raw files are a bare top-level JSON array, which the Hive JSON SerDe
> cannot parse: it collapsed an 89-element file into a single row.

**SerDe = Serializer/Deserializer.** Translates bytes on disk into rows and
columns. The catalog stores which SerDe a table uses.

Hive JSON SerDe expects **JSON Lines**, one object per line:
```json
{"id": 1, "title": "Data Engineer"}
{"id": 2, "title": "Analyst"}
```

Lever returns one array across many lines:
```json
[
  {"id": 1, "title": "Data Engineer"},
  {"id": 2, "title": "Analyst"}
]
```

The SerDe reads line by line, sees one document, emits one row. 89 postings
collapse to 1. **No error, just wrong data.** Worst possible failure mode.

Spark's `multiLine` treats the file as one document and expands the array.

**Exam relevance:** most AWS analytics tooling assumes JSON Lines. Firehose
writes it, Athena's JSON SerDe expects it, Spark defaults to it. Scenario
mentions array-wrapped JSON not parsing? This is why.

---

## The surviving crawler

```hcl
resource "aws_glue_crawler" "curated_postings" {
  s3_target { path = "s3://${aws_s3_bucket.curated.bucket}/postings/" }
  s3_target { path = "s3://${aws_s3_bucket.curated.bucket}/posting_skills/" }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
```

**What a crawler does:** samples files, infers schema, detects partitions
from prefix structure, writes table definitions.

Curated gets one because Athena and the dashboard query it with SQL. That's
the test: does something need SQL access?

**Two targets, one crawler.** $0.44/DPU-hour, billed per second, 10-minute
minimum per run. Two crawlers means two minimums for identical output. Each
`s3_target` becomes its own table, named from the final path component.

### Schema change policy

| `update_behavior` | Behavior |
|---|---|
| `UPDATE_IN_DATABASE` | apply schema changes to the table |
| `LOG` | record it, leave the table alone |

| `delete_behavior` | Behavior |
|---|---|
| `LOG` | note data is gone, keep the table |
| `DELETE_FROM_DATABASE` | drop the table or partition |
| `DEPRECATE_IN_DATABASE` | mark deprecated, keep it |

`LOG` on deletes is the safe choice. A transient failure showing an empty
prefix would, under `DELETE_FROM_DATABASE`, drop your table and break every
downstream query.

**The principle:** permissive about additions, conservative about deletions.

### Not configured

**No schedule.** Triggered by Step Functions, so it runs after the jobs
write rather than on a timer that might fire mid-write.

**No `recrawl_policy`.** Defaults to `CRAWL_EVERYTHING`, rescanning all
files every run. `CRAWL_NEW_FOLDERS_ONLY` is much cheaper on a growing
dataset.

**No classifiers.** Custom classifiers override schema inference for formats
Glue guesses wrong. Not needed for Parquet, which is self-describing.

**No exclusions.** Would skip `_temporary` and `_SUCCESS` marker files.

---

## The alternative: partition projection

Crawlers are one way to register partitions. **Partition projection** is the
other, and it's heavily tested.

Instead of scanning S3, you declare the pattern in table properties:
```
ingest_date: date type, range 2026-01-01 to NOW, format yyyy-MM-dd
company:     enum, values komodohealth,zocdoc,...
```
Athena computes the S3 prefix at query time.

| | Crawler | Partition projection |
|---|---|---|
| Discovery | scans S3 | computed from a rule |
| Cost | DPU-hours per run | free |
| Latency | must run before new data is queryable | instant |
| Fit | unpredictable structure | predictable structure |
| Scale | slow with many partitions | scales well |

**Decision rule:** highly partitioned data with a predictable pattern that
must be queryable immediately means partition projection. Unknown or
irregular structure means a crawler.

Your `ingest_date` partitions are perfectly regular, so projection would
work and cost nothing.

---

## The curated bucket

```hcl
resource "aws_s3_bucket" "curated" {
  bucket = "${var.name_prefix}-curated-${data.aws_caller_identity.current.account_id}"
}
```

Structurally identical to `s3.tf`: public access blocked, versioning
enabled, SSE-S3. Everything in `02-storage.md` applies. What matters is
**why it's a second bucket.**

```
raw/       immutable, source fidelity, gzipped JSON
curated/   query-ready, Parquet, typed
```

Common naming is bronze / silver / gold. Data moves through zones, each more
processed, each with different rules.

**Why separate buckets, not two prefixes:**

- **IAM boundary.** The real reason. Normalize gets read-only on raw and
  read-write on curated. Trivial with two bucket ARNs, awkward with prefix
  conditions.
- **Blast radius.** A bad lifecycle rule or `DeleteObject` grant can only
  damage one zone. Raw is the zone you can't regenerate.
- **Different lifecycle needs.** Raw wants long retention and archival
  tiers. Curated is rewritten constantly and wants aggressive
  noncurrent-version expiry.
- **Different access patterns.** Curated is what Athena and the dashboard
  hit. Raw is touched only by normalize.

### What actually lives in curated

```
postings/            fact table, Parquet
posting_skills/      bridge table, Parquet
scripts/             Glue job source, uploaded by Terraform
reference/skills/    VPS dictionary export
tmp/                 Glue --TempDir scratch
```

**Worth flagging.** Job artifacts and reference data mixed into a data
bucket muddies the boundary. `scripts/` is code, not data, and it's why the
extract-skills role needs a `scripts/*` grant. A separate artifacts bucket
would be cleaner.

---

## Deploying job code

```hcl
resource "aws_s3_object" "normalize_script" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/normalize_greenhouse.py"
  source = "${path.module}/../src/glue/normalize_greenhouse.py"
  etag   = filemd5("${path.module}/../src/glue/normalize_greenhouse.py")
}
```

Glue jobs don't carry their code. The job definition holds a
`script_location` pointing at S3, and Glue fetches the script at run time.

**`etag = filemd5(...)` is the same trick as `source_code_hash` on the
Lambda.** Without it Terraform compares only the `.tf` files, you edit a
script, apply, nothing uploads, and the job runs yesterday's code.

> The bucket is SSE-S3, so the object ETag stays a plain MD5 and the
> comparison holds.

**S3's ETag is only an MD5 under specific conditions.** With SSE-KMS, or
with a multipart upload, the ETag becomes something else (multipart ETags
look like `<hash>-<partcount>`). Then `filemd5()` never matches, and
Terraform either shows a permanent diff or stops detecting changes.

A real, concrete cost of choosing SSE-KMS that most people never consider.

`roles.py` uploads the same way even though it isn't a job script. It's a
shared module delivered via `--extra-py-files`.

---

## Two roles, deliberately

> Its own role rather than glue_normalize. That role holds Get/Put/Delete on
> the whole curated bucket because the normalize jobs own the postings
> table; this job only consumes postings and must not be able to rewrite
> them.

**Separation of duties, and the best security decision in the repo.**

| Role | raw | postings | posting_skills |
|---|---|---|---|
| `glue_normalize` | read | **write** | (write, incidentally) |
| `glue_extract_skills` | none | read | **write** |

Extract-skills reads `postings` and writes `posting_skills`. Sharing a role
with normalize would mean a bug in the skills matcher could corrupt the
postings table. Separate roles make that impossible via IAM rather than
relying on the code being correct.

**Exam framing:** least privilege isn't just "don't grant admin." Each
component gets exactly what its job requires, and one component's failure
cannot damage another's data. Offered "one role for the pipeline" versus "a
role per job," the second wins.

Note the normalize policy grants `curated.arn/*` (whole bucket) while
extract-skills is scoped to prefixes. Normalize is looser than it needs to
be. Worth tightening.

### The scripts/ grant

```hcl
resources = [
  "${aws_s3_bucket.curated.arn}/reference/*",
  "${aws_s3_bucket.curated.arn}/postings/*",
  "${aws_s3_bucket.curated.arn}/scripts/*",
]
```

**Glue uses the job's IAM role to download the script.** No `scripts/*`
grant means the job fails before a single line of your Python executes, with
an error that looks like a code problem but isn't.

### Where least privilege hits a wall

```hcl
statement {
  sid       = "ListCurated"
  actions   = ["s3:ListBucket"]
  resources = [aws_s3_bucket.curated.arn]
}
```

> Spark's own listing walks several prefixes on the way to the leaf files,
> and a prefix condition tight enough to be meaningful breaks it.

Honest documentation of a compromise. `s3:prefix` conditions can scope
`ListBucket`, but Spark's partition discovery lists multiple levels. The
mitigation is stated: object-level access is still scoped, so `ListBucket`
only reveals key names, not contents.

### DeleteObject and dynamic overwrite

```hcl
actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
```

**"Overwrite" in Spark is not an S3 operation.** S3 has no overwrite. The
sequence is list the target partition, delete what's there, write new files.
So `mode("overwrite")` requires delete permission.

People routinely grant only `PutObject`, then get AccessDenied on a job that
appears to only be writing.

---

## Log group per job

```hcl
resource "aws_cloudwatch_log_group" "glue_normalize" {
  name              = "/aws-glue/jobs/${local.normalize_job_name}"
  retention_in_days = 14
}
```

> Left at the default it writes to the shared `/aws-glue/jobs/logs-v2` group
> and the retention below would apply to every Glue job in the account.

Glue's default continuous log destination is **shared across every Glue job
in the account.** Setting 14-day retention there would silently apply to
jobs you don't own, and Terraform would be managing a resource other teams
depend on.

---

## Job sizing and billing

```hcl
glue_version      = "4.0"
worker_type       = "G.1X"
number_of_workers = 2
max_retries       = 0
timeout           = 10
```

**`glue_version`** pins the runtime:

| Glue | Spark | Python |
|---|---|---|
| 2.0 | 2.4 | 3.7 |
| 3.0 | 3.1 | 3.7 |
| 4.0 | 3.3 | 3.10 |
| 5.0 | 3.5 | 3.11 |

**`worker_type`** is instance size, measured in DPUs (Data Processing
Units):

| Type | DPU | vCPU | Memory | Use |
|---|---|---|---|---|
| G.1X | 1 | 4 | 16 GB | standard default |
| G.2X | 2 | 8 | 32 GB | memory-heavy transforms |
| G.4X | 4 | 16 | 64 GB | large joins |
| G.8X | 8 | 32 | 128 GB | very large |
| G.025X | 0.25 | 2 | 4 GB | streaming, small batches |

`number_of_workers = 2` is the minimum for Spark ETL. Correct for a few MB
a day.

**Billing:** $0.44 per DPU-hour, per-second, **1-minute minimum** for Glue
2.0+. Different from crawlers, which keep the 10-minute minimum.

**`timeout = 10` is in MINUTES.** Lambda's timeout is in seconds. This trips
people constantly.

**`max_retries = 0`** is deliberate. Step Functions owns retry logic. Two
retry layers means a transient failure can retry 3 × 3 = 9 times,
multiplying cost and making failures hard to reason about. **Pick one layer
to own retries.**

---

## Command type

```hcl
command {
  name           = "glueetl"
  python_version = "3"
}
```

| `name` | What it is | Cost |
|---|---|---|
| `glueetl` | Spark ETL, distributed | full DPU pricing |
| `pythonshell` | plain Python, single node | 0.0625 or 1 DPU, far cheaper |
| `gluestreaming` | Spark Structured Streaming | continuous |

**`pythonshell` is the answer when a scenario describes a small,
non-distributed task** like calling an API or a light transformation.
Spinning up Spark for that wastes money.

Your jobs use `explode`, `mapPartitions`, and partitioned Parquet writes, so
`glueetl` is right, though arguably over-provisioned at a few MB a day.

---

## Job bookmarks

```hcl
# The partition comes from --ingest_date, so a rerun is an idempotent
# overwrite of one day. A bookmark would silently skip that rerun.
"--job-bookmark-option" = "job-bookmark-disable"
```

**One of the most heavily tested Glue features.**

A bookmark is state Glue persists between runs, tracking what's already been
processed so it's skipped next time. For S3 sources it tracks object keys
and timestamps.

| Option | Behavior |
|---|---|
| `job-bookmark-enable` | track state, skip processed data |
| `job-bookmark-disable` | no state, reprocess everything in scope |
| `job-bookmark-pause` | read the bookmark but don't advance it |

**Why disabled here is correct.** Idempotency already comes from a different
mechanism: `--ingest_date` scopes the read to one day, and dynamic partition
overwrite replaces exactly that day's output. Rerun the same date, get the
same result.

A bookmark would break that. Rerun after a bad transform and the bookmark
says "already processed," the job skips everything and exits 0. **Silent
no-op, the worst failure mode.**

**The rule: bookmarks and explicit partition parameters are two competing
solutions to the same problem. Use one.** Bookmarks fit append-only
incremental loads over an unbounded source. Date-parameterized overwrite
fits reprocessable batch.

---

## Metrics, logging, and arguments

```hcl
"--enable-metrics"                   = "true"
"--enable-continuous-cloudwatch-log" = "true"
"--extra-py-files" = "s3://.../scripts/roles.py"
"--raw_bucket"     = aws_s3_bucket.raw.bucket
"--TempDir"        = "s3://.../tmp/"
```

`--enable-metrics` publishes Spark metrics (executor counts, memory, shuffle
bytes) to CloudWatch.

`--enable-continuous-cloudwatch-log` streams logs **during** the run rather
than dumping at the end. Without it, debugging a job that hangs at minute 8
means waiting for it to die.

Related: `--enable-spark-ui` writes Spark event logs to S3,
`--enable-job-insights` gives AWS-generated failure analysis.

`--extra-py-files` puts additional `.py`, `.zip`, or `.egg` files on the
Python path. How `roles.py` reaches both normalize jobs without duplication.
Alternatives: `--additional-python-modules` pip-installs from PyPI at job
start, and Glue 4.0+ supports `requirements.txt`.

`--TempDir` is required. Spark shuffle spill and Glue internal staging.

**Custom arguments must be prefixed `--`** and are read via
`getResolvedOptions`.

### The deliberate absence of a default

```hcl
# --ingest_date is deliberately NOT defaulted. It is passed per run, so a
# job started without it fails instead of guessing a date.
```

Same philosophy as `os.environ["RAW_BUCKET"]` with brackets. **Fail loudly
on missing required input rather than guessing.** Defaulting to today's date
would mean a manual backfill for last Tuesday silently reprocesses today.

```hcl
# The dictionary is NOT passed as an argument: it is read from
# s3://<curated>/reference/skills/ at runtime so approving an alias on the
# VPS does not require a terraform apply.
```

**Config that changes on a different cadence than code should not be
deployed with the code.**

---

## Glue script boilerplate

```python
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ingest_date", "raw_bucket", "curated_bucket"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
...
job.commit()
```

`getResolvedOptions` parses `--arg value` pairs. Listing an argument that
wasn't passed raises immediately, which is what makes the missing
`--ingest_date` failure loud.

`GlueContext` wraps `SparkContext` and adds DynamicFrames, catalog
integration, bookmarks, and the logger.

`job.init()` / `job.commit()` is the **bookmark lifecycle**. `commit()`
advances the bookmark. With bookmarks disabled it's a formality, but
required. Note `commit()` is called even on early-exit paths. Correct: an
uncommitted job leaves bookmark state ambiguous.

---

## Dynamic partition overwrite

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

**This single line is the difference between a working pipeline and data
loss.**

| Mode | `mode("overwrite")` behavior |
|---|---|
| `static` (default) | **deletes the entire table directory**, writes new output |
| `dynamic` | deletes only the partitions present in the output |

With the default, running normalize for 2026-08-27 would wipe every
partition of `postings` and leave only that one day. Eight months of history
gone, no error.

**Top-tier exam and interview topic.** Scenario describes incremental
partition rewrites? This is the mechanism.

---

## Partition key choice

```python
# partitionBy is (ingest_date, company), NOT ingest_date alone.
.partitionBy("ingest_date", "company")
```

Only matters because **two separate jobs write the same table.**

Greenhouse and Lever normalize both write `postings`. Partitioned on
`ingest_date` alone, they'd share a directory and whichever ran second would
delete the first's output. Adding `company` gives each job disjoint
partitions, so they can even run in parallel.

**The principle: with dynamic overwrite, partition keys must be granular
enough that concurrent writers never target the same partition.**

---

## Compaction and file format

```python
.coalesce(1).write.mode("overwrite").partitionBy("ingest_date", "company")
 .option("compression", "snappy").parquet(output_path)
```

**`coalesce(1)` is the fix for the small files problem** flagged in the
ingest layer. Without it Spark writes one file per partition per task.
`coalesce(1)` gives exactly one Parquet file per `(ingest_date, company)`.

| | Shuffle | Can increase partitions |
|---|---|---|
| `coalesce(n)` | no, merges existing | no, only reduce |
| `repartition(n)` | yes, full shuffle | yes |

`coalesce` is cheaper because it avoids a shuffle. Downside: it reduces
parallelism upstream too, since the whole stage runs with fewer tasks.
Irrelevant at this volume. At scale you'd want `repartition` or
`maxRecordsPerFile`.

**Parquet plus Snappy** is the standard curated format, and the payoff for
everything in the compression table from `03-ingest`:

- **Columnar**, so Athena reads only the columns your query names
- **Self-describing**, schema travels with the data
- **Predicate pushdown**, row-group statistics let readers skip blocks
- **Splittable even with Snappy**, because Parquet's block structure handles
  the splitting

Compare with gzipped JSON in raw: row-oriented, not splittable, must be
fully parsed. Raw is for fidelity, curated is for querying.

---

## DynamicFrame vs DataFrame

```python
curated = DynamicFrame.fromDF(postings, glue_context, "curated_postings")
curated.toDF().coalesce(1).write...
```

**This round-trips and gains nothing.** Convert to DynamicFrame, immediately
convert back, then use Spark's writer. Removing both calls would change
nothing.

Know the distinction anyway:

| | DynamicFrame | DataFrame |
|---|---|---|
| Origin | AWS Glue | Spark |
| Schema | self-describing, per-record | fixed, enforced |
| Bad data | keeps ambiguous types via choice | fails or nulls |
| Key methods | `resolveChoice`, `relationalize`, `applyMapping` | full Spark SQL |
| Catalog | native integration | needs config |

**DynamicFrame's real value is `resolveChoice`** for messy semi-structured
data where a field is sometimes a string and sometimes an int. Your schema
is already enforced explicitly, so DataFrames are right and the conversion
is vestigial.

---

## Fail loud, everywhere

The strongest theme in these scripts. Every check prevents a **silent empty
result.**

```python
if not found:
    raise FileNotFoundError("... every board failed to ingest. "
                            "Refusing to treat a missing snapshot as an empty board.")

if "_corrupt_record" in raw_df.columns:
    raise ValueError(f"malformed JSON under {input_path}; refusing to write")

if exploded.where(F.col("company") == "").limit(1).count() > 0:
    raise ValueError("could not parse company= from the source path")

def require_postings_partition(bucket, ingest_date):
    """An absent partition is not an empty day."""
```

**"Missing is not empty" is the principle.** Zero rows because nobody posted
a job and zero rows because ingestion broke look identical downstream.
Distinguishing them at the boundary is the only place it's cheap.

The `_corrupt_record` check is Spark-specific and worth knowing: when
Spark's JSON reader can't parse a record it doesn't fail. It adds a
`_corrupt_record` column and moves on. Checking for that column is how you
detect parse failures.

**Exam connection:** this is what **Glue Data Quality** automates. Rules in
DQDL (`RowCount > 0`, `IsComplete "company"`, `ColumnValues "salary_min"
between 0 and 1000000`) fail or flag the job when violated. If a scenario
asks how to enforce data quality in a Glue pipeline, Glue Data Quality is
the named answer.

---

## The schema contract

```python
CURATED_COLUMNS = ["ats", "company", "ingest_date", ...]
```

Duplicated byte-for-byte in both normalize scripts, with comments saying it
must stay identical.

```python
F.lit(None).cast(TimestampType()).alias("updated_at")   # Lever has none
F.lit(False).alias("has_sections")                      # Greenhouse has none
```

Lever has no `updated_at`, but the column must exist **with the right
type**, not as an untyped null. Otherwise the two jobs produce different
Parquet schemas in one table, and Athena either fails or silently drops
rows.

**Two producers, one table, no enforcement mechanism.** The contract lives
in comments and discipline. Real options: put `CURATED_COLUMNS` in
`roles.py` which already ships to both, use a Glue Data Quality schema rule,
or define the table explicitly in the catalog so a mismatch fails.

Most likely place this pipeline breaks silently in six months.

---

## Python UDFs

```python
clean_html_udf = F.udf(clean_html, StringType())
classify_role_udf = F.udf(roles.classify_role, StringType())
```

**The standard Spark performance trap.** Every row gets serialized from the
JVM to a Python process, executed, and serialized back. Native Spark
functions run in the JVM with no crossing.

Preference order:

1. Native Spark SQL functions, fastest
2. Pandas UDFs (vectorized, Arrow-based), much faster than row-wise
3. Python UDFs, slowest

At ~1000 postings a day this doesn't matter. At 10 million it would dominate
runtime.

The code mitigates correctly:

```python
postings.cache()
row_count = postings.count()
...
postings.unpersist()
```

Spark is lazy, so `count()` and `write()` would each recompute the whole
lineage including the UDFs. `cache()` materializes once, `unpersist()`
releases. Textbook.

---

## Recovering company from the path

```python
input_path = f"s3://{raw_bucket}/raw/ats={ATS}/*/ingest_date={ingest_date}/"

with_company = raw_df.withColumn(
    "company", F.regexp_extract(F.input_file_name(), COMPANY_IN_PATH, 1)
)
```

The glob `*` at the `company=` level means **the job never needs to know
which companies exist.** Add a company to `companies.yml` and normalize
picks it up with no code change. Config-as-data paying off again.

> `input_file_name()` is resolved during the scan, so the company column has
> to be attached before anything reshapes the rows.

After an `explode` or a shuffle, the row's connection to its source file is
gone. Column must be added first.

The path starts below the partition root, so Spark can't infer `company` as
a partition column. Hence the regex. `extract_skills.py` solves the same
problem with `basePath`.

---

## Why one run per ATS, not per company

> A per-company run spent roughly 90 seconds of a 120 second job booting a
> Spark cluster, so 22 companies cost 22 cluster startups to process a few
> MB.

**75% of runtime was cluster boot.** Same shape as the crawler
consolidation. Both are "amortize a fixed startup cost over more work."

**General lesson:** for distributed compute, batch size should be large
enough that startup is a small fraction of runtime. Lambda has the opposite
profile, tens of milliseconds of overhead, so fine-grained invocation is
fine there.

---

## The escaping difference

```python
# Greenhouse: DOUBLE-escaped -> unescape, strip, unescape
# Lever:      SINGLE-escaped -> strip, unescape
```

Two ATSes, two escaping conventions, and the order is not interchangeable.
The comment explains both failure directions, which is exactly what a future
reader needs.

**Normalization is where source-specific weirdness gets absorbed.**
Downstream, `posting_skills` and the dashboard see one clean
`description_text` and never learn that Greenhouse double-escapes.

The evidence in the comments ("307 `&nbsp;`, 79 `&amp;`, 54 `&mdash;` still
sitting in the text", "0 newlines in a 548 KB snapshot") is the right way to
document a decision. Verified, not assumed.

---

## roles.py: rule-based classification

```python
ROLE_PATTERNS = {
  'product':   r'product manager|...',
  'ai-ml':     r'\bai engineer|...',
  ...
  'software':  r'software engineer|...|\bengineer\b|...',
}
```

**Dict order is precedence, first match wins.** Relies on Python 3.7+
guaranteeing insertion order. Load-bearing behavior resting on a language
guarantee, correctly documented.

`software` last because it holds the bare `\bengineer\b` catch-all. Move it
up and "Sales Engineer" classifies as software.

```python
if is_eng and category in BUSINESS:
    continue
```

Two-pass logic: engineering titles skip the business-function layer, so
"Risk Systems Engineer" lands in software, not risk-compliance. The stated
policy, **function beats domain for non-engineering roles**, has to be
applied consistently or the dimension is noise.

**Rule-based vs ML.** This is deterministic, auditable, debuggable, needs no
training data. The AWS-native alternative is **Amazon Comprehend** for entity
extraction and custom classification, or Bedrock for an LLM approach. If a
scenario asks about extracting entities or sentiment from unstructured text
at scale, Comprehend is the expected answer. For a fixed taxonomy where you
must explain every decision, regex wins.

---

## Broadcast variables

```python
broadcast_dictionary = sc.broadcast(dictionary)
```

**Ships a read-only value to every executor once**, rather than serializing
it with every task. Classic Spark optimization for lookup tables and small
reference data.

More commonly tested: a **broadcast join** (map-side join) sends a small
table to every executor so a large table joins without a shuffle. Spark does
this automatically below `spark.sql.autoBroadcastJoinThreshold`, default 10
MB. Scenario describes a slow join between a huge fact table and a small
dimension? Broadcast join.

---

## The pickle failure

Most technically interesting comment in the repo.

```
AttributeError: Can't get attribute 'SkillMatcher' on
<module 'pyspark.daemon' ...>
```

> `sc.broadcast` serializes with the PLAIN pickler, which stores instances
> by reference to their defining module. This script is `__main__` on the
> driver and `pyspark.daemon` on the worker, so the class is unresolvable
> there. Closures escape this because PySpark cloudpickles them BY VALUE.

Unpacked:

- **Standard `pickle` stores a class instance by reference:** "instance of
  `SkillMatcher` from module `__main__`." On the executor the script isn't
  `__main__`, it's `pyspark.daemon`. The reference doesn't resolve.
- **`cloudpickle` serializes by value**, embedding the actual code. PySpark
  uses it for closures and UDFs, which is why a function defined in your
  driver script works on executors.
- **Broadcasts use plain pickle.** Different code path, different behavior.

Fix: broadcast only plain `str`/`int`/`None` tuples, rebuild the compiled
regexes on the executor inside `mapPartitions`.

You won't be tested on this. But "why does my Spark job fail only on the
executors" is a real debugging skill and this is the canonical version.

---

## mapPartitions

```python
rows = projected.rdd.mapPartitions(make_partition_extractor(broadcast_dictionary))
```

| | Calls your function |
|---|---|
| `map` | once per row |
| `mapPartitions` | once per partition, receives an iterator |

Amortizes setup cost. Here `compile_entries()` compiles 93 regexes once per
partition instead of once per row. Standard uses: opening a DB connection,
loading a model, compiling patterns.

The closure wrapper is deliberate, so PySpark cloudpickles it by value and
the closure captures the Broadcast handle rather than a compiled pattern.

---

## basePath and partition type inference

```python
postings = spark.read.option(
    "basePath", f"s3://{curated_bucket}/postings/"
).parquet(input_path)
```

Reading from `postings/ingest_date=2026-08-27/` starts below the partition
root, so Spark can't tell `ingest_date` was a partition column. `basePath`
tells it where the table root is.

```python
# Spark infers partition column types and would hand back a date for
# 2026-08-29, while the column is a string everywhere else.
F.lit(ingest_date).alias("ingest_date")
```

**Spark type-infers partition values from their string form.** `2026-08-29`
looks like a date so Spark makes it a `DateType`. Everywhere else it's a
string. Using the argument keeps the type consistent. Global off switch is
`spark.sql.sources.partitionColumnTypeInference.enabled=false`.

---

## The bridge table

```python
POSTING_SKILLS_SCHEMA = StructType([
    StructField("ingest_date", ...), StructField("company", ...),
    StructField("ats_job_id", ...), StructField("skill_id", ...),
    StructField("canonical_name", ...), StructField("category", ...),
])
```

One row per `(posting, skill)`. This is the **bridge table** resolving the
many-to-many between postings and skills.

```
postings          (fact)       ats_job_id, title, salary, role_category
posting_skills    (bridge)     ats_job_id -> skill_id
skills            (dimension)  lives on the VPS, exported to reference/
```

Textbook dimensional modeling. The alternative, an array column of skills on
`postings`, would make "how many postings mention Python" awkward. The
bridge makes it a `GROUP BY`.

`canonical_name` and `category` are **denormalized** into the bridge rather
than requiring a join back to a skills dimension. Deliberate: Athena joins
cost scan volume, and the values are small and stable. Classic analytics
tradeoff, storage is cheap and joins are expensive.

---

## Presence, not counts

> The inner loop breaks after the first surviving hit for a skill on purpose
> -- Python appearing five times in one description measures the writer, not
> demand.

**A semantic decision, not a technical one, and the right call.** Term
frequency in job descriptions measures verbosity. Presence measures demand.
Once you're not counting, `break` on first match is free performance.

---

## Guard and deny windows

```python
GUARD_WINDOW = 80
window = text[max(0, m.start() - GUARD_WINDOW) : m.end() + GUARD_WINDOW]
if deny and deny.search(window): continue
if guard and not guard.search(window): continue
```

Locality-scoped disambiguation. Document-scope matching would break "Java"
on any posting mentioning JavaScript, and would let "R" be validated by the
word "statistics" a thousand characters away.

```python
# NOT \b. \b is defined against \w, so it fails on aliases that end in
# punctuation -- C++, C#, .net -- which are exactly the ones that matter.
re.compile(r"(?<![A-Za-z0-9])" + re.escape(alias) + r"(?![A-Za-z0-9])", re.I)
```

`\b` is a boundary between `\w` and non-`\w`. `+` is already non-word, so
`\bC\+\+\b` never matches. Lookarounds with explicit character classes fix
it.

---

## Two systems, one contract

> This is a PORT of the VPS matcher. The two systems run the same dictionary
> over the same postings and must agree on the same posting, so the three
> semantics below are copied deliberately and must not be "improved."

The hardest kind of constraint: two independent implementations that must
produce identical output. Every "obvious improvement" is a bug, because it
creates disagreement.

> A Databricks posting therefore still emits skill_id 23, exactly as the VPS
> emits it.

Knowingly emitting a false positive to preserve agreement. A real
engineering tradeoff, documented rather than silently resolved.

---

## The ordering dependency

> ExtractSkills runs BEFORE CrawlCurated in the state machine, so today's
> partition is by definition not yet cataloged.

Confirms the pipeline order and explains why neither job can depend on the
catalog. **This is exactly the coupling the raw crawler was removed to
avoid**, and it would have been reintroduced if extract-skills read the
catalog.

---

# Exam angles

**Domain 1, Ingestion and Transformation (34%)**
- Glue as managed Spark, `glueetl` vs `pythonshell` vs `gluestreaming`
- **Job bookmarks: three modes, and when disabling is correct**
- **Dynamic vs static partition overwrite**
- Crawlers for schema discovery, and when you don't need one
- SerDe selection, JSON Lines vs JSON arrays, `multiLine`
- Parquet + Snappy for curated, splittability, columnar benefits
- `coalesce` vs `repartition`, compaction
- DynamicFrame vs DataFrame, `resolveChoice`
- Python UDF cost, Pandas UDFs, native functions
- Glue Data Quality for rule-based validation
- Comprehend as the managed alternative to rule-based text extraction

**Domain 2, Data Store Management (26%)**
- Glue Data Catalog as a Hive-compatible metastore shared across engines
- **Partition projection vs crawler**, cost and latency tradeoff
- Catalog is regional
- Fact / bridge / dimension modeling, denormalization tradeoffs
- Multi-zone lake (raw / curated), why separate buckets
- Schema evolution: `UPDATE_IN_DATABASE` + `LOG` deletes

**Domain 3, Operations (22%)**
- DPU-hour billing, worker types, 1-min job minimum vs 10-min crawler
- Glue timeout in minutes, Lambda in seconds
- Retry ownership: one layer only
- Continuous CloudWatch logging, per-job log groups
- Cluster boot as fixed cost, batching to amortize it

**Domain 4, Security (18%)**
- **Trust policy vs permissions policy**
- **Bucket ARN vs object ARN**, `ListBucket` vs `GetObject`
- **Separation of duties: a role per job, not per pipeline**
- `DeleteObject` required for Spark overwrite
- Glue fetches the script using the job role
- AWS managed vs customer managed vs inline

---

# Decision rules

```
Small non-distributed task        -> pythonshell, not glueetl
Spark / distributed transform     -> glueetl
Streaming source                  -> gluestreaming
Append-only incremental load      -> job bookmarks enabled
Date-parameterized reprocessing   -> bookmarks disabled + dynamic overwrite
Rewriting one partition           -> partitionOverwriteMode = dynamic
Concurrent writers, same table    -> partition keys must be disjoint
Many small output files           -> coalesce or repartition before write
Curated / query layer             -> Parquet + Snappy
Messy semi-structured types       -> DynamicFrame + resolveChoice
Enforced schema                   -> plain DataFrame
Rule-based validation in Glue     -> Glue Data Quality (DQDL)
Entity extraction from text       -> Comprehend
Schema unknown or changing        -> crawler
Schema known, partitions regular  -> partition projection
Nothing queries it with SQL       -> no crawler at all
Multiple targets, same run        -> one crawler, multiple s3_target blocks
Expensive per-row setup           -> mapPartitions
Small lookup table on executors   -> broadcast
Huge fact + small dimension join  -> broadcast join
Spark mode("overwrite")           -> grant s3:DeleteObject
Component must not write a table  -> separate IAM role
```

---

# Gaps

- [ ] Implement partition projection, drop the recurring crawl
- [ ] Add `recrawl_policy = CRAWL_NEW_FOLDERS_ONLY`
- [ ] Tighten the normalize role from `curated/*` to specific prefixes
- [ ] Enforce `CURATED_COLUMNS` mechanically instead of by comment
- [ ] Remove the vestigial DynamicFrame round-trip
- [ ] Check whether `data "aws_region" "current"` is actually used
- [ ] Delete the orphaned `jobmarket.ats_greenhouse` table
- [ ] Glue Data Quality: DQDL syntax and how rules fail a job
- [ ] Pandas UDFs, when they're worth the change
- [ ] Lake Formation as a permission layer over the catalog
- [ ] Glue Streaming and how it differs from batch
- [ ] EMR vs Glue: when EMR wins