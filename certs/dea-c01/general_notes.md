# DEA-C01 Reference

**Exam facts:** 65 questions, 130 minutes, scaled 100 to 1000, pass at 720 (~72%). 15 unscored questions mixed in. $150, or $75 with the 50% voucher from a prior cert. 14-day wait between attempts.

**Domain weights:**

| #  | Domain                            | Weight |
|-----|-----------------------------------|--------|
| 1   | Data Ingestion and Transformation | 34%    |
| 2   | Data Store Management             | 26%    |
| 3   | Data Operations and Support       | 22%    |
| 4   | Data Security and Governance      | 18%    |

## Contents

- [S3 and storage](#s3-and-storage)
  - [Storage classes](#storage-classes)
  - [Encryption](#encryption)
  - [Versioning and lifecycle](#versioning-and-lifecycle)
  - [Access control](#access-control)
- [Kinesis and streaming ingestion](#kinesis-and-streaming-ingestion)
  - [Picking the service](#picking-the-service)
  - [Data Streams shards](#data-streams-shards)
  - [Partition keys and hot shards](#partition-keys-and-hot-shards)
  - [Enhanced fan-out](#enhanced-fan-out)
  - [IteratorAge](#iteratorage)
  - [Firehose](#firehose)
  - [MSK](#msk)
  - [Streaming concepts](#streaming-concepts)
- [DMS](#dms)
- [Lambda and ingest](#lambda-and-ingest)
- [File formats and layout](#file-formats-and-layout)
  - [Partitioning](#partitioning)
  - [Format comparison](#format-comparison)
  - [Compression](#compression)
- [Glue](#glue)
  - [The catalog](#the-catalog)
  - [Jobs](#jobs)
  - [Job bookmarks](#job-bookmarks)
  - [Spark](#spark)
  - [Glue Data Quality](#glue-data-quality)
- [Athena](#athena)
- [Redshift](#redshift)
  - [Cluster types](#cluster-types)
  - [Distribution styles](#distribution-styles)
  - [DISTKEY vs SORTKEY](#distkey-vs-sortkey)
  - [VACUUM and ANALYZE](#vacuum-and-analyze)
  - [Loading and unloading](#loading-and-unloading)
  - [Concurrency and WLM](#concurrency-and-wlm)
  - [Spectrum and sharing](#spectrum-and-sharing)
  - [Encryption and security](#encryption-and-security)
- [Step Functions and orchestration](#step-functions-and-orchestration)
  - [Standard vs Express](#standard-vs-express)
  - [Integration patterns](#integration-patterns)
  - [Map and Parallel](#map-and-parallel)
  - [Data flow](#data-flow)
  - [Error handling](#error-handling)
  - [Scheduling](#scheduling)
  - [MWAA (Managed Airflow)](#mwaa-managed-airflow)
- [EMR](#emr)
- [Monitoring and operations](#monitoring-and-operations)
- [IAM, security, and governance](#iam-security-and-governance)
  - [Lake Formation](#lake-formation)
  - [Other governance services](#other-governance-services)
- [Data modeling](#data-modeling)
  - [Slowly changing dimensions](#slowly-changing-dimensions)
  - [Schemas](#schemas)
- [AI services for unstructured input](#ai-services-for-unstructured-input)
- [Quick decision index](#quick-decision-index)
- [Terraform](#terraform)

---

## S3 and storage

### Storage classes

| Class                      | Access pattern         | Retrieval        | Min duration | Notes                                                         |
|----------------------------|------------------------|------------------|--------------|---------------------------------------------------------------|
| Standard                   | Frequent               | Instant          | None         | No retrieval fee, priciest per GB                             |
| Intelligent-Tiering        | Unknown or changing    | Instant          | None         | Auto-tiers. Small per-object monitoring fee, no retrieval fee |
| Standard-IA                | ~Monthly               | Instant          | 30 days      | 3+ AZs. Retrieval fee applies                                 |
| One Zone-IA                | ~Monthly, reproducible | Instant          | 30 days      | Single AZ, ~20% cheaper than Standard-IA                      |
| Express One Zone           | Very frequent          | Single-digit ms  | None         | Single AZ, most expensive. ML training, hot analytics         |
| Glacier Instant Retrieval  | ~Quarterly             | Instant          | 90 days      | Cheaper storage than IA, higher retrieval fee                 |
| Glacier Flexible Retrieval | ~Yearly                | Minutes to hours | 90 days      | Yearly compliance backups                                     |
| Glacier Deep Archive       | Almost never           | ~12 hours        | 180 days     | Cheapest storage in AWS                                       |

**Decision phrases.** “Unknown / unpredictable / changing access pattern” points to Intelligent-Tiering. “Can be regenerated” points to One Zone-IA. “12 hours is acceptable” points to Deep Archive. Early deletion still bills the full minimum duration.

### Encryption

| Option      | Who holds the key            | CloudTrail audit | Cost                  | When it wins                                          |
|-------------|------------------------------|------------------|-----------------------|-------------------------------------------------------|
| SSE-S3      | AWS, invisible               | No               | Free                  | Default. No compliance requirement                    |
| SSE-KMS     | AWS KMS, you control it      | **Yes**          | Per key + per request | Audit trail, key rotation, cross-account key policies |
| SSE-C       | You, sent with every request | No               | Free                  | You must hold the keys yourself. Rare                 |
| Client-side | You, before upload           | No               | Free                  | AWS never sees plaintext                              |

**The rule:** audit trail, key rotation, or granular key access control points to KMS. Everything else points to SSE-S3.

**S3 Bucket Keys** cut KMS request volume by *up to* 99% using a short-lived bucket-level key. The fix when a job reading millions of small KMS-encrypted objects hits KMS throttling, without giving up the key or the audit trail.

At rest and in transit are separate controls. TLS is enforced by a bucket policy denying `aws:SecureTransport` false, not by any encryption setting.

### Versioning and lifecycle

Three states: Unversioned, Enabled, Suspended. **Unversioned is a one-way door.** Once enabled you can only suspend.

Deleting writes a **delete marker** on top. Nothing is removed, and you keep paying for every version underneath.

Lifecycle actions:

- `expiration` deletes current versions after N days
- `noncurrent_version_expiration` deletes old versions. Versioning without this is unbounded growth
- `transition` / `noncurrent_version_transition` move to another class
- `abort_incomplete_multipart_upload` cleans up orphaned parts you pay for and cannot see in a listing

### Access control

`block_public_acls` rejects **new** public ACLs. `ignore_public_acls` neutralizes ones **already set**. Same split for `block_public_policy` and `restrict_public_buckets`. Account-level and bucket-level disagree, and **the more restrictive wins.**

ARN forms, and the distinction that costs people points:

    arn:aws:s3:::mybucket      s3:ListBucket   (bucket-level, no /*)
    arn:aws:s3:::mybucket/*    s3:GetObject    (object-level, needs /*)

S3 ARNs carry no region and no account, because bucket names are globally unique. Almost every other service’s ARN embeds both.

`force_destroy = true` lets Terraform delete a non-empty bucket. **MFA Delete** gates permanent version deletion behind a second factor. **Object Lock** in compliance mode is stronger WORM. It requires versioning, and can now be enabled on an existing bucket (it used to be creation-time only, so older material says otherwise).

Strong read-after-write consistency for all operations since December 2020.

## Kinesis and streaming ingestion

### Picking the service

|            | Data Streams                  | Firehose                       | Managed Flink                   |
|------------|-------------------------------|--------------------------------|---------------------------------|
| What it is | Durable, replayable log       | Managed delivery pipeline      | Stateful stream processing      |
| You manage | Shards (or on-demand)         | Nothing                        | KPUs, app code                  |
| Consumers  | Many, independent, replayable | One destination                | Reads a stream, writes anywhere |
| Latency    | ~200 ms                       | Seconds to minutes (buffered)  | Sub-second                      |
| Replay     | Yes, up to 365 days           | **No**                         | Via source retention            |
| Transform  | Your consumer code            | Lambda, plus format conversion | Full SQL / Java / Python Flink  |
| Ordering   | Per shard                     | None guaranteed                | Per key                         |

**The decision that gets tested:** if the question says “multiple applications consume the same data independently,” or “replay the last N hours,” it is Data Streams. If it says “no code to manage,” “no shards to size,” or “just land it in S3 / Redshift / OpenSearch,” it is Firehose. If it says “windowed aggregation,” “join two streams,” or “anomaly detection over a time window,” it is Managed Flink.

Firehose can read *from* a Data Stream. That combination is common: Data Streams for durability and replay, Firehose for the hands-off S3 delivery.

### Data Streams shards

Per shard, provisioned mode:

| Direction        | Limit                                                  |
|------------------|--------------------------------------------------------|
| Write            | 1 MB/s **or** 1,000 records/s                          |
| Read (shared)    | 2 MB/s or 2,000 records/s, shared across all consumers |
| GetRecords calls | 5 per second per shard                                 |

Exceeding write throughput gives `ProvisionedThroughputExceededException`. The record payload cap is 10 MiB (it was 1 MiB for years, so older practice questions still say 1 MB).

**Retention:** 24 hours default, extendable to 365 days. Extended retention past 24 hours costs extra.

**Capacity modes:**

|          | Provisioned                                | On-demand                                 |
|----------|--------------------------------------------|-------------------------------------------|
| You set  | Shard count                                | Nothing                                   |
| Scaling  | Manual resharding, or your own auto-scaler | Automatic                                 |
| Cost     | Per shard-hour, cheap if well sized        | Per GB, ~5x more per unit at steady state |
| Best for | Predictable, steady volume                 | Spiky, unpredictable, or new workloads    |

On-demand starts at 4 MB/s write and 8 MB/s read and doubles capacity within 15 minutes of a traffic increase. It can only handle 2x the previous 30-day peak, so a true cold-start spike can still throttle.

### Partition keys and hot shards

The partition key is hashed to pick a shard. **Same key always goes to the same shard**, which is how you get ordering per key. That is also how you create a hot shard.

A low-cardinality key (`country`, `event_type`, or worst of all a constant) piles most traffic onto one shard while the others idle. Adding shards does not help, because the hash still sends that key to one place. The fix is a **higher-cardinality key** (user ID, device ID, or a natural key with a random suffix if you do not need ordering).

Symptom on the exam: “one shard is throttling while the others are underutilized.” Answer: change the partition key, not the shard count.

**Resharding** is split (one shard into two) and merge (two adjacent into one). Both create new shards and mark the parents closed. Consumers must drain a parent fully before reading its children, or ordering breaks.

### Enhanced fan-out

Standard consumers share the shard’s 2 MB/s read budget by polling `GetRecords`. Five consumers on one shard means each effectively gets 400 KB/s, and polling adds latency.

Enhanced fan-out gives each registered consumer its **own dedicated 2 MB/s per shard**, pushed over HTTP/2, dropping propagation delay to ~70 ms from ~200 ms.

Limit is 20 registered consumers per stream (50 on On-Demand Advantage). It costs extra per consumer-shard-hour, so it is the answer when the question mentions **multiple consumers competing for throughput** or **lowest latency**, not by default.

### IteratorAge

`GetRecords.IteratorAge` (or `MillisBehindLatest`) is how far behind the newest record your consumer is. **Rising IteratorAge means the consumer cannot keep up**, not that the producer is too fast.

Causes, in order of exam likelihood: consumer processing too slow (fix by adding shards so more Lambda instances run in parallel, or by making the handler faster), a poison-pill record blocking its shard (by default a failing batch retries until the record expires, which can be days), or too few consumer instances.

The poison-pill fix is to **bound** the retries, not raise them: lower `MaximumRetryAttempts`, set `MaximumRecordAgeInSeconds`, turn on `BisectBatchOnFunctionError` to isolate the bad record, and send the discarded batch to an **on-failure destination** (SQS or SNS).

For Lambda consumers, concurrency is **one invocation per shard** by default, so shard count is your parallelism dial. `ParallelizationFactor` (up to 10) runs multiple batches per shard concurrently while preserving per-partition-key order.

If IteratorAge approaches the retention period you are about to lose data permanently. Alarm on it.

### Firehose

Fully managed. No shards to size, no consumer to write. Delivers to S3, Redshift, OpenSearch, Splunk, and third-party HTTP endpoints.

**Buffering is the core concept.** Two settings, and it flushes on **whichever hits first**:

- **Buffer size:** 1 to 128 MB for S3
- **Buffer interval:** 0 to 900 seconds (zero buffering was added in Dec 2023; the classic minimum was 60 seconds)

Small buffer means lower latency and many small files. Large buffer means fewer, better-sized files and higher latency. That tradeoff is the whole question when the exam complains about “too many small files in S3.”

**Format conversion:** Firehose converts JSON to Parquet or ORC inline, using a Glue table for the schema. Input must be JSON. If you need to convert something else, transform it with Lambda first.

**Dynamic partitioning** builds `key=value` prefixes from record content using JQ expressions or a Lambda. Without it every record lands in the same date-based prefix. Default cap is 500 active partitions per stream.

**Lambda transformation** must return each record with a `recordId`, a `result` of `Ok` / `Dropped` / `ProcessingFailed`, and base64 `data`. Failed records go to an S3 error prefix, not to the destination.

**Retry and backup:** on delivery failure Firehose retries for up to 24 hours (DirectPut), then drops. Enable S3 backup to keep the raw source records regardless.

Firehose’s Redshift delivery is really S3 plus an automatic `COPY`, so it always lands in S3 first. **Redshift streaming ingestion** is the direct path: Redshift consumes a Kinesis Data Stream or MSK topic straight into a materialized view with no S3 staging. That’s the answer for lowest-latency stream-into-warehouse.

### MSK

Managed Kafka. The answer when the question says **Kafka** explicitly, when you are migrating an existing Kafka workload, or when you need Kafka-specific features (log compaction, consumer groups, the Kafka API).

|               | MSK                        | Kinesis Data Streams     |
|---------------|----------------------------|--------------------------|
| API           | Kafka                      | AWS proprietary          |
| Retention     | Unlimited (tiered storage) | 365 days max             |
| Ordering unit | Partition                  | Shard                    |
| Scaling       | Add brokers / partitions   | Add shards, or on-demand |
| Ops burden    | Higher, even managed       | Lower                    |

**MSK Serverless** removes broker sizing. **MSK Connect** runs Kafka Connect connectors managed.

### Streaming concepts

**Windowing:**

| Window            | Shape                                  | Use                                                |
|-------------------|----------------------------------------|----------------------------------------------------|
| Tumbling          | Fixed size, no overlap                 | “Count per 5-minute bucket”                        |
| Sliding (hopping) | Fixed size, overlapping, fixed advance | “Rolling 5-minute average, updated every minute”   |
| Session           | Gap-defined, variable length           | “Group activity until the user is idle 30 minutes” |

**Event time vs processing time.** Event time is when it happened, processing time is when you saw it. Anything that must be correct despite network delay or replay must use event time.

**Watermarks** are how event-time processing decides a window is done. A watermark says “I believe I have seen everything up to time T.” Records arriving after their window’s watermark are **late data**, and are either dropped or routed to a side output. Longer allowed lateness means more correctness and more latency and more state held in memory. That tradeoff is the question.

**Delivery semantics:**

- **At-most-once:** fire and forget. Can lose data.
- **At-least-once:** retried until acked. Can duplicate. This is the default nearly everywhere, including Firehose and Step Functions Express.
- **Exactly-once:** effectively-once, achieved with checkpointing plus idempotent or transactional sinks. Flink does this via checkpoints; the sink must cooperate.

**Managed Flink specifics:** one **KPU** is 1 vCPU, 4 GB memory, 50 GB disk. KPUs = `Parallelism` / `ParallelismPerKPU` (default 1). Checkpoints are automatic fault tolerance (default 60-second interval); **snapshots** are manual, durable state you restore from on update or rollback. Exactly-once during updates and scaling depends on snapshots being enabled.

## DMS

Migrates databases into AWS, and into S3 or Redshift as pipeline sources.

**Three task types:**

- **Full load:** one-time copy of existing data. The only type that migrates views.
- **CDC only:** ongoing changes after the target is already loaded.
- **Full load + CDC:** initial copy then continuous replication. The default answer for “migrate with minimal downtime.”

**Homogeneous vs heterogeneous.** Same engine to same engine is a straight DMS task. Different engines (Oracle to PostgreSQL) need **AWS SCT** or DMS Schema Conversion to convert schema and code first; DMS moves only the data.

**DMS Serverless** removes replication instance sizing. **Data validation** compares source and target row by row after load. CDC latency is seconds to minutes, not real-time.

Targets include S3 (Parquet or CSV, which is how DMS feeds a lake) and Redshift. A common lake pattern is DMS full load + CDC landing Parquet into a raw S3 prefix, then Glue merging into curated tables.

## Lambda and ingest

**900 seconds** is the hard timeout ceiling. This is the exam’s favourite way to eliminate Lambda. Over 15 minutes points to Glue, Fargate, or Batch.

**Memory is the CPU dial.** CPU scales proportionally and reaches one full vCPU around **1769 MB**. Billing is GB-seconds, so raising memory can make a CPU-bound function *cheaper* by cutting duration. Does nothing for IO-bound work.

**Reserved concurrency** caps simultaneous executions to protect a downstream system. **Provisioned concurrency** pre-warms instances to kill cold starts. Opposite jobs, commonly swapped on the exam.

`handler = "handler.handler"` reads as *filename without extension*, then *function inside it*. `main.lambda_handler` means `main.py` containing `def lambda_handler`.

Package limits: **250 MB unzipped** for a zip, **10 GB** for a container image.

Initialize clients like `boto3.client("s3")` at **module scope**, not inside the handler, so they are reused across warm invocations.

VPC-attached Lambda loses its default internet path. Public API needs a **NAT gateway**. AWS service needs a **VPC endpoint**. It also needs `AWSLambdaVPCAccessExecutionRole` for ENI management. `AWSLambdaBasicExecutionRole` is the CloudWatch Logs minimum every function needs.

Environment variables are encrypted at rest but readable by anyone with `lambda:GetFunction`. Secrets go in Secrets Manager or SSM Parameter Store.

`source_code_hash` is what gives Terraform something that changes when your Python changes. Without it Terraform compares only `.tf` files and silently deploys nothing.

## File formats and layout

### Partitioning

**Hive-style partitioning** is the `key=value` prefix convention. Glue and Athena expose each segment as a queryable column, so filters prune whole prefixes. Partition keys should be **low cardinality** and match your WHERE clauses. One partition per row means catalog metadata explodes.

### Format comparison

| Format  | Layout    | Splittable         | Schema evolution         | Best for                                 |
|---------|-----------|--------------------|--------------------------|------------------------------------------|
| Parquet | Columnar  | Yes                | Add columns well         | Analytics reads, curated zone            |
| ORC     | Columnar  | Yes                | Add columns well         | Hive-heavy stacks, best compression      |
| Avro    | Row       | Yes                | **Best**, full evolution | Streaming, write-heavy, evolving schemas |
| JSON    | Row, text | Only as JSON Lines | Anything goes            | Raw landing zone, APIs                   |
| CSV     | Row, text | Yes                | None                     | Interchange, nothing else                |

Columnar wins when you select few columns from many rows. Row wins when you write whole records or read most columns. **Avro is the answer whenever the question stresses schema evolution or write-heavy streaming.**

### Compression

| Codec  | Splittable alone | Ratio   | Speed   |
|--------|------------------|---------|---------|
| gzip   | No               | High    | Slow    |
| bzip2  | **Yes**          | Highest | Slowest |
| snappy | No\*             | Low     | Fastest |
| lz4    | No\*             | Low     | Fastest |
| zstd   | No\*             | High    | Fast    |

\* Splittable *inside Parquet*, because Parquet’s block structure handles the splitting. That is why Parquet + Snappy is the curated-zone default.

Gzip’s lack of splittability means one worker reads an entire file regardless of size.

**JSON Lines vs arrays.** Most AWS tooling assumes one JSON object per line. A bare multi-line array collapses to a single row with no error. Spark’s `multiLine` option parses the file as one document and expands the array.

**Small files** are the recurring performance villain: each one costs a separate S3 request and a task launch. Fix by increasing Firehose buffer size, compacting with a Glue job, or `coalesce()` before write. Target roughly 128 MB to 1 GB per file.

## Glue

### The catalog

A **metadata store**, not a data store. Hive Metastore compatible. **Regional**, so a catalog in us-east-1 is invisible to Athena in us-west-2.

**Crawlers** discover schema. If nothing queries the data with SQL, you don’t need one. Billed per DPU-hour with a **10-minute minimum per run** (Glue *jobs* have a 1-minute minimum, different numbers). Put multiple `s3_target` blocks on one crawler rather than running several: each additional crawler is another 10-minute minimum.

`delete_behavior = LOG` is the safe default. `DELETE_FROM_DATABASE` would drop your table when a transient failure shows an empty prefix.

**Partition projection** computes partition locations from a declared rule instead of scanning S3. Free, instant, and scales with partition count. Use it when the pattern is predictable; use a crawler when the structure is unknown.

### Jobs

| Command type    | What it is                            |
|-----------------|---------------------------------------|
| `glueetl`       | Spark ETL, distributed                |
| `pythonshell`   | Single-node Python, fraction of a DPU |
| `gluestreaming` | Spark Structured Streaming            |

Worker types:

| Type   | DPU  | vCPU | Memory |
|--------|------|------|--------|
| G.025X | 0.25 | 2    | 4 GB   |
| G.1X   | 1    | 4    | 16 GB  |
| G.2X   | 2    | 8    | 32 GB  |
| G.4X   | 4    | 16   | 64 GB  |
| G.8X   | 8    | 32   | 128 GB |

`timeout = 10` on a Glue job means **10 minutes**. Lambda timeouts are in seconds. Retry logic should live in exactly **one layer**: Glue’s `max_retries` plus a Step Functions Retry means a transient failure costs up to nine runs.

**Flex execution** runs non-urgent jobs on spare capacity at a discount, with variable start time and no SLA. The answer for “reduce cost of a nightly job where runtime doesn’t matter.”

**Auto Scaling** adds and removes workers during the run based on demand, instead of you sizing for peak.

### Job bookmarks

| Option                 | Behavior                                 |
|------------------------|------------------------------------------|
| `job-bookmark-enable`  | Track state, skip data already processed |
| `job-bookmark-disable` | No state, reprocess everything in scope  |
| `job-bookmark-pause`   | Read state but don’t advance it          |

Bookmarks and date-parameterized reprocessing solve the same problem. **Use one.** Enable both and a rerun after a bad transform silently skips everything and exits 0.

### Spark

`partitionOverwriteMode = dynamic` makes `mode("overwrite")` replace **only** the partitions present in the output. The **static** default deletes the entire table directory first, with no error. Concurrent writers to one table need partition keys granular enough to never collide.

Spark overwrite is list-delete-write, so it needs `s3:DeleteObject`, not just `PutObject`.

`coalesce(n)` merges without a shuffle but can only reduce. `repartition(n)` shuffles, can increase, and rebalances skew.

**Predicate pushdown** is the row-group statistics letting a reader skip blocks that can’t match a filter. **Projection pushdown** is reading only the named columns. Both are why Parquet is fast.

`resolveChoice` is the DynamicFrame method for a field that’s sometimes a string and sometimes an int. `_corrupt_record` is the column that appears when Spark’s JSON reader fails to parse. It does *not* throw.

`mapPartitions` calls your function once per partition with an iterator, amortizing setup cost. `basePath` tells Spark where the partition root is when reading below it. `autoBroadcastJoinThreshold` defaults to **10 MB**.

**DynamicFrame vs DataFrame:** DynamicFrame handles schema inconsistency and self-describing records, and has `resolveChoice`, `relationalize`, and the Glue transforms. DataFrame is standard Spark, faster, and has the full SQL API. Convert with `.toDF()` / `.fromDF()`. Use DynamicFrame at the messy edges and DataFrame for the real work.

**Data skew** shows up as one task running far longer than the rest. Fix with salting the join key, broadcasting the small side, or `repartition`.

### Glue Data Quality

Enforces rules in **DQDL**, e.g. `RowCount > 0`, `IsComplete "company"`, `Completeness "col" > 0.95`, `ColumnValues "status" in ["A","B"]`.

Can run on a catalog table (scheduled) or inside an ETL job (as a step). On failure it can fail the job or just publish results. It emits to CloudWatch and EventBridge, which is how you alert on it. Recommendation mode generates a starter ruleset from your data.

## Athena

**Billing is bytes scanned from S3: $5.00 per TB**, 10 MB minimum per query. Failed queries aren’t charged. That single fact drives every optimization.

Scan reduction, in order of impact: **partitioning** (skip prefixes entirely), **Parquet** (read only named columns), **compression** (fewer bytes on disk). Views do *not* reduce scanning, they’re saved SQL and the underlying query still runs.

**CTAS** (`CREATE TABLE AS SELECT`) converts query output into a new table in a chosen format in one statement. No Glue job needed. Max 100 partitions per statement, target location must be empty. `INSERT INTO` appends beyond that limit.

**Federated Query** uses Lambda connectors to join S3 against DynamoDB, RDS, Redshift, CloudWatch Logs and more, without moving data.

**Workgroups** are the isolation boundary for cost, result location, encryption, and scan limits. `enforce_workgroup_configuration = true` makes those settings binding rather than a default a client can override. `bytes_scanned_cutoff_per_query` kills one runaway query; **data usage controls** watch aggregate spend over a window and can alert or disable the workgroup.

**Query results are data.** Every SELECT writes a CSV to S3 and must be encrypted separately from the source. This is the classic “auditor asks about sensitive data” gap.

**Athena engine v3** is the current one. **Athena for Spark** runs notebooks. **ACID / time travel** needs Iceberg tables.

**Service selection:**

- Intermittent, ad-hoc, no idle cost: **Athena**
- Constant heavy concurrent querying: **Redshift**
- Redshift cluster needing to query S3: **Redshift Spectrum**
- Spark, Hive, custom frameworks: **EMR**
- Business dashboards without re-scanning S3: **QuickSight + SPICE**

## Redshift

### Cluster types

|          | DC2                                | RA3                                  | Serverless                 |
|----------|------------------------------------|--------------------------------------|----------------------------|
| Storage  | Local SSD, fixed to node count     | **Managed storage in S3**, decoupled | Managed, decoupled         |
| Scaling  | Add nodes for storage *or* compute | Scale each independently             | Automatic                  |
| Best for | Small, fixed, <1 TB               | Most production clusters             | Spiky or intermittent, dev |
| Billing  | Per node-hour                      | Per node-hour + storage              | Per RPU-second             |

**RA3 separates compute from storage.** That sentence is the answer to “we keep adding nodes just for storage.”

**Serverless** bills in **RPUs** (1 RPU = 16 GB memory), per second with a **60-second minimum** per query. Base capacity 8 to 512 RPUs typically (4 RPU minimum in some regions). Set max RPU-hours as a cost guardrail. Right answer for intermittent or unpredictable workloads where a cluster would idle.

### Distribution styles

| Style | How rows are placed                                                                                                       | Cost of getting it wrong                                         |
|-------|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| AUTO  | The default. Redshift starts with ALL, moves to KEY on a suitable column as the table grows, then EVEN if no column suits | Rarely wrong, rarely optimal. Fine until a join gets slow        |
| EVEN  | Round-robin. Large tables rarely joined, or with no good join column                                                      | Every join redistributes data across the network                 |
| KEY   | Hash on one column. Two large tables joined on the same column, set the same DISTKEY on both                              | A skewed key creates hot slices, and only that one join benefits |
| ALL   | Full copy on every node. Small, slow-changing dimension joined by almost everything                                       | Storage multiplies by node count and every write is amplified    |

### DISTKEY vs SORTKEY

|                | DISTKEY                                                               | SORTKEY                                                                           |
|----------------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| Controls       | **Where** a row lives, which slice it sits on                         | **What order** rows are stored in on disk                                         |
| Solves         | Data movement between nodes during joins                              | Reading blocks that could never match the filter                                  |
| Set it to      | The columns you join on, same on both tables                          | The column you filter or range-scan                                               |
| Mechanism      | Hash placement, so matching rows are co-located and the join is local | Zone maps store min/max per 1 MB block, so whole blocks are skipped               |
| Plan symptom   | `DS_BCAST_INNER`, `DS_DIST_BOTH`                                      | Rows scanned far exceeds rows returned                                            |
| One per table? | One column only                                                       | Compound (ordered, the default) or interleaved (equal weight, costly to maintain) |

**Compound vs interleaved.** Compound sorts by the columns in declared order, so it only helps when your filter includes the leading column. Interleaved gives equal weight to every sort column, helping queries that filter on any one of them, but it degrades fast on load and needs `VACUUM REINDEX`. **Compound is almost always the answer.** Interleaved only when queries genuinely filter on different columns with no dominant leading one.

### VACUUM and ANALYZE

**VACUUM** reclaims space from deleted rows and re-sorts. **ANALYZE** updates statistics the planner uses. Different problems, commonly confused.

| Variant              | What it does                                                                           |
|----------------------|----------------------------------------------------------------------------------------|
| `VACUUM FULL`        | Default. Re-sorts and reclaims space                                                   |
| `VACUUM SORT ONLY`   | Re-sorts, doesn’t reclaim                                                              |
| `VACUUM DELETE ONLY` | Reclaims, doesn’t re-sort                                                              |
| `VACUUM REINDEX`     | For interleaved sort keys. Re-analyzes key distribution, then full vacuum. Much slower |
| `VACUUM RECLUSTER`   | Sorts only the unsorted region. Fast, for large tables with frequent loads             |

Symptom mapping: **table much larger on disk than row count justifies** points to VACUUM (dead rows from deletes/updates). **Query plan estimates are wildly off** points to ANALYZE. Redshift runs automatic vacuum delete and automatic table sort in the background, which reduces but does not eliminate the need.

Loading in sort key order avoids the re-sort entirely. Deep copy (recreate + insert) is faster than VACUUM on a badly unsorted large table.

### Loading and unloading

**COPY** is the only sane bulk load path. It loads in parallel across slices. Single-row `INSERT` is the classic anti-pattern.

COPY best practices: **split input into files that are a multiple of the slice count** so every slice works, use a manifest file for exactly-once and explicit file lists, compress the inputs, and load in sort key order. COPY from S3, DynamoDB, EMR, or remote hosts over SSH.

**UNLOAD** exports query results to S3 **in parallel**, one file per slice, in Parquet, CSV, or JSON. `PARALLEL OFF` gives one file. `MAXFILESIZE` controls the split.

### Concurrency and WLM

**WLM** (workload management) carves the cluster into queues with memory shares and concurrency slots. The canonical scenario: **overnight ETL and daytime BI queries compete for the same cluster, so WLM separates them** into queues so a heavy ETL job can’t starve dashboards.

**Auto WLM** lets Redshift set concurrency and memory. **Manual WLM** is you setting slots and memory percentages. Auto is the default recommendation; manual is for when you need hard guarantees per queue.

**Query monitoring rules (QMR)** act on metrics like execution time or rows scanned, and can log, hop, or abort a query.

**Concurrency scaling** adds transient cluster capacity automatically during query bursts. You earn one free hour of credit per 24 hours of main cluster usage. It is the answer to “dashboard queues up during peak.” It covers **writes too** (COPY, INSERT, UPDATE, DELETE, CTAS, VACUUM), not just reads, which is the detail a question can hinge on.

**SQA** (short query acceleration) runs short queries in a dedicated space so they don’t wait behind long ones.

### Spectrum and sharing

**Redshift Spectrum** queries S3 directly from a Redshift cluster using the Glue catalog, billed at $5/TB scanned like Athena, on separate Spectrum nodes. Use it for cold data left in S3 and joined against hot cluster tables. Requires an external schema and an IAM role.

Spectrum vs Athena: Spectrum when you already have a cluster and want to join S3 data against local tables. Athena when there’s no cluster and the workload is ad-hoc.

**Data sharing** gives live read access to another cluster or account with no copying (RA3 and Serverless only). **Federated query** reaches into RDS/Aurora directly. **Zero-ETL integrations** replicate from Aurora, RDS MySQL, and DynamoDB into Redshift with no pipeline to build. When a question describes “keep the warehouse in sync with the operational DB without building ETL,” that’s zero-ETL.

**Materialized views** precompute results and support incremental refresh, plus auto-refresh and automatic query rewriting.

### Encryption and security

Encryption at rest must be enabled **at cluster creation** for older clusters; RA3 supports enabling later via a migration. Column-level and row-level security available. Use IAM auth or `GetClusterCredentials` rather than static passwords.

## Step Functions and orchestration

### Standard vs Express

|              | Standard             | Express                |
|--------------|----------------------|------------------------|
| Max duration | 1 year               | 5 minutes              |
| Semantics    | Exactly-once         | At-least-once          |
| Billing      | Per state transition | Per request + duration |
| History      | 90 days, in console  | CloudWatch Logs only   |

Express with at-least-once means your states must be idempotent. Synchronous Express returns a result to the caller; asynchronous does not.

### Integration patterns

| Pattern           | Suffix              | Behavior                      |
|-------------------|---------------------|-------------------------------|
| Request Response  | none                | Fire and forget               |
| Run a Job         | `.sync`             | Wait for terminal state       |
| Wait for Callback | `.waitForTaskToken` | Pause until `SendTaskSuccess` |

`.sync` does **not** poll. It registers a managed EventBridge rule and waits for a callback, which is why the execution role needs `events:PutRule`.

**AWS SDK integrations** (`arn:aws:states:::aws-sdk:`) reach 200+ services and 9,000+ actions, but are always Request Response. No `.sync`.

### Map and Parallel

**Parallel** has fixed branches in the ASL; **Map** has one dynamic branch per input item.

|                   | Inline Map                 | Distributed Map                        |
|-------------------|----------------------------|----------------------------------------|
| Max concurrency   | **40**                     | 10,000                                 |
| Failure tolerance | None, one failure kills it | `ToleratedFailureCount` / `Percentage` |
| Execution         | Within the parent          | Child execution per item               |
| Item source       | State input only           | Input, or S3 objects / CSV / JSON      |
| History           | Counts against parent      | Separate per child                     |

`ToleratedFailurePercentage` exists **only** on Distributed Map. That’s usually the reason to choose it.

### Data flow

The `.$` suffix means the value is a JSONPath expression to evaluate, not a literal. `$$` is the context object (`$$.Execution.Name`, `$$.Map.Item.Value`).

`ResultPath` semantics:

- omitted: the task’s output **replaces the entire state**
- `"$.field"`: result is added under that key, input preserved
- `null`: result discarded, input passes through unchanged

`InputPath` filters what the task sees. `OutputPath` filters what moves on. `Parameters` reshapes the payload.

### Error handling

**Retry runs first; Catch only fires after retries are exhausted.** `BackoffRate` gives exponential backoff. Catch blocks evaluate **in order**, so list the specific error before `States.ALL`. Retry transient infrastructure errors; never retry a deterministic logic failure.

`States.TaskFailed`, `States.Timeout`, and `Lambda.ServiceException` are the common named errors.

### Scheduling

**EventBridge Scheduler** (principal `scheduler.amazonaws.com`) handles timezone- and DST-aware schedules across 270+ services, with one-time schedules and a flexible time window. EventBridge **Rules** are for event routing and are UTC-only.

Passing the scheduler’s `scheduled-time` context attribute into the payload makes retries idempotent, because the retry keeps the original timestamp and overwrites the same key.

Step Functions delivers logs only to groups under `/aws/vendedlogs/`. Vended logs are logs a service writes on your behalf, as opposed to logs your own code writes.

### MWAA (Managed Airflow)

Managed Apache Airflow. DAGs are Python files in an S3 bucket; MWAA syncs them. `requirements.txt` and a `plugins.zip` in the same bucket handle dependencies.

Environment classes are mw1.micro, small, medium, large, xlarge, and 2xlarge, with configurable worker auto-scaling (min/max workers). **mw1.micro is the one class that can’t auto-scale.**

**Step Functions vs MWAA:**

|            | Step Functions                                 | MWAA                                            |
|------------|------------------------------------------------|-------------------------------------------------|
| Definition | ASL JSON, visual                               | Python DAGs                                     |
| Best for   | Event-driven, serverless service orchestration | Complex scheduled batch DAGs, many dependencies |
| Cost       | Per transition, zero idle                      | Per environment-hour, always running            |
| Migration  | Rewrite                                        | **Lift and shift existing Airflow**             |

The tell for MWAA is “we already use Airflow,” “existing DAGs,” or a team that wants Python-defined dependencies. The tell for Step Functions is serverless, event-driven, and pay-per-use.

## EMR

Managed Hadoop ecosystem: Spark, Hive, Presto/Trino, HBase, Flink, Hudi, Iceberg.

**Deployment options:** EMR on EC2 (full control, cluster you size), EMR on EKS (runs on your Kubernetes cluster), **EMR Serverless** (no cluster, per-job capacity, the answer for spiky or infrequent Spark).

**Node types:**

- **Primary (master):** manages the cluster. One. Losing it kills the cluster unless you use multi-master (3 primaries).
- **Core:** run tasks *and* hold HDFS data. Removing them risks data loss, so **never put core nodes on Spot** for anything stateful.
- **Task:** compute only, no HDFS. **Spot-safe**, and the standard cost lever.

**Instance fleets vs instance groups.** Fleets let you specify a target capacity and a list of acceptable instance types with a Spot allocation strategy, so capacity is found across types and AZs. Groups pin one type per group. Fleets are the modern answer for Spot resilience.

**Transient vs long-running.** A transient cluster spins up, runs steps, and terminates (`auto-terminate`), which is the cheap pattern for batch. Long-running clusters serve interactive or always-on workloads.

**EMRFS** is the S3 connector, letting you treat S3 as the storage layer instead of HDFS. Use HDFS only as scratch during a job. Data in HDFS dies with the cluster.

**EMR vs Glue:** Glue is serverless and Spark-only with a catalog attached, good for standard ETL with minimal ops. EMR is the answer when you need a **specific framework or version** (Hive, Presto, HBase, custom Spark build), fine-grained cluster tuning, or Spot economics on large jobs.

## Monitoring and operations

**Alarm anatomy.** Namespace, dimensions (scope it to one resource, or you alarm on every resource in the account), statistic, period, evaluation periods, threshold.

Statistics: **Sum** for error counts, **p99** for latency. Average on errors is meaningless; average on latency hides the tail users actually feel.

Three states: **OK, ALARM, INSUFFICIENT_DATA**.

`treat_missing_data` values: **notBreaching, breaching, ignore, missing** (default). `notBreaching` stops weekend flapping on a weekday-only job, but means a job that never runs at all is silent. The fix is a **dead man’s switch**: alarm on `ExecutionsStarted < 1` over a window longer than the interval, with `treat_missing_data = breaching`.

A **metric filter** turns a value inside a JSON log line into an alarmable metric. That’s the payoff for structured logging.

**Composite alarms** combine several alarms with AND/OR to cut noise. **Anomaly detection** bands replace static thresholds for metrics with a daily or weekly shape.

**Log retention defaults to never expire**, which is a silent cost leak. Set it.

**SNS vs SQS.** SNS pushes to many subscribers who each get a copy, with no retention. SQS is a pull queue where one consumer gets each message. The **fanout** pattern subscribes multiple queues to one topic, giving broadcast plus durable buffering. EventBridge routes on content.

**SQS specifics:** standard is at-least-once with best-effort ordering; **FIFO** is exactly-once with strict ordering, 300 TPS (3,000 batched). Visibility timeout should exceed processing time or you get duplicate work. **DLQ** after `maxReceiveCount` catches poison pills. Long polling cuts empty receives and cost.

**CloudTrail** records API calls (who did what). **CloudWatch** records metrics and logs (how it’s performing). **AWS Config** records resource configuration over time (what it looked like then). Different questions.

**X-Ray** traces requests across services for latency breakdown.

**Cost tooling:** Cost Explorer for analysis, Budgets for alerts, **cost allocation tags** for attribution (they must be activated in the billing console before they do anything), and the Cost and Usage Report for line-item detail.

## IAM, security, and governance

**IAM.** A **trust policy** answers *who can assume this role*. A **permissions policy** answers *what it can do*. Attach every permission in the world and the service still can’t use the role without the trust policy.

`sts:AssumeRole` returns temporary, auto-rotating credentials. AWS-managed policies are recognizable by having **no account ID in the ARN** (`arn:aws:iam::aws:policy/...`). They’re convenient and usually broader than needed, so least privilege points to customer-managed.

**Least privilege means a role per component**, not one role for the pipeline, so a bug in one job can’t corrupt another’s data.

**Evaluation order:** explicit Deny always wins, then SCPs, then resource policies and identity policies, then permission boundaries. Anything not explicitly allowed is denied.

**IAM Roles Anywhere** gives workloads outside AWS temporary credentials via X.509 certificates. The right answer whenever an external app needs to reach AWS, instead of a static access key.

**Secrets Manager vs SSM Parameter Store:** Secrets Manager does automatic rotation and costs per secret. Parameter Store is free for standard parameters, no built-in rotation. “Rotate the database credentials automatically” is Secrets Manager.

**Macie** discovers PII sitting in S3. **Comprehend** detects and redacts PII inside text you feed it. Macie is at-rest discovery; Comprehend is in-flight NLP.

**KMS:** customer-managed keys let you set key policies, rotation, and grants. Multi-Region keys for cross-region workloads. **Envelope encryption** is a data key encrypting the data, and KMS encrypting the data key.

### Lake Formation

Centralized, fine-grained permissions over the Glue catalog, enforced by Athena, Redshift Spectrum, EMR, Glue, and QuickSight. It replaces managing dozens of S3 bucket policies with **database, table, and column grants that look like SQL** (`GRANT SELECT ON`).

**How it works:** you **register** an S3 location with Lake Formation, handing it a role. From then on Lake Formation vends short-lived, scoped credentials to query engines instead of every principal needing direct S3 access.

**Three levels of filtering:**

- **Column-level:** show only certain columns (hide SSN, salary)
- **Row-level:** a PartiQL filter expression on column values (a regional analyst sees only their region)
- **Cell-level:** rows and columns combined, so different columns are hidden depending on the row

Data filters apply to **reads only**.

**LF-Tags (tag-based access control)** are the scaling answer. Instead of granting on each of 500 tables, you tag resources (`classification=pii`, `domain=sales`) and grant a principal access to the *tag value*. New tables inherit the tag from their database and are covered automatically. Grant on tags when the catalog is large or growing; grant on named resources when it’s small and static.

**Cross-account sharing** works by granting to another account (or an AWS Organization / OU) directly, no bucket policy juggling. The recipient’s admin then re-grants to their own users. Tag-based grants make this scale.

**Hybrid access mode** lets IAM-based and Lake Formation permissions coexist on the same table during migration, so you don’t have to cut over everything at once.

`IAMAllowedPrincipals` is the backwards-compatibility grant that makes Lake Formation permissions effectively a no-op. **If Lake Formation grants seem to be ignored, that’s usually why.**

### Other governance services

**DataZone** is the business-facing data catalog and marketplace layer: domains, projects, publishing data assets, subscription workflows for requesting access. It sits above Glue and Lake Formation. The tell is “business users discover and request access to datasets.”

**Audit trail** for the lake: CloudTrail data events on S3 (off by default, costs extra) plus Lake Formation’s own access logs.

## Data modeling

### Slowly changing dimensions

| Type | Behavior                                           | History   |
|------|----------------------------------------------------|-----------|
| 0    | Never changes                                      | None      |
| 1    | Overwrite in place                                 | None      |
| 2    | New row per change, with `valid_from` / `valid_to` | Full      |
| 3    | Extra column holding the previous value            | One level |
| 4    | Current in main table, history in a separate table | Full      |
| 6    | Types 1 + 2 + 3 combined                           | Full      |

“Track historical changes over time” points to **Type 2**, essentially always. It works via a **surrogate key** unique per version, which is what facts join on so a fact row points at the values in effect when it occurred.

### Schemas

**Star schema:** a central fact table of measures joining outward to descriptive dimensions. Denormalized on purpose, because joins cost scan volume.

**Snowflake schema:** dimensions normalized into sub-dimensions. Saves storage, costs joins. Star is the default answer for analytics.

**Bridge table:** one row per pair, resolving a many-to-many. Makes “how many postings mention Python” a GROUP BY instead of array gymnastics.

**Grain:** what one row of a fact table means, stated in a sentence, declared before any columns are designed.

**Fact table types:** transaction (one row per event), periodic snapshot (one row per entity per period), accumulating snapshot (one row per process, updated as it progresses).

**Data vault** (hubs, links, satellites) shows up occasionally as the auditable, source-agnostic alternative for the raw layer.

## AI services for unstructured input

**Textract** pulls text, form fields, and tables out of scanned docs and images. **Comprehend** does NLP on text you already have: entities, sentiment, key phrases, PII. **Kendra** does semantic search across documents; ask a question, get the passage. **Translate** does machine translation. **Transcribe** is speech to text. **Rekognition** is images and video.

Textract gets text *out* of a document. Comprehend *understands* it. Kendra searches *across* many. They chain.

**SageMaker** shows up in the data engineering context mostly as Feature Store (shared feature repository) and Data Wrangler (visual prep).

## Quick decision index

| When you see                                       | Answer                                                    |
|----------------------------------------------------|-----------------------------------------------------------|
| Unknown or changing access pattern                 | S3 Intelligent-Tiering                                    |
| Multiple consumers, replay needed                  | Kinesis Data Streams                                      |
| No code, no shards, just deliver it                | Firehose                                                  |
| Windowed aggregation over a stream                 | Managed Service for Apache Flink                          |
| One shard hot, others idle                         | Change the partition key                                  |
| IteratorAge climbing                               | Consumer too slow, add shards or parallelization          |
| Too many small files in S3                         | Increase Firehose buffer size, or compact                 |
| Job needs more than 15 minutes                     | Not Lambda. Glue, Fargate, or Batch                       |
| Schema evolves constantly                          | Avro                                                      |
| Analytics reads, few columns                       | Parquet + Snappy                                          |
| Query scans too much                               | Partition first, then columnar, then compress             |
| Ad-hoc SQL, no infrastructure                      | Athena                                                    |
| Constant concurrent BI queries                     | Redshift                                                  |
| Adding Redshift nodes just for storage             | RA3                                                       |
| Redshift idle most of the day                      | Redshift Serverless                                       |
| Lowest-latency stream into the warehouse           | Redshift streaming ingestion                              |
| Table bigger on disk than rows justify             | VACUUM                                                    |
| Query plan estimates wildly off                    | ANALYZE                                                   |
| `DS_BCAST_INNER` in the plan                       | Wrong DISTKEY                                             |
| ETL and BI fighting for the cluster                | WLM queues                                                |
| Dashboards queue during peak                       | Concurrency scaling                                       |
| Column or row-level access across the lake         | Lake Formation                                            |
| Hundreds of tables to permission                   | LF-Tags                                                   |
| Lake Formation grants ignored                      | `IAMAllowedPrincipals` still present                      |
| Find PII sitting in S3                             | Macie                                                     |
| Redact PII inside text                             | Comprehend                                                |
| Existing Airflow DAGs                              | MWAA                                                      |
| Event-driven serverless orchestration              | Step Functions                                            |
| Need Hive, Presto, or a specific Spark version     | EMR                                                       |
| Spiky Spark, no cluster to manage                  | EMR Serverless                                            |
| Keep warehouse in sync with operational DB, no ETL | Zero-ETL integration                                      |
| Migrate a database with ongoing changes            | DMS full load + CDC                                       |
| External app needs AWS credentials                 | IAM Roles Anywhere                                        |
| Rotate credentials automatically                   | Secrets Manager                                           |
| Job that never runs is silent                      | Dead man’s switch alarm, `treat_missing_data = breaching` |

## Terraform

**Not on the exam.** AWS only tests CloudFormation, CDK, and SAM. Kept here because it’s career-relevant.

Block types:

| Block       | Purpose                                                                    |
|-------------|----------------------------------------------------------------------------|
| `terraform` | Configures Terraform itself. The only block that can’t reference variables |
| `provider`  | Configures a downloaded plugin: region, credentials, settings              |
| `resource`  | Creates and manages a thing. Deleting the block destroys it                |
| `data`      | Read-only lookup. Creates nothing, destroys nothing                        |
| `variable`  | Declares an input someone can supply                                       |
| `output`    | Exposes a value after apply, passes it to a parent module                  |
| `locals`    | Computed values. Plural, many assignments in one block                     |
| `module`    | Calls a reusable group of resources                                        |

`~>` is the **pessimistic constraint operator**: the rightmost number you wrote can increment, nothing to its left. `~> 5.0` allows 5.1 through 5.99, not 6.0. `~> 5.31.0` allows only 5.31.x.

`.terraform.lock.hcl` pins exact versions and checksums. **Commit it.**

Variable precedence, low to high: `default`, `TF_VAR_` env var, `terraform.tfvars`, `*.auto.tfvars`, CLI `-var`. Omitting `default` is what makes a variable required. `sensitive = true` only hides the value from plan and apply output; plaintext still lands in state.

A **validation block** is the variable argument that catches bad input at plan time with your own error message.

`path.module` is the module’s own directory. `path.root` is the root module. `path.cwd` is where you invoked terraform.

`for_each` keys by a stable string, so removing one item destroys one resource. `count` indexes by position, so a removal shifts every later index and recreates them all. Use `for_each` for anything that changes.

`aws_caller_identity.account_id` builds globally unique bucket names and keeps the account number out of git, so the repo stays portable and one string isn’t repeated in 30 typo-able places.

**Drift** is CloudFormation’s term for someone changing a resource by hand in the console.

**Parameterized queries**, not string interpolation, are the safe way to build SQL. Dropdown values are data-driven, one text input makes injection real, and Athena supports `DROP TABLE` and `INSERT INTO`.
