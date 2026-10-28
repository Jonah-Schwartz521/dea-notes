# 02 - Storage

The raw landing zone. Everything the pipeline ingests lands here first.

**Files:** `infra/s3.tf`

---

## Why four resources for one bucket

```hcl
resource "aws_s3_bucket" "raw" { }
resource "aws_s3_bucket_public_access_block" "raw" { }
resource "aws_s3_bucket_versioning" "raw" { }
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" { }
```

In AWS provider v3 and earlier, all of this was arguments inside one
`aws_s3_bucket` block. v4 split them into separate resources.

The reason: S3 bucket settings are separate AWS APIs
(`PutBucketVersioning`, `PutBucketEncryption`, etc). One giant resource
meant Terraform couldn't tell which sub-setting drifted, and a change to
one caused noisy diffs across all of them.

Practical consequence: any tutorial older than 2022 shows the old syntax
and will not work on `~> 5.0`. This is exactly why the version pin in
`main.tf` matters.

---

## The bucket

```hcl
resource "aws_s3_bucket" "raw" {
  bucket = "${var.name_prefix}-raw-${data.aws_caller_identity.current.account_id}"
}
```

Resolves to `jobmarket-raw-123456789012`.

Both pieces from `01-foundation`: prefix for scoping and grouping,
account ID for global uniqueness.

**Terraform naming vs AWS naming.** Two different names here:

```hcl
resource "aws_s3_bucket" "raw" {     # "raw" = Terraform's local name
  bucket = "jobmarket-raw-1234..."   # the actual AWS bucket name
}
```

`raw` is only used inside Terraform. Change it and Terraform thinks you
deleted one resource and created another, so it destroys and recreates.
Change the `bucket` argument and same thing, because bucket names are
immutable in AWS. Rename means new bucket.

---

## Implicit dependencies

```hcl
bucket = aws_s3_bucket.raw.id
```

Every other resource in the file references the bucket this way, and this
is how Terraform builds its dependency graph.

`aws_s3_bucket.raw.id` doesn't exist until the bucket is created. So
Terraform automatically knows: create the bucket first, then the other
three. No ordering instructions needed anywhere in the file.

Terraform builds a DAG from these references and parallelizes anything
unrelated. The three config resources here run concurrently with each
other, since none of them reference each other.

There's also `depends_on` for forcing order when there's no reference,
but needing it usually means a design smell. Implicit is better.

Note `.id` for a bucket returns the bucket name, not a separate ID. S3 is
a bit unusual that way. `.arn` gives you the full
`arn:aws:s3:::jobmarket-raw-1234...`.

---

## Public access block

```hcl
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
```

Four independent switches. All four on is the correct default and there
is basically never a reason to relax them for a data pipeline.

What each one actually does:

| Setting | Blocks |
|---|---|
| `block_public_acls` | **New** public ACLs from being set. Rejects the API call. |
| `ignore_public_acls` | **Existing** public ACLs. They stay but are ignored. |
| `block_public_policy` | **New** bucket policies that grant public access. |
| `restrict_public_buckets` | **Existing** public bucket policies. Only the owner and AWS services can access. |

The pattern is two pairs: block-new vs ignore-existing, for ACLs and for
policies separately. That's why there are four and not two.

**Why this exists at all.** Public S3 buckets are the single most common
cause of major data breaches. Accenture, Verizon, Capital One adjacent
incidents, US voter records, all leaked buckets. AWS added this feature
specifically because customers kept doing it by accident.

Since 2023 AWS blocks public access by default on new buckets, so this is
partly belt-and-braces now. Declaring it explicitly is still right,
because it's visible in code review and it survives someone changing an
account-level default.

**Account-level version exists too.** `aws_s3_account_public_access_block`
applies to every bucket in the account. Worth knowing for the exam: when
account-level and bucket-level disagree, **the more restrictive wins**.
You cannot re-open a bucket that the account level has locked down.

---

## Versioning

```hcl
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}
```

With versioning on, overwriting an object doesn't destroy the old one. S3
keeps every version and gives each a version ID. The newest is what you
get by default; the older ones are still retrievable.

Deleting doesn't really delete either. It writes a **delete marker** on
top. The object is hidden but recoverable by removing the marker.
Permanent deletion requires specifying a version ID explicitly.

**Why it's on for the raw zone specifically.** Raw is the only
irreplaceable data in the pipeline. Curated can be rebuilt by rerunning
Glue. Athena results are just query output. But if a bad ingest run
overwrites yesterday's Greenhouse pull with an empty file, that data is
gone from the API, since job postings disappear when they're filled.
Versioning is the undo button for the one thing you can't regenerate.

**Three states, and the third is a trap:**

```
Unversioned  -> default, never enabled
Enabled      -> keeping versions
Suspended    -> stops making NEW versions, keeps existing ones
```

You can never go back to Unversioned. Once enabled, the most you can do
is Suspend. Exam likes this.

**The cost catch.** You pay for every version. A job that rewrites the
same file daily accumulates copies forever. Versioning without a
lifecycle rule to expire old versions is a slow-growing bill. See gaps
below, since I don't have one yet.

**Versioning is also a prerequisite** for cross-region replication and
for MFA delete. Both show up in exam scenarios about durability and
compliance.

---

## Encryption at rest

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

`AES256` here means **SSE-S3**. AWS manages the keys entirely. Encryption
and decryption happen transparently, and anyone with S3 read permission
can read the data. No extra cost.

**The four options, and this is heavily tested:**

| Option | Who holds the key | Cost | When it wins |
|---|---|---|---|
| **SSE-S3** (`AES256`) | AWS, invisible | Free | Default. No compliance requirement. |
| **SSE-KMS** (`aws:kms`) | AWS KMS, you control the key | Per-key + per-request | Need audit trail, key rotation, or cross-account key policies |
| **SSE-C** | You send the key with every request | Free | You must hold keys yourself. Rare. |
| **Client-side** | You, before upload | Free | AWS never sees plaintext |

The exam decision rule: **audit trail, key rotation, or granular key
access control means SSE-KMS. Otherwise SSE-S3.**

The tiebreakers that show up:

- KMS logs every decrypt to CloudTrail. SSE-S3 doesn't. That alone
  decides most compliance questions.
- KMS has request quotas. High-throughput workloads reading millions of
  small objects can throttle on KMS, which is what **S3 Bucket Keys** fix
  by reducing KMS calls up to 99%.
- KMS costs per request. At data lake scale that's real money.
- Cross-account access to KMS-encrypted data needs the key policy to
  allow it too, not just the bucket policy. Very common exam trap.

**Why SSE-S3 is right here.** Job postings are public data scraped from
public ATS endpoints. No PII, no compliance regime, no audit requirement.
Paying for KMS would buy nothing.

**Worth knowing:** since January 2023, S3 encrypts all new objects with
SSE-S3 by default. So this block is technically redundant, same as the
public access block. Declaring it explicitly is still correct, because it
documents intent and it pins the algorithm even if defaults change.

---

## What's missing from this file

Honest gaps, not oversights I'm hiding:

**No lifecycle policy on this bucket.** Raw JSON accumulates daily and
old versions accumulate on top. Should have a rule transitioning objects
to a cheaper storage class after N days and expiring noncurrent versions.
Note that `athena-results-lifecycle.json` exists in `iam/`, so lifecycle
exists elsewhere in the stack but not here.

**No bucket policy.** Access is controlled purely through IAM roles
attached to Lambda and Glue. That's fine and arguably cleaner for a
single-account setup. Bucket policies matter for cross-account access.

**No tags.** Should tag with project and environment for cost allocation.

**No `force_destroy`.** Means `terraform destroy` fails if the bucket has
objects in it. Safe default, mildly annoying for a study project I tear
down often.

---

## Storage classes for the exam

Not in this file, but Domain 2 tests it hard.

| Class | Retrieval | Min duration | Use |
|---|---|---|---|
| Standard | instant | none | active data |
| Intelligent-Tiering | instant | none | unknown/changing access patterns |
| Standard-IA | instant | 30 days | infrequent but needs speed |
| One Zone-IA | instant | 30 days | reproducible data, one AZ |
| Glacier Instant Retrieval | instant | 90 days | archive, rare instant access |
| Glacier Flexible Retrieval | mins to hours | 90 days | archive |
| Glacier Deep Archive | 12 hours | 180 days | compliance, cheapest |

**Decision rules:**

- "Access pattern is unpredictable / unknown" -> Intelligent-Tiering.
  This is the giveaway phrase.
- "Can be regenerated" -> One Zone-IA is acceptable.
- "Must retain 7 years for compliance, rarely accessed" -> Deep Archive.
- Retrieving from IA before its minimum duration still bills the full
  minimum. Small objects accessed often are cheaper in Standard.

---

## Exam angles

**Domain 2, Data Store Management (26%)**
- Storage class selection by access pattern
- Lifecycle transitions and expiration
- Versioning behavior, delete markers, Suspended state
- Bucket naming: globally unique, DNS-compliant, lowercase

**Domain 4, Data Security and Governance (18%)**
- SSE-S3 vs SSE-KMS vs SSE-C decision rule
- Bucket Keys as the fix for KMS throttling and cost
- Public access block, and account-level beating bucket-level
- Encryption in transit is separate. That's `aws:SecureTransport` in a
  bucket policy, not configured here.

---

## Gaps

- [ ] Add a lifecycle rule for noncurrent version expiration
- [ ] `aws_s3_bucket_lifecycle_configuration` syntax
- [ ] When S3 Bucket Keys actually matter, with numbers
- [ ] Cross-region replication requirements
- [ ] S3 consistency model (strong read-after-write since Dec 2020)