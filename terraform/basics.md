# Terraform Basics

Reference for the IaC concepts behind every `.tf` file in my projects.
Terraform itself is not on DEA-C01, but it's what data teams actually run,
and the concepts map onto CloudFormation, which is tested.

---

## What Terraform is

Two ways to create AWS resources. Click through the console, or describe
what you want in a text file and let a tool make the API calls.

Terraform is the second one. The payoff isn't automation, it's that the
text file becomes the source of truth. The whole environment is readable,
diffable, version-controlled text. Anyone can read the repo and know what
exists without logging in.

It's **declarative**. You don't write steps ("create bucket, then set
encryption"). You write the desired end state, and Terraform figures out
which API calls get from where you are to where you said you want to be.

That's why running `apply` twice does nothing the second time. Reality
already matches the file.

---

## Block syntax

`.tf` files are written in HCL. The unit is a block:

```hcl
block_type "label_one" "label_two" {
  argument = value
}
```

Label count is fixed by block type:

```hcl
terraform { }                              # zero labels
provider "aws" { }                         # one
resource "aws_s3_bucket" "raw" { }         # two: type, then my name
data "aws_caller_identity" "current" { }   # two: type, then my name
variable "region" { }                      # one
output "bucket_name" { }                   # one
locals { }                                 # zero
```

---

## The block types

### terraform

Configures Terraform itself. The only block that can't reference
variables, because it's read before variables load.

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}
```

### provider

Configures a downloaded plugin. Where and who.

```hcl
provider "aws" {
  region = var.region
}
```

Credentials are deliberately absent here. The provider resolves them
through a chain: arguments in the block (never do this), `AWS_ACCESS_KEY_ID`
env vars, `~/.aws/credentials` from `aws configure`, then an IAM role if
running on EC2 or in CI.

Never hardcode keys in a `.tf` file. Bots scan public GitHub for AWS keys
within seconds of a push.

### resource vs data

The fundamental distinction.

```hcl
resource "aws_s3_bucket" "raw" { }         # CREATE. Terraform owns it.
data "aws_caller_identity" "current" { }   # LOOK UP. Read-only.
```

A `resource` goes into state, updates when you edit the config, and gets
**destroyed** if you delete the block.

A `data` block creates nothing and destroys nothing. Delete it and AWS is
unchanged. Data sources exist because your infrastructure never lives in
isolation: you need the VPC someone else made, the latest AMI ID, or your
own account number.

```hcl
data "aws_caller_identity" "current" {}

# referenced as data.<type>.<name>.<attribute>
data.aws_caller_identity.current.account_id   # 123456789012
data.aws_caller_identity.current.arn
```

### variable

Declares an input. Covered in full below.

### output

Exposes a value after apply, and makes it available to a parent module.

```hcl
output "raw_bucket" {
  value       = aws_s3_bucket.raw.bucket
  description = "Name of the raw landing bucket"
}
```

Read them with `terraform output` or `terraform output -json`.

### locals

Named values computed inside the config. Not inputs, so no one can
override them.

```hcl
locals {
  state_machine_name = "${var.name_prefix}-daily"
  active_companies = [
    for c in local.companies_file.companies : c
    if c.active && contains(["greenhouse", "lever"], c.ats)
  ]
}
```

Referenced as `local.name`. Use them to avoid repeating an expression, or
to name a piece of derived logic.

**variable vs local:** a variable is an input someone supplies. A local is
something you compute. If a caller should be able to change it, it's a
variable.

---

## State

`terraform.tfstate` is Terraform's memory between runs. It maps your
code's names to real AWS IDs:

> the thing I call `aws_s3_bucket.raw` is really the bucket
> `jobmarket-raw-123456789012`

Without it, Terraform has no way to know whether to create something or
update it. It would just create duplicates every run.

**Two rules.** Never hand-edit it. Never commit it to a public repo,
because it can contain secrets in plaintext.

`terraform.tfstate.backup` is the previous version, kept automatically in
case an apply corrupts the current one.

### Remote state

For a solo project, local state is fine. On a team it breaks: two people
each have their own state file, each believes it owns the same resources,
and whoever applies second tries to create things that already exist.

The fix is remote state in S3 with DynamoDB locking, so everyone reads the
same state and only one person can apply at a time.

```hcl
terraform {
  backend "s3" {
    bucket         = "my-tf-state"
    key            = "job-market/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "tf-locks"
    encrypt        = true
  }
}
```

### Drift and import

Terraform only manages what's in code and in state. A resource someone
created by hand in the console is invisible to it, and `plan` shows
nothing.

`terraform import` brings an existing resource under management. **Drift**
is the CloudFormation term for reality diverging from the template, and
it's the vocabulary the exam uses.

Removing a resource block also doesn't remove things that resource
*produced*. Delete a crawler and the tables it created stay behind as
orphans. Same class of problem as a Lambda auto-creating its own log group.

---

## The workflow

**`terraform init`** reads the `terraform` block, downloads providers into
a hidden `.terraform/` folder, writes or verifies `.terraform.lock.hcl`.
Run once per project, and again whenever you add a provider.

**`terraform plan`** reads all `.tf` files, resolves variables, executes
data sources, reads state, queries AWS to check reality, and prints a
diff. Creates nothing. Read it every time.

**`terraform apply`** does the same, shows the diff, asks yes or no, then
makes the calls and updates state.

**`terraform destroy`** tears down everything in state. Genuinely useful
for cost control on a study project.

**`terraform fmt`** reformats and indents. Run it before every plan.
Misplaced blocks become obvious because the indentation goes wrong.

**`terraform validate`** checks syntax and internal consistency without
touching AWS.

### Reading a plan

Three symbols that matter:

```
+  create
~  update in place
-  destroy
-/+  destroy and recreate
```

`-/+` is the one to stop and look at. It means an immutable attribute
changed, so Terraform must delete and rebuild. On a bucket with data in
it, that either fails or loses the data.

The last line is the summary: `Plan: 2 to add, 1 to change, 0 to destroy.`
Check the destroy count before every apply.

---

## Providers and versioning

Terraform core knows nothing about AWS. It's a generic engine that reads
config, tracks state, and diffs. All cloud knowledge lives in downloadable
plugins called **providers**.

```hcl
required_providers {
  aws = {
    source  = "hashicorp/aws"
    version = "~> 5.0"
  }
  archive = {
    source  = "hashicorp/archive"
    version = "~> 2.4"
  }
}
```

- The key (`aws`) is the **local name**. It's why resource types read
  `aws_s3_bucket`, `aws_glue_job`. That prefix matches this key.
- `source` is a Terraform Registry address, `namespace/name`. The registry
  works like npm; the namespace is how you avoid a lookalike.

Not every provider talks to a cloud. `hashicorp/archive` does one local
job, turning a folder into a zip, because Lambda won't accept a directory.

### Version constraints

Providers use semver, `MAJOR.MINOR.PATCH`.

- PATCH = bug fix
- MINOR = new features, nothing breaks
- MAJOR = breaking changes on purpose

| Constraint | Allows | Verdict |
|---|---|---|
| `= 5.31.0` | only 5.31.0 | frozen, no fixes |
| `>= 5.0` | 5.0, 6.0, 7.0, anything | dangerous |
| `~> 5.0` | 5.0 to <6.0 | the usual choice |
| `~> 5.31.0` | 5.31.0 to <5.32.0 | patch only |

`~>` is the **pessimistic constraint operator**. The rule for reading it:
the rightmost number you wrote can increment, nothing to its left can.

- `~> 5.0` has two places, so minor moves. 5.1, 5.42, not 6.0.
- `~> 5.31.0` has three places, so patch moves. 5.31.9, not 5.32.0.

The AWS provider ships weekly because AWS launches services constantly, so
you want fixes automatically but not a breaking 6.0 on a Monday morning.

### The lock file

`~> 5.0` is a range, not a version. Init in January gets 5.31.0, init in
March gets 5.42.0, and two machines now behave differently.

The first `terraform init` resolves a concrete version and writes it plus
a checksum into `.terraform.lock.hcl`:

```hcl
provider "registry.terraform.io/hashicorp/aws" {
  version     = "5.31.0"
  constraints = "~> 5.0"
  hashes      = ["h1:..."]
}
```

Everyone who clones gets 5.31.0 exactly. The hash also blocks a tampered
plugin. **Commit this file.** Move up deliberately with
`terraform init -upgrade`.

---

## Variables

```hcl
variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}
```

Variables are the public interface of a repo. Someone can run it in a
different region with a different prefix without editing any logic.

**`description`** shows in the interactive prompt when a value is missing,
and gets pulled into auto-generated docs by `terraform-docs`.

**`type`** validates at plan time. A clear Terraform error instead of a
cryptic AWS API error mid-apply.

```hcl
type = string
type = number
type = bool
type = list(string)   # ["a", "b"]
type = map(string)    # { env = "prod" }
type = set(string)    # unordered, deduped
type = object({ name = string, size = number })
```

Omitting `type` gives `any`, which validates nothing. Always declare it.

**`default`** decides required vs optional. Has one, `apply` runs with no
config. Missing, Terraform refuses to proceed and prompts.

### Declare vs assign

Different syntax, easy to mix up:

```hcl
# variables.tf  -> DECLARE
variable "region" {
  type    = string
  default = "us-east-1"
}
```

```hcl
# prod.tfvars   -> ASSIGN
region      = "us-west-2"
name_prefix = "jobmarket-prod"
```

```bash
terraform apply -var-file=prod.tfvars
terraform apply -var="region=us-west-2"
```

One repo, multiple environments. Same code, different inputs.

### Precedence

Low to high, later wins:

```
1. default in the variable block
2. TF_VAR_region=us-west-2       (env var)
3. terraform.tfvars              (auto-loaded)
4. *.auto.tfvars                 (auto-loaded, alphabetical)
5. -var / -var-file              (CLI)
```

### validation and sensitive

```hcl
variable "name_prefix" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.name_prefix))
    error_message = "Lowercase alphanumeric and hyphens only (S3 rules)."
  }
}
```

Catches bad input at plan time with your own message, instead of at apply
time after half the stack already built. `type = string` only checks it's
a string, so `Job_Market` passes validation and then fails on S3's naming
rules mid-apply.

```hcl
variable "db_password" {
  type      = string
  sensitive = true
}
```

`sensitive` redacts the value from plan and apply output. **It does not
encrypt state.** Plaintext still sits in `terraform.tfstate`. Real secrets
go in Secrets Manager or SSM Parameter Store, fetched at runtime.

---

## Expressions and functions

### Interpolation

```hcl
bucket = "${var.name_prefix}-raw-${data.aws_caller_identity.current.account_id}"
```

`${}` embeds an expression in a string. When the whole value is an
expression, drop the quotes and the braces:

```hcl
bucket = aws_s3_bucket.raw.id          # right
bucket = "${aws_s3_bucket.raw.id}"     # works, but noisy
```

### for expressions

Build a list or map from another collection.

```hcl
# list -> filtered list
[for c in local.companies : c if c.active]

# list -> reshaped objects
[for c in local.companies : { ats = c.ats, company = c.board_token }]

# list -> map, keyed on a field
{ for c in local.scheduled : c.board_token => c }
```

Square brackets produce a list, curly braces produce a map. The map form
is what converts a list into something `for_each` can take.

### Useful functions

```hcl
file("${path.module}/../config/companies.yml")   # read a file as a string
yamldecode(...)                                  # YAML -> data structure
jsondecode(...) / jsonencode(...)                # JSON both ways
templatefile(path, { vars })                     # file + variable substitution
filemd5(path)                                    # hash, for change detection
contains(["a","b"], x)                           # membership
merge(map1, map2)                                # combine maps
lookup(map, key, default)                        # safe map access
coalesce(a, b, c)                                # first non-null
try(expr, fallback)                              # swallow an error
can(expr)                                        # true/false, for validation
```

### Path references

```hcl
path.module   # directory of the module being evaluated
path.root     # directory of the root module
path.cwd      # where you invoked terraform
```

Use `path.module` for relative paths so the config works no matter what
directory you run terraform from.

### Conditionals

```hcl
count = var.enable_alarms ? 1 : 0
name  = var.env == "prod" ? "app" : "app-${var.env}"
```

---

## Meta-arguments

Arguments that work on any resource block regardless of type. Terraform
handles them itself rather than passing them to the provider. There are
four.

### count

Creates N copies.

```hcl
resource "aws_instance" "web" {
  count         = 3
  ami           = "ami-123"
  instance_type = "t3.micro"
}
```

Addressed by index. `count.index` gives the current number inside the
block.

```
aws_instance.web[0]
aws_instance.web[1]
aws_instance.web[2]
```

Also works as an on/off switch, which is its most common real use:

```hcl
resource "aws_cloudwatch_metric_alarm" "extra" {
  count = var.enable_alarms ? 1 : 0
}
```

Zero means the resource isn't created at all.

### for_each

Creates one copy per element of a map or set.

```hcl
resource "aws_scheduler_schedule" "ingest" {
  for_each = { for c in local.scheduled : c.board_token => c }

  name = "ingest-${each.key}"
  # each.value is the whole object
}
```

Addressed by key, not index:

```
aws_scheduler_schedule.ingest["komodohealth"]
aws_scheduler_schedule.ingest["zocdoc"]
```

`each.key` and `each.value` give the current entry. Takes a map or a set
of strings, so a list needs converting first, which is what the
`{ for ... }` expression does.

### Why for_each beats count

The thing to actually remember.

`count` indexes by **position**. Remove the second of five entries and
everything after it shifts down:

```
before                        after
[0] komodohealth              [0] komodohealth
[1] zocdoc          <- gone   [1] truveta      (was [2])
[2] truveta                   [2] talkspace    (was [3])
[3] talkspace                 [3] doximity     (was [4])
[4] doximity
```

Terraform tracks resources by address, so it sees `[1]` change from
zocdoc to truveta, `[2]` from truveta to talkspace, and so on. It
destroys and recreates **four** resources to remove one.

`for_each` keys by a **stable string**. Remove zocdoc and Terraform
destroys exactly `["zocdoc"]`. Everything else is untouched because their
addresses never changed.

**Rule: `count` only for a fixed number or an on/off toggle. `for_each`
for anything driven by a list that will change.**

The corollary: the key you choose is permanent. Change what you key on and
every resource is destroyed and recreated, same as switching between the
two meta-arguments.

### depends_on

Forces ordering when Terraform can't infer it.

```hcl
resource "aws_lambda_function" "ingest" {
  # ...
  depends_on = [aws_cloudwatch_log_group.ingest]
}
```

Only for the case where a real ordering requirement exists but there's no
attribute to reference. The Lambda above never mentions its log group, but
if Lambda runs first it auto-creates one with Never Expire retention and
Terraform collides with it.

Needing `depends_on` usually means you should be referencing an attribute
instead.

### lifecycle

```hcl
lifecycle {
  create_before_destroy = true    # build the new one first
  prevent_destroy       = true    # refuse to delete, ever
  ignore_changes        = [tags]  # don't fight external edits
  replace_triggered_by  = [...]   # force replacement when something else changes
}
```

`prevent_destroy` on a production database or a state bucket is cheap
insurance. `ignore_changes` is for attributes another system manages,
where Terraform would otherwise revert them on every apply.

---

## Dependencies and the graph

Terraform builds a directed acyclic graph from your config and uses it to
decide order and parallelism.

**Implicit dependencies** come from references. This is the normal case
and always preferable:

```hcl
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id     # <- creates the dependency
}
```

`aws_s3_bucket.raw.id` doesn't exist until the bucket is created, so
Terraform knows the order without being told. Anything unrelated runs in
parallel.

**Explicit dependencies** are `depends_on`, for when there's genuinely no
attribute to reference.

**Cycles** are rejected. The usual cause is self-reference, like a state
machine role that needs permission to invoke the state machine that uses
that role:

```
role -> policy -> state machine -> role
```

The escape hatch is constructing the ARN by hand instead of referencing
the resource, since ARN format is deterministic:

```hcl
locals {
  state_machine_arn = "arn:aws:states:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.state_machine_name}"
}
```

---

## Config as data

Reading external config keeps repetitive definitions out of the `.tf`
files entirely.

```hcl
locals {
  companies_file = yamldecode(file("${path.module}/../config/companies.yml"))

  active = [
    for c in local.companies_file.companies : c
    if c.active && contains(["greenhouse", "lever"], c.ats)
  ]
}
```

Adding an entry becomes a YAML edit and an apply, not a code change. The
alternative is a dozen near-identical resource blocks that drift the
moment someone edits one and not the others.

`templatefile()` does the same for a file that needs values substituted
into it, which is how a Step Functions definition gets its ARNs:

```hcl
definition = templatefile("${path.module}/statemachine.asl.json", {
  ingest_function_arn = aws_lambda_function.ingest.arn
  alerts_topic_arn    = aws_sns_topic.alerts.arn
})
```

---

## Change detection for code artifacts

Terraform compares `.tf` files. It does not notice that your Python
changed. That has to be wired up explicitly.

**Lambda:**

```hcl
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../src/ingest"
  output_path = "${path.module}/../build/ingest.zip"
}

resource "aws_lambda_function" "ingest" {
  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256
}
```

**S3-hosted scripts, like Glue jobs:**

```hcl
resource "aws_s3_object" "script" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/normalize.py"
  source = "${path.module}/../src/glue/normalize.py"
  etag   = filemd5("${path.module}/../src/glue/normalize.py")
}
```

Without the hash or etag, you edit the code, run apply, Terraform sees no
diff, and the old version keeps running. Most common Terraform mistake
with deployed code.

**One catch on the etag trick:** S3's ETag is only an MD5 for single-part
uploads on unencrypted or SSE-S3 buckets. SSE-KMS or a multipart upload
changes it, and `filemd5()` never matches again.

---

## Terraform vs CloudFormation

DEA-C01 only tests AWS-native IaC. Terraform never appears.

| Terraform | AWS equivalent |
|---|---|
| `.tf` files (HCL) | CloudFormation templates (YAML/JSON) |
| `terraform.tfstate` | CloudFormation manages state invisibly |
| `terraform plan` | change set |
| console edits breaking things | **drift** |
| modules | nested stacks |
| providers | built in, it's all AWS |
| `terraform apply` | create/update stack |

**The three AWS tools:**

- **CloudFormation** — YAML or JSON templates, the base layer
- **CDK** — real Python or TypeScript that compiles to CloudFormation
- **SAM** — a simplified CloudFormation dialect for serverless

**What actually transfers to the exam:** any question with "least
operational overhead," "repeatable," or "consistent across environments"
where one option is IaC and another is a manual runbook, pick IaC.

Worth 30 minutes at some point just reading a CloudFormation template so
the syntax isn't foreign when it shows up in a question.

---

## Gotchas worth not relearning

- **`~>` reads right to left.** The rightmost number you wrote moves.
- **`-/+` in a plan means destroy and recreate.** Stop and check.
- **Bucket names are immutable.** Changing one destroys and rebuilds.
- **`count` on a changing list causes cascading recreation.** Use `for_each`.
- **Deleting a resource block doesn't delete what it produced.** Orphans stay.
- **`sensitive = true` doesn't encrypt state.**
- **`type = string` doesn't validate content.** Add a `validation` block.
- **Terraform doesn't see code changes** unless you hash them.
- **Run `terraform fmt` before every plan.** Bad indentation reveals
  misplaced blocks immediately.