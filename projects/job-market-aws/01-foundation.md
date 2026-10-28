# 01 - Foundation

Provider config, versioning, account lookup, and shared inputs.
Creates zero infrastructure. Sets up how everything else gets built.

**Files:** `infra/main.tf`, `infra/variables.tf`

---

## main.tf

### Full file

```hcl
terraform {
  required_version = ">= 1.5"

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
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}
```

Three blocks. None of them create anything in AWS.

---

### Block syntax

Every `.tf` file is written in HCL. The unit is a block:

```hcl
block_type "label_one" "label_two" {
  argument = value
}
```

Label count is fixed by block type:

```hcl
terraform { }                                  # zero labels
provider "aws" { }                             # one label
data     "aws_caller_identity" "current" { }   # two: type, then my name
resource "aws_s3_bucket" "raw" { }             # two: type, then my name
```

---

### terraform block

```hcl
terraform {
  required_version = ">= 1.5"
  ...
}
```

Configures Terraform itself, not any cloud resource. It's the only block
that cannot reference `var.` anything, because it's read before variables
load.

`required_version = ">= 1.5"` is a floor on the Terraform CLI. Anyone on
1.2 gets a clear "your Terraform is too old" instead of a confusing parse
error three steps in. `>=` is open-ended, so 2.0 would also satisfy it.

---

### required_providers

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

Terraform core knows nothing about AWS. It's a generic engine that reads
config, tracks state, and diffs. All cloud knowledge lives in downloadable
plugins called providers.

Breaking down the `aws` entry:

- `aws` (the key) is the **local name**. It's why resource types are
  written `aws_s3_bucket`, `aws_lambda_function`, `aws_glue_job`. That
  `aws_` prefix matches this key.
- `source = "hashicorp/aws"` is the Terraform Registry address, format
  `namespace/name`. Registry works like npm or PyPI. The namespace is how
  you avoid downloading a lookalike.
- `version = "~> 5.0"` is the constraint. See below.

The `archive` provider talks to no cloud. It does one local job: turn a
folder into a zip. It's here because Lambda will not accept a directory,
only a zip deployment package.

The bridge from `src/ingest/handler.py` to `build/ingest.zip` is in
`lambda.tf` and looks like:

```hcl
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${path.module}/../src/ingest"
  output_path = "${path.module}/../build/ingest.zip"
}
```

Terraform hashes the zip. Python changed means hash changed means the
Lambda redeploys. Nothing changed means nothing redeploys. Dependency
tracking for free, versus running a zip script manually and eventually
forgetting.

---

### Version constraints

Providers use semver: `MAJOR.MINOR.PATCH`, so `5.31.0`.

- PATCH bump = bug fix
- MINOR bump = new features, nothing breaks
- MAJOR bump = breaking changes on purpose

The AWS provider ships weekly because AWS launches services constantly.

| Constraint | Allows | Verdict |
|---|---|---|
| `= 5.31.0` | only 5.31.0 | frozen, no fixes |
| `>= 5.0` | 5.0, 6.0, 7.0, anything | dangerous |
| `~> 5.0` | 5.0 to <6.0 | what I used |
| `~> 5.31.0` | 5.31.0 to <5.32.0 | patch only |

`~>` is the **pessimistic constraint operator**. Rule for reading it: the
rightmost number you wrote can increment, nothing left of it can.

- `~> 5.0` has two places, so minor moves. 5.1, 5.42, not 6.0.
- `~> 5.31.0` has three places, so patch moves. 5.31.9, not 5.32.0.

### .terraform.lock.hcl

`~> 5.0` is a range, not a version. Init in January gets 5.31.0, init in
March gets 5.42.0, and now two machines behave differently.

First `terraform init` resolves to a concrete version and writes it plus a
checksum into `.terraform.lock.hcl`:

```hcl
provider "registry.terraform.io/hashicorp/aws" {
  version     = "5.31.0"
  constraints = "~> 5.0"
  hashes = [
    "h1:...",
  ]
}
```

Everyone who clones gets 5.31.0 exactly. The hash also blocks a tampered
plugin. **Commit this file to git.** To move up deliberately:
`terraform init -upgrade`.

---

### provider block

```hcl
provider "aws" {
  region = var.region
}
```

`required_providers` said which plugin. This says how to configure it.

Region points at a variable, not a literal, so the whole stack is
relocatable with one edit.

Why single-region matters for a data pipeline:

- **Cost.** Cross-region data transfer is billed. Same-region between
  services is generally free. Lambda writes S3, Glue reads and writes S3,
  Athena reads S3. Split those across regions and every hop bills, every
  run, forever.
- **Coupling.** Glue Data Catalog is regional. Athena reads the catalog in
  its own region. Data in us-west-2 with a catalog in us-east-1 means
  Athena sees no tables at all.

**Credentials are deliberately absent.** The provider resolves them in a
chain:

1. Arguments in this block (never do this)
2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
3. `~/.aws/credentials`, which is what `aws configure` writes
4. IAM role, if running on EC2 or in CI

I'm on #3 via WSL. Never hardcode keys in a `.tf` file. Bots scan public
GitHub for AWS keys within seconds of a push.

---

### data "aws_caller_identity" "current"

```hcl
data "aws_caller_identity" "current" {}
```

**resource vs data**, the core distinction:

```hcl
resource "aws_s3_bucket" "raw"      { }  # CREATE. Terraform owns it.
data     "aws_caller_identity" "current" { }  # LOOK UP. Read-only.
```

A `resource` goes into state, gets updated when I edit config, and gets
**destroyed** if I delete the block. A `data` block creates nothing and
destroys nothing. Delete it and AWS is unchanged.

Empty `{}` because it needs no arguments. It's asking about the credentials
already in use. Other data sources do take arguments:

```hcl
data "aws_ami" "ubuntu" {
  most_recent = true
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-*"]
  }
}
```

Returns three attributes, referenced as
`data.<type>.<name>.<attribute>`:

```hcl
data.aws_caller_identity.current.account_id  # 123456789012
data.aws_caller_identity.current.arn         # arn:aws:iam::123456789012:user/jonah
data.aws_caller_identity.current.user_id
```

**Why I need the account ID.**

S3 bucket names are globally unique across every AWS customer on Earth,
not just my account. `jobmarket-raw` was taken years ago. So:

```hcl
bucket = "${var.name_prefix}-raw-${data.aws_caller_identity.current.account_id}"
# -> jobmarket-raw-123456789012
```

Guaranteed unique because nobody else has my account number.

And ARNs embed the account ID. ARN = Amazon Resource Name, the unique
address of any AWS thing:

```
arn:aws:lambda:us-east-1:123456789012:function:jobmarket-ingest
 |   |    |        |          |          |          |
 |   |    |        |          |          |          resource name
 |   |    |        |          |          resource type
 |   |    |        |          account id
 |   |    |        region
 |   |    service
 |   partition
```

Every IAM policy in `iam/` is written in terms of ARNs, so every one of
them needs this value.

Hardcoding `123456789012` instead would: weld the repo to my account, leak
the ID into git, and repeat the same string 30 places to typo.

---

## variables.tf

### Full file

```hcl
variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for AWS resource names."
  type        = string
  default     = "jobmarket"
}

variable "schedule_expression" {
  description = "Cron expression for the daily pipeline schedule (UTC). Weekdays only; job boards barely move at the weekend."
  type        = string
  default     = "cron(34 6 ? * MON-FRI *)"
}
```

Declared here, referenced elsewhere as `var.region`, `var.name_prefix`,
`var.schedule_expression`.

Variables are the public interface of the repo. Someone can run this in a
different region with a different prefix without editing any logic.

---

### The three arguments

```hcl
variable "region" {
  description = "AWS region for all resources."   # docs + interactive prompt text
  type        = string                            # validated at plan time
  default     = "us-east-1"                       # makes it optional
}
```

**`description`** shows up when Terraform prompts for a missing variable,
and gets pulled into auto-generated docs by `terraform-docs`.

**`type`** gives plan-time validation. Pass a number where a string is
expected and you get a clear Terraform error instead of a cryptic AWS API
error mid-apply. Available types:

```hcl
type = string
type = number
type = bool
type = list(string)     # ["a", "b"]
type = map(string)      # { env = "prod" }
type = set(string)      # unordered, deduped
type = object({ name = string, size = number })
```

Omitting `type` gives you `any`, which validates nothing. Always declare it.

**`default`** decides required vs optional. Has a default: optional, and
`terraform apply` runs with no config. No default: Terraform refuses to
proceed and prompts. All three of mine have defaults, so the repo is
clone-and-apply.

---

### How values get set

Precedence, low to high. Later wins:

1. `default` in the block
2. `TF_VAR_` env var: `export TF_VAR_region=us-west-2`
3. `terraform.tfvars`, auto-loaded
4. `*.auto.tfvars`, auto-loaded alphabetically
5. `-var` / `-var-file` on the CLI

A `.tfvars` file **assigns**, it doesn't declare. Different syntax:

```hcl
# variables.tf   -> DECLARE
variable "region" { type = string }

# prod.tfvars    -> ASSIGN
region      = "us-west-2"
name_prefix = "jobmarket-prod"
```

```bash
terraform apply -var-file=prod.tfvars
terraform apply -var="region=us-west-2"
```

That's how one repo drives multiple environments. Same code, different
inputs.

---

### Two arguments I don't use yet

```hcl
variable "db_password" {
  type      = string
  sensitive = true
}
```

`sensitive` redacts the value from plan and apply output. **It does not
encrypt it in state.** The plaintext still sits in `terraform.tfstate`.
Real secrets go in Secrets Manager or SSM Parameter Store, fetched at
runtime.

```hcl
variable "name_prefix" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.name_prefix))
    error_message = "Lowercase alphanumeric and hyphens only (S3 naming rules)."
  }
}
```

`validation` catches bad input at plan time with my error message, instead
of at apply time after half the stack already built. Worth adding to
`name_prefix` since an underscore or capital would fail S3 bucket naming.

---

### Why name_prefix isn't cosmetic

Threaded through every resource name, it buys four things:

**Scoped IAM.** Policies can target a wildcard instead of enumerating
every bucket:

```json
"Resource": "arn:aws:s3:::jobmarket-*"
```

Least privilege stays maintainable. Direct Domain 4 relevance.

**Cost visibility.** Cost Explorer filters by resource name. One