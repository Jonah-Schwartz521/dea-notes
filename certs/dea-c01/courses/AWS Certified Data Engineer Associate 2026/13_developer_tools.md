# Developer Tools

How you reach AWS from code and the terminal (access keys, CLI, SDK), how you define infrastructure in code (CDK, SAM), and how you ship code automatically (CodeCommit, CodeBuild, CodeDeploy, CodePipeline). For a data engineer, this is how Glue jobs, Lambda functions, Step Functions, and their infrastructure get deployed without clicking through the console. Exam tells are inline, and there's a decision cheat sheet at the end.

## Contents

- [AWS Access Keys, CLI, & SDK](#aws-access-keys-cli--sdk)
  - [How can users access AWS?](#how-can-users-access-aws)
  - [Example (fake) access keys](#example-fake-access-keys)
  - [Access key best practices](#access-key-best-practices)
  - [What's the AWS CLI?](#whats-the-aws-cli)
  - [What's the AWS SDK?](#whats-the-aws-sdk)
  - [How credentials are found](#how-credentials-are-found)
  - [AWS CloudShell](#aws-cloudshell)
- [AWS Cloud Development Kit (CDK)](#aws-cloud-development-kit-cdk)
  - [Constructs](#cdk-constructs)
  - [CDK in a diagram](#cdk-in-a-diagram)
  - [CDK commands](#cdk-commands)
- [AWS Serverless Application Model (SAM)](#aws-serverless-application-model-sam)
  - [CDK vs SAM](#cdk-vs-sam)
  - [CDK + SAM](#cdk--sam)
- [CI/CD Tools](#cicd-tools)
  - [AWS CodeCommit](#aws-codecommit)
  - [AWS CodeBuild](#aws-codebuild)
  - [AWS CodeDeploy](#aws-codedeploy)
  - [AWS CodePipeline](#aws-codepipeline)
  - [AWS CodeArtifact](#aws-codeartifact)
- [CI/CD for data pipelines](#cicd-for-data-pipelines)
- [Developer tools exam tells](#developer-tools-exam-tells)
- [Decision cheat sheet](#decision-cheat-sheet)

---

## AWS Access Keys, CLI, & SDK

### How can users access AWS?

- To access AWS, you have three options:
  - **AWS Management Console**: protected by password + MFA
  - **AWS Command Line Interface (CLI)**: protected by access keys
  - **AWS Software Development Kit (SDK)**: for code, protected by access keys
- Access Keys are generated through the AWS Console
- Users manage their own access keys
- <u>**Access Keys are secret, just like a password. Don't share them**</u>
- Access Key ID ~= username
- Secret Access Key ~= password

### Example (Fake) Access Keys

| Access key ID | Created | Last used | Status |
|---|---|---|---|
| `AKIASK4E37PV4TU3RD6C` | 2020-05-25 15:13 UTC+0100 | N/A | Active |

- Access key ID: `AKIASK4E37PV4983d6C`
- Secret Access Key: `AZPN3zojWozWCndIjhB0Unh8239a1bzbzO5fqqkZq`
- <u>**Remember: don't share your access keys**</u>

> Use access keys to make secure REST or HTTP Query protocol requests to AWS service APIs. For your protection, you should never share your secret keys with anyone. As a best practice, AWS recommends frequent key rotation.

- Long-term IAM user keys start with **`AKIA`**. Temporary (STS) keys start with **`ASIA`** and come with a session token

### Access key best practices

Access keys are **long-term credentials**, and AWS now recommends avoiding them where you can:

| Situation | Use instead of access keys |
|---|---|
| Code running on EC2, Lambda, ECS, Glue, EMR | **IAM role** (temporary credentials, rotated automatically) |
| Humans using the CLI | **IAM Identity Center** (`aws sso login`) for short-lived credentials |
| Quick commands in the browser | **CloudShell** |
| Another AWS account or external IdP | **STS AssumeRole** / identity federation |

If you must use access keys:

- **Never** create access keys for the **root user**
- Never hardcode them in code or commit them to Git
- Rotate them: each IAM user can have **2 access keys** at once, so you create the new one, switch over, then deactivate and delete the old one
- Use the IAM **credential report** and "last used" info to find stale keys

### What's the AWS CLI?

- A tool that enables you to interact with AWS services using commands in your command-line shell
- Direct access to the public APIs of AWS services
- You can develop scripts to manage your resources
- It's open-source: https://github.com/aws/aws-cli
- Alternative to using AWS Management Console

```bash
$ aws configure          # stores keys + default region in ~/.aws/credentials and ~/.aws/config
$ aws s3 cp myfile.txt s3://ccp-mybucket/myfile.txt
upload: ./myfile.txt to s3://ccp-mybucket/myfile.txt

$ aws s3 ls s3://ccp-mybucket
2021-05-14 03:22:52          0 myfile.txt
```

- Use **named profiles** (`--profile dev`) to switch between accounts and roles
- `aws s3 sync` copies only changed files (handy for pushing Glue scripts or MWAA DAGs to S3)

### What's the AWS SDK?

- AWS Software Development Kit (AWS SDK)
- Language-specific APIs (set of libraries)
- Enables you to access and manage AWS services programmatically
- Embedded within your application
- Supports
  - SDKs (JavaScript/Node.js, Python, PHP, .NET, Ruby, Java, Go, C++, Rust, Kotlin, ...)
  - Mobile SDKs (Android, iOS, ...)
  - IoT Device SDKs (Embedded C, Arduino, ...)
- Example: AWS CLI is built on the AWS SDK for Python (**botocore**, the same core that **boto3** uses)
- The Python SDK (**boto3**) is the one you'll see in Lambda functions and Glue jobs

```mermaid
flowchart TD
    subgraph APP["Your Application"]
        SDK["📦 AWS SDK"]
    end
    SDK -- "API calls (signed with credentials)" --> AWS["AWS services"]
```

### How credentials are found

The CLI and SDKs look for credentials in a fixed order (the **credential provider chain**) and use the first one they find:

1. Command-line options / explicit credentials in code
2. **Environment variables** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
3. Shared files: `~/.aws/credentials` and `~/.aws/config` (including SSO profiles)
4. Container credentials (ECS task role)
5. **EC2 instance profile** (instance role, via the instance metadata service)

This is why code on EC2 or Lambda "just works" with no keys: it falls through to the role. It's also why a stray environment variable can override the role you expected.

### AWS CloudShell

- A browser-based shell inside the AWS Console
- **Pre-authenticated** with your console credentials, CLI already installed
- 1 GB of persistent storage per region
- Good for quick commands without setting up keys on your laptop

---

## AWS Cloud Development Kit (CDK)

- Define your cloud infrastructure using a familiar language:
  - JavaScript/TypeScript, Python, Java, .NET, and Go
- Contains high level components called **constructs**
- The code is "compiled" (synthesized) into a CloudFormation template (JSON/YAML)
- You can therefore deploy infrastructure and application runtime code together
  - Great for Lambda functions
  - Great for Docker containers in ECS / EKS

```typescript
export class MyEcsConstructStack extends core.Stack {
  constructor(scope: core.App, id: string, props?: core.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, "MyVpc", {
      maxAzs: 3 // Default is all AZs in region
    });

    const cluster = new ecs.Cluster(this, "MyCluster", {
      vpc: vpc
    });

    // Create a load-balanced Fargate service and make it public
    new ecs_patterns.ApplicationLoadBalancedFargateService(this, "MyFargateService", {
      cluster: cluster, // Required
      cpu: 512, // Default is 256
      desiredCount: 6, // Default is 1
      taskImageOptions: { image: ecs.ContainerImage.fromRegistry("amazon/amazon-ecs-sample") },
      memoryLimitMiB: 2048, // Default is 512
      publicLoadBalancer: true // Default is false
    });
  }
}
```

The highlighted constructs on the slide are `ec2.Vpc`, `ecs.Cluster`, and `ecs_patterns.ApplicationLoadBalancedFargateService`. The last one is a high-level "pattern" construct that creates the ALB, Fargate service, task definition, and security groups in one line.

Note: this slide uses CDK **v1** syntax (`core.Stack`). In CDK **v2** (the current version) everything ships in one package, `aws-cdk-lib`, so you'd write `import { Stack } from 'aws-cdk-lib'`.

### CDK constructs

| Level | What it is | Example |
|---|---|---|
| **L1** | 1:1 with a CloudFormation resource, every property manual. Named `Cfn*` | `CfnBucket` |
| **L2** | Curated resource with sensible defaults and helper methods | `s3.Bucket`, `bucket.grantRead(role)` |
| **L3** (patterns) | Multiple resources wired together for a common architecture | `ApplicationLoadBalancedFargateService` |

Helpers like `grantRead()` generate least-privilege IAM policies for you, which is a big reason people pick CDK.

### CDK in a diagram

```mermaid
flowchart LR
    subgraph APP["CDK Application"]
        C["CDK Application Constructs<br/>(Lambda, DynamoDB, S3, ECS, ...)"]
        PL["Programming Languages<br/>Python · TypeScript · Java · .NET · Go"]
    end

    APP --> CLI["CDK CLI<br/>cdk synth"]
    CLI --> T["CloudFormation Template"]
    T --> CF["CloudFormation"]
```

### CDK commands

| Command | What it does |
|---|---|
| `cdk init` | Create a new CDK project |
| `cdk bootstrap` | **One-time per account/region**: creates the `CDKToolkit` stack (S3 bucket, ECR repo, IAM roles) CDK needs to deploy assets |
| `cdk synth` | "Compiles" your CDK code into a CloudFormation template |
| `cdk diff` | Shows what would change versus the deployed stack |
| `cdk deploy` | Synthesizes and deploys the stack through CloudFormation |
| `cdk destroy` | Deletes the stack |

Classic error: "This stack uses assets, so the toolkit stack must be deployed" means you forgot **`cdk bootstrap`**.

---

## AWS Serverless Application Model (SAM)

- Framework for building **serverless** applications
- Templates are YAML/JSON, a **shorthand on top of CloudFormation**
  - The template header `Transform: AWS::Serverless-2016-10-31` tells CloudFormation to expand SAM resources
  - Resource types: `AWS::Serverless::Function`, `AWS::Serverless::Api`, `AWS::Serverless::SimpleTable`, `AWS::Serverless::StateMachine`
- A few lines of SAM expand into many CloudFormation resources (function, role, permissions, event source mappings)
- **SAM CLI** can run Lambda and API Gateway **locally** for testing
- Can use **CodeDeploy** to shift traffic gradually to new Lambda versions (canary / linear)

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Resources:
  ProcessFileFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.handler
      Runtime: python3.12
      Events:
        NewFile:
          Type: S3
          Properties:
            Bucket: !Ref RawBucket
            Events: s3:ObjectCreated:*
  RawBucket:
    Type: AWS::S3::Bucket
```

| SAM CLI command | What it does |
|---|---|
| `sam init` | Start a project from a template |
| `sam build` | Build dependencies and package code |
| `sam local invoke` | Run a function locally (in Docker) |
| `sam local start-api` | Run API Gateway + Lambda locally |
| `sam deploy --guided` | Package to S3 and deploy through CloudFormation |
| `sam sync` | Fast sync of code changes to the cloud for dev |

### CDK vs SAM

- **SAM:**
  - Serverless focused
  - Write your template declaratively in JSON or YAML
  - Great for quickly getting started with Lambda
  - Leverages CloudFormation
- **CDK:**
  - All AWS services
  - Write infra in a programming language: JavaScript/TypeScript, Python, Java, .NET, and Go
  - Leverages CloudFormation

| | SAM | CDK |
|---|---|---|
| Scope | Serverless (Lambda, API Gateway, DynamoDB, Step Functions) | All AWS services |
| Written in | JSON / YAML (declarative) | Programming language (imperative) |
| Under the hood | CloudFormation | CloudFormation |
| Local testing | Built in (`sam local`) | Through SAM CLI after `cdk synth` |
| Best for | Quick-start serverless apps | Complex infra, reusable constructs |

### CDK + SAM

- You can use SAM CLI to locally test your CDK apps
- You must first run `cdk synth`

```mermaid
flowchart LR
    subgraph APP["CDK Application (MyCDKApp)"]
        L["λ Lambda function<br/>(myFunction)"]
    end

    APP --> S["cdk synth"]
    S --> T["CloudFormation Template<br/>(MyCDKStack.template.json)"]
    SAM["SAM CLI"] --> T
```

```bash
sam local invoke -t MyCDKStack.template.json myFunction
```

---

## CI/CD Tools

The full pipeline, and which service does each step:

```mermaid
flowchart LR
    subgraph CP["CodePipeline: orchestration layer"]
        SRC["Source<br/>CodeCommit / GitHub / S3"] --> BLD["Build + Test<br/>CodeBuild"]
        BLD --> APR["Manual approval<br/>(optional)"]
        APR --> DEP["Deploy<br/>CodeDeploy / CloudFormation / Elastic Beanstalk"]
    end
```

### AWS CodeCommit

- AWS-managed private **Git** repositories
- Integrates with IAM for access control, encrypted at rest with KMS
- Note: AWS paused new customer onboarding to CodeCommit in 2024. Many teams use **GitHub, GitLab, or Bitbucket** through **AWS CodeConnections** (formerly CodeStar Connections) as the pipeline source instead. The exam may still show CodeCommit in diagrams

### AWS CodeBuild

- Code building service in the cloud (name is obvious)
- Compiles source code, runs tests, and produces packages that are ready to be deployed (by CodeDeploy for example)
- Build instructions live in a **`buildspec.yml`** file at the root of your repo (phases: `install`, `pre_build`, `build`, `post_build`, plus `artifacts`)
- Each build runs in a fresh **Docker container** (AWS-managed images or your own)
- Sources: CodeCommit, GitHub, Bitbucket, S3
- Output artifacts go to **S3**, logs go to **CloudWatch Logs** and/or S3
- Can run **inside your VPC** to reach private resources (e.g. a test database)
- Secrets come from **Secrets Manager** or **Parameter Store**, referenced in the buildspec

```mermaid
flowchart LR
    CC["CodeCommit"] -- "Retrieve code" --> CB["CodeBuild"]
    CB -- "Build code" --> A["Ready-to-deploy artifact"]
```

```yaml
version: 0.2
phases:
  install:
    commands:
      - pip install -r requirements.txt
  build:
    commands:
      - pytest tests/
      - aws s3 cp src/glue_job.py s3://my-scripts-bucket/glue/
artifacts:
  files:
    - template.yaml
```

- Benefits:
  - Fully managed, serverless
  - Continuously scalable & highly available
  - Secure
  - Pay-as-you-go pricing: only pay for the build time

### AWS CodeDeploy

- We want to deploy our application automatically
- Works with EC2 Instances
- Works with On-Premises Servers
- <u>Hybrid</u> service
- Servers / Instances must be provisioned and configured ahead of time with the **CodeDeploy Agent**
  - The agent is only needed for **EC2 / on-premises**
- Also deploys to **Lambda** and **ECS**, with no agent, by shifting traffic between versions
- Deployment steps defined in an **`appspec.yml`** file
- Can **automatically roll back** on failure or when a **CloudWatch alarm** fires

```mermaid
flowchart LR
    subgraph EC2["EC2 Instances being upgraded"]
        A1["v1"] --> B1["v2"]
        A2["v1"] --> B2["v2"]
        A3["v1"] --> B3["v2"]
    end

    subgraph ONP["On-premises Servers being upgraded"]
        S1["🖥️ v1"] --> S2["🖥️ v2"]
    end
```

Deployment strategies:

| Platform | Options |
|---|---|
| EC2 / on-prem | **In-place** (`AllAtOnce`, `HalfAtATime`, `OneAtATime`) or **blue/green** |
| Lambda | **Canary** (e.g. 10% for 5 min, then 100%), **Linear** (e.g. +10% every minute), **AllAtOnce** |
| ECS | Blue/green with canary, linear, or all-at-once traffic shifting |

### AWS CodePipeline

- Orchestrate the different steps to have the code automatically pushed to production
  - Code => Build => Test => Provision => Deploy
  - Basis for CICD (Continuous Integration & Continuous Delivery)
- Benefits:
  - Fully managed, compatible with CodeCommit, CodeBuild, CodeDeploy, Elastic Beanstalk, CloudFormation, GitHub, 3rd-party services & custom plugins...
  - Fast delivery & rapid updates
- Made of **stages** (Source, Build, Deploy...), each with one or more **actions**
- Artifacts are passed between stages through an **S3 artifact bucket**
- **Manual approval** action pauses the pipeline until someone approves (often via SNS email)
- Triggered automatically on a code push (or by EventBridge, or on a schedule)
- Pipeline state changes emit **EventBridge** events, so you can alert on failures

```mermaid
flowchart LR
    subgraph CP["CodePipeline: orchestration layer"]
        CC["CodeCommit"] --> CB["CodeBuild"]
        CB --> CD["CodeDeploy"]
        CD --> EB["Elastic Beanstalk"]
    end
```

### AWS CodeArtifact

- Managed **package repository** for your dependencies (pip, npm, Maven, NuGet...)
- Proxies and caches public repositories (PyPI, npm), so builds keep working even if the public repo is down
- Share internal libraries across teams (e.g. a shared Python package for your Glue jobs)

---

## CI/CD for data pipelines

How this applies to data engineering work:

- **Infrastructure** (buckets, Glue jobs, crawlers, Step Functions, IAM roles) is defined in **CloudFormation, CDK, SAM, or Terraform**
- **Glue job scripts** live in Git, CodeBuild runs unit tests, then copies them to the S3 scripts location
- **MWAA DAGs**: CI copies the DAG files to the environment's S3 `dags/` folder
- **Step Functions** state machines are deployed as code (SAM has `AWS::Serverless::StateMachine`)
- Use separate **dev / staging / prod** stacks or accounts, with a **manual approval** before prod
- Lambda versions and aliases with **CodeDeploy canary** releases let you roll out a new transform gradually

```mermaid
flowchart LR
    GIT["Git push<br/>(Glue scripts + IaC)"] --> CP["CodePipeline"]
    CP --> CB["CodeBuild<br/>unit tests, package"]
    CB --> DEV["Deploy to dev<br/>(CloudFormation / CDK)"]
    DEV --> AP["Manual approval"]
    AP --> PROD["Deploy to prod"]
```

---

## Developer tools exam tells

| Scenario | Answer |
|---|---|
| App on EC2 / Lambda / Glue needs to call AWS APIs | **IAM role**, never access keys in code |
| Developers need CLI access with short-lived credentials | IAM Identity Center (`aws sso login`) |
| Rotate an access key without downtime | Create second key, switch, deactivate, delete old one |
| Run AWS CLI commands without configuring credentials locally | CloudShell |
| Code on EC2 is using the wrong credentials | Check environment variables (they win over the instance role) |
| Define infrastructure in Python or TypeScript | CDK |
| `cdk deploy` fails asking for a toolkit stack | Run `cdk bootstrap` |
| Preview CDK changes before deploying | `cdk diff` |
| Quickly build and deploy a serverless app in YAML | SAM |
| Test a Lambda function locally | SAM CLI (`sam local invoke`) |
| Test a CDK Lambda locally | `cdk synth`, then `sam local invoke -t <template>` |
| Compile code and run tests in the cloud | CodeBuild (`buildspec.yml`) |
| Deploy to EC2 and on-premises servers | CodeDeploy (with the agent) |
| Gradually shift traffic to a new Lambda version | CodeDeploy canary / linear (often via SAM) |
| Roll back automatically if errors spike after deploy | CodeDeploy + CloudWatch alarm |
| Automate source → build → deploy on every commit | CodePipeline |
| Require sign-off before production | CodePipeline manual approval action |
| Use GitHub as the pipeline source | CodeConnections (formerly CodeStar Connections) |
| Private repository for pip/npm packages | CodeArtifact |

---

## Decision cheat sheet

**Accessing AWS**

| Need | Use |
|---|---|
| Humans, clicking | Console (password + MFA) |
| Humans, terminal | CLI with IAM Identity Center |
| Code running on AWS | SDK + IAM role |
| Quick terminal in the browser | CloudShell |

**Infrastructure as code**

| Need | Use |
|---|---|
| Declarative YAML/JSON, any service | CloudFormation |
| Real programming language, any service | CDK |
| Serverless shorthand + local testing | SAM |

**CI/CD**

| Step | Service | Config file |
|---|---|---|
| Source | CodeCommit / GitHub (CodeConnections) | n/a |
| Build + test | CodeBuild | `buildspec.yml` |
| Deploy | CodeDeploy | `appspec.yml` |
| Orchestrate it all | CodePipeline | Pipeline definition |
| Store packages | CodeArtifact | n/a |