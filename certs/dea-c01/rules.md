# Exam Rules 

1. **Secure + audit S3 data &rarr; KMS + IAM role + CloudTrail**
    
    - **What it means:** "Secure and audited" covers three jobs. Encrypt the data, control who can use it, and log who did.

    - **Why:** KMS makes the data unreadable without permission to use the key. The IAM role is where that permission is granted. CloudTrail logs every API call, including every use of the key, which gives you the audit trail

    - **Trap:** VPC endpoints, Shield, WAF and Access Points all sounds like security, but none of them encrypt, grant access or log access.

2. **SSE-KMS AccessDenied, S3 permissions fine &rarr; kms:Decrypt**

    - **What it means:** With SSE-KMS, the key that unlocks the object lives in KMS, not S3. Reading the object means S3 has to ask KMS to decrypt it on your behalf. 

    - **Why:** That's two separate checks. S3 checks `s3:GetObject`, and KMS checks. 

    - **Trap:** A bucket policy only covers the S3 check, so it can never fix a KMS denial. Switching to SSE-S3 "fixes" it by dropping the requirement, and that is never the right answer. 

3. **One service accessing another &rarr; an IAM role it assumes:**

    - **What is means:** Glue. Lambda, EC2, EKS pods and API Gateway each get an IAM role. When the service runs, it assumes the role and STS hands it temporary credentials

    - **Why:** The credentials expire on their own (usually within a few hours) and are never written anywhere, so there's nothing to leak or rotate. Least privilege means the role's policy allows only the specific actions on the specific resources the service needs.

    - **Trap:** Any option that stores access keys is wrong: environment variables, config files, Kubernetes secrets, a file in S3. In EKS, the answer is IRSA (IAM roles for service accounts), which gives pod its own role.


4. **Cross-account &rarr; a role in the owning account:**

    - **What it means:** Account A owns the data and creates a role that can read it. 
    - **Why:** The account that owns the data keeps control. It can tighten or revoke access by editing one role, and CloudTrail in account A shows every access.
    - **Trap:** VPC peering connects networks between accounts and grants zero permission. Public bucket access "works" but is never right. A bucket policy that allows account B is a valid alternative for S3, but when a role option is offered for services like Redshift Spectrum, pick the role.

5. **Private subnet, no internet &rarr; VPC endpoint**

    - **What it means:** S3, DynamoDB and other AWS services are normally reaches over public endpoints. A private subnet has no route to them. A VPC endpoint gives your VPC a private path straight to the service.

    - **Why:** Traggic stays on AWS's network and never touches the internet. Gateway endpoints cover S3 and DynamoDB and are free. Interface endpoints (PrivateLink) cover most other services and cost a little per hour.

    - **Trap:** A NAT gateway also reaches the service, but by routing traffic out to the internet, which breaks the "no internet" rule. VPN and API Gateway proxies work too, but they're extra infrastructure you don't need.

6. **A VPC Endpoint is in the path &rarr; its policy has to allow the access**

    - **What it means:** An endpoint can carry its own policy that limits what can pass through it, like "only these buckets" or "only `GetObject`."

    - **Why:** A request through an endpoint gets three checks: the IAM role's policy, the resource policy (the bucket policy) and the endpoint policy. If any one says no, it's no. That's how companies stop data from being copied out to buckets they don't own.

    - **Trap:** You fix IAM and the bucket policy still fails. If the question mentions an endpoint, check its policy.

7. **Rotate DB passwords automatically &rarr; Secrets Manager**

    - **What it means:** Some systems need a real username and password (JDBC to Aurora, say), so a role can't replace it. Secrets Manager stores the password encrypted and rotates it on a schedule.

    - **Why:** For RDS and Aurora, rotation is built in. It changes the password on the database and in the stored secret at the same time. Your Glue job or app detches the secret at runtime, so it always gets the current password and nothing breaks.

    - **Trap:** Parameter Store can hold secrets but has no built-in rotation, so "rotate automatically" points to Secrets Manager. A Lambda that rotates the password and emails it, or a password file in S3, is extra work and adds risk 

8. **Find PII &rarr; Macie. Mask PII &rarr; DataBrea**

    - **What it means:** Macie scans S3 using pattern matching and ML and reports findings like "this bucket has 4,000 SSNs." DataBrew is a visual data-prep tool with transforms that mask, hash, or redact columns. 

    - **Why:** They do different jobs. Macie only reports. It never changes or blocks the data. DataBrew changes the data but doesn't search a whole S3 estate for PII. 

    - **Trap:** "Discover/classify sensitive data" &rarr; Macie. "Obfuscate before analysis" &rarr; DataBrew. "Control who can see it" &rarr; IAM or Lake Formation. 

9. **Permissions inside Redshift &rarr; `GRANT` to a group**

    - **What it means:** IAM controls who can connect to Redshift or get temporary credentials. What they can do once inside (which schemas, which tables, `SELECT` vs `INSERT`) is set in Redshift with SQL.

    - **Why:** `GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO GROUP analysts;` covers all 30 analysts in one line. Onboarding of offboarding someone means adding them to or removing the from the group. You never touch the grants or IAM.

    - **Trap:** Per-person IAM policies on the Data API are the overhead trap, and IAM can't restrict SQL down to `SELECT` anyway. For row- or column-level limits inside Redshift, the answer is RLS policies or column level grants.

10. **Networking answers only when the question says "private" or "no internet"**

    - **What it means:** Security groups, VPC endpoints, VPNs and peering control where traffic can go. They don't control who can read the data.

    - **Why:** Exam writers know these sound secure, so they put them in as a distractors on permission and audit questions. They're only the answer when the question is actually about network paths.

    - **Trap:** I previously picked VPC endpoint, bucket deny, Access Points, and Shield. These are network or edge answers to a permission-and-audit question.

11. **Lambda mounting EFS &rarr; inbound 2049 on the mount target's security group**

    - **What it means:** EFS mount targets have a security group. Inbound rules control who can connect to them, and EFS speak NFS on port 2049. The Lambda also has to be in the VPC to reach EFS at all.

    - **Why:** The rule is "allow TCP 2049 inbound from the Lambda's security group." Secuity groups are stateful, so once a connection is allowed in, the reply is automatically allowed back out, and you never add an outbound rule for responses.

    - **Trap:** Wrong direction (outbound) and wrong port (443 is HTTPS). Mount timeouts almost always mean the security group is blocking the connection. They're not a permissions problem.