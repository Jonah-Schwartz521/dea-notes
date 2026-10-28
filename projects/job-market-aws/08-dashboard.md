# 08 - Dashboard

Streamlit app on the VPS querying Athena. The consumption layer.

**Files:** `app.py`, `data.py`

---

## Architecture

```
VPS (Streamlit)
  -> awswrangler
    -> Athena (workgroup: jobmarket)
      -> Glue Data Catalog
        -> S3 curated/posting_skills/  (Parquet)
```

**The dashboard runs outside AWS.** That's what makes
`iam/dashboard-reader.json` a different security problem from every other
role in this stack. Service roles are assumed by AWS services with automatic
credential rotation. An external VPS needs credentials that actually travel
to it.

The right answer is IAM Roles Anywhere or a federated identity. The common
answer is a long-lived access key on the box, which is exactly the pattern
the exam marks wrong everywhere else. Worth checking which one you did.

---

## awswrangler

```python
import awswrangler as wr

wr.athena.read_sql_query(sql, database=DB, s3_output=OUT, ctas_approach=False)
```

**AWS SDK for pandas** (formerly awswrangler). Wraps boto3 to return pandas
DataFrames directly from Athena, Redshift, S3, and Glue. Handles the
submit-poll-fetch cycle that raw boto3 would make you write.

### ctas_approach

This parameter is the interesting one and it maps straight to exam content.

| | `ctas_approach=True` (default) | `ctas_approach=False` |
|---|---|---|
| Mechanism | wraps your SQL in `CREATE TABLE AS SELECT` | plain query, reads the CSV result |
| Result format | Parquet | CSV |
| Types | preserved | everything is a string until pandas guesses |
| Speed | faster on large results | faster on small results |
| Side effects | creates a temp table, needs write + Glue perms | none |

**Setting it to `False` is right here.** Results are 25 rows. CTAS would
create a Glue table, write Parquet, read it, and drop the table, which is
pure overhead at this size. It also means the dashboard role needs no write
or catalog-create permissions, which is a real security win.

At tens of thousands of rows the answer flips.

---

## Caching

```python
@st.cache_data(ttl=3600)
def top_skills(ingest_date, limit=25, category=None):
```

**`@st.cache_data` memoizes on the function's arguments.** Same arguments
within the TTL means no Athena call at all.

This matters more than it looks. Streamlit **re-runs the entire script on
every widget interaction.** Move a slider, change a dropdown, the whole file
executes top to bottom. Without caching, every click is a fresh Athena query.

At $5/TB and a 10 MB minimum per query, an uncached dashboard with a few
users clicking around is a genuine cost problem. This is the mitigation.

One-hour TTL is well matched to a pipeline that runs once daily. You could
push it much higher.

**Note the ordering bug in `app.py`:**

```python
@st.cache_data(ttl=3600)
def q(sql):
    return wr.athena.read_sql_query(...)
```

The decorator is on the generic `q(sql)` function, keyed on the SQL string.
Works, but it's brittle: any whitespace change produces a cache miss.
`data.py` does it correctly by caching the named functions with semantic
arguments.

---

## The SQL injection problem

```python
def top_skills(ingest_date: str, limit: int = 25, category: str | None = None):
    cat = f"AND category = '{category}'" if category else ""
    return _q(f"""
        ...
        WHERE ingest_date = '{ingest_date}' {cat}
        ...
        LIMIT {limit}
    """)
```

**Every query in `data.py` builds SQL by string interpolation.** This is the
textbook injection pattern.

```python
skill_trend("'; DROP TABLE posting_skills; --")
```

Athena won't drop a table from a `SELECT` context, and the values currently
come from your own dropdowns rather than free text. So the practical risk
today is low. But:

- Dropdown values come from `ingest_dates()`, which reads from the database.
  Data-driven input, not hardcoded.
- Add one text input for skill search and the exposure becomes real.
- Athena supports `DROP TABLE` and `INSERT INTO`. It is not read-only.

**awswrangler supports parameterized queries:**

```python
wr.athena.read_sql_query(
    "SELECT ... WHERE ingest_date = :date AND category = :cat",
    database=DB,
    params={"date": ingest_date, "cat": category},
    paramstyle="named",
)
```

Worth doing. It's a small change and it's the difference between "this is
fine because of how I happen to call it" and "this is safe."

**The exam angle:** SQL injection isn't heavily tested on DEA-C01, but
parameterized queries and least-privilege query roles are the standard
answers when it comes up. The related and more testable point is that
**Athena permissions should be read-only for a BI consumer**, so even a
successful injection can't write.

---

## The hardcoded date

```python
# app.py
WHERE ingest_date = '2026-08-31'
```

Frozen. This dashboard shows the same day forever.

`data.py` has `ingest_dates()` returning the available dates for a picker,
so the intent was clearly a selector that `app.py` never wired up. Looks
like `app.py` is a prototype and `data.py` is the real query layer that
hasn't been connected yet.

**The partition filter itself is correct and important.** Every query in
both files filters on `ingest_date`, which is the partition key. That's what
keeps Athena scanning one day instead of the whole table, and it's why the
1 GB workgroup cutoff never fires.

The exception:

```python
def skill_trend(canonical_name: str):
    SELECT ingest_date, count(DISTINCT ats_job_id)
    FROM posting_skills
    WHERE canonical_name = '{canonical_name}'
    GROUP BY ingest_date
```

**No partition filter.** By design, since a trend needs every date. But it
means a full table scan every time, growing linearly forever.

At current volume that's a few MB. In two years it's still small. But it's
the one query that will eventually hit the cutoff, and the fix would be a
pre-aggregated summary table written by a nightly Glue job rather than
scanning raw history on every dashboard load.

**That's the general pattern:** aggregate once during transform, query the
aggregate many times. Recomputing the same aggregation on every page view is
the most common cost mistake in Athena-backed dashboards.

---

## Query patterns

```sql
count(DISTINCT ats_job_id) AS postings
```

Correct, and it reflects the bridge table's grain. `posting_skills` has one
row per `(posting, skill)`, so `count(*)` grouped by skill would work, but
`count(DISTINCT ats_job_id)` is explicit about what's being counted and
survives any future change that adds rows per posting.

Worth noting `ats_job_id` alone isn't globally unique across ATSes. Two
companies could theoretically collide. The true key is
`(ats, company, ats_job_id)`. Not a problem in practice, but a latent one.

```sql
GROUP BY 1, 2       -- app.py, ordinal positions
GROUP BY canonical_name, category   -- data.py, explicit
```

`data.py`'s version is better. Ordinals break silently when you reorder the
SELECT list.

**Everything reads `posting_skills` and nothing reads `postings`.** So
`title`, `salary_min`, `salary_max`, `role_category`, and `location_norm`
are all computed by the pipeline and never displayed. The `role_category`
classifier in `roles.py` is doing real work for no consumer yet.

That's the biggest unrealized value in the project. A join of
`posting_skills` to `postings` on `(ats, company, ats_job_id)` would let you
answer "which skills appear in data engineering roles specifically" or
"what's the salary spread for postings mentioning Spark," which is a much
more interesting question than a raw skill count.

---

## Config via environment

```python
DB  = os.environ["ATHENA_DATABASE"]
OUT = os.environ["ATHENA_OUTPUT"]
```

Same required-config pattern as `handler.py`. Brackets, not `.get()`, so a
missing variable crashes at import rather than failing mysteriously later.

**Note the workgroup is not specified.** `wr.athena.read_sql_query` accepts a
`workgroup=` parameter, and without it queries run in `primary`, not
`jobmarket`.

That means the dashboard gets **none of the workgroup's protections**: no 1 GB
scan cutoff, no enforced result location, no encryption enforcement, and its
usage doesn't show up in the workgroup's CloudWatch metrics.

Which makes the `bytes_scanned_cutoff_per_query` in `athena.tf` decorative
for the one client that actually queries in volume. Worth adding:

```python
wr.athena.read_sql_query(..., workgroup="jobmarket")
```

Small fix, closes a real gap between what the infrastructure enforces and
what the application does.

---

## Where a dashboard belongs, for the exam

The comparison AWS actually asks about:

| | Athena direct | QuickSight | Redshift |
|---|---|---|---|
| Latency | seconds | seconds, sub-second with SPICE | sub-second |
| Concurrency | limited, queues | SPICE handles many users | high |
| Cost model | per TB scanned | per user or per session | per hour |
| Caching | none built-in | SPICE in-memory | result cache |

**QuickSight is the AWS-native BI answer.** SPICE is its in-memory cache, and
its whole purpose is exactly the problem your `@st.cache_data` solves by
hand: don't re-scan S3 for every dashboard view.

If a scenario says "business users need interactive dashboards over a data
lake without repeatedly scanning S3," the answer is QuickSight with SPICE.

Streamlit on a VPS is the right call for a personal project. You control it,
it's free, and you already have the box. But know the managed answer.

---

# Exam angles

**Domain 2, Data Store Management (26%)**
- Partition filters as the primary Athena cost control
- Pre-aggregated summary tables vs recomputing on every query
- Bridge table grain and `count(DISTINCT ...)`
- CTAS as a materialization strategy, and when the overhead isn't worth it

**Domain 3, Operations (22%)**
- Caching to reduce query volume and cost
- Workgroup assignment as the enforcement point for query governance
- **QuickSight + SPICE as the managed dashboard answer**

**Domain 4, Security (18%)**
- **External compute needs credentials: IAM Roles Anywhere, not static keys**
- Read-only query roles for BI consumers
- Parameterized queries over string interpolation
- Query results as sensitive output

---

# Decision rules

```
BI over a data lake, many users     -> QuickSight + SPICE
Ad-hoc SQL, few users               -> Athena direct
Small result set                    -> ctas_approach=False
Large result set, typed             -> ctas_approach=True
Repeated identical query            -> cache, or a summary table
Trend over all history              -> pre-aggregate, don't scan every time
User-supplied query values          -> parameterized, never f-strings
External app querying AWS           -> IAM Roles Anywhere, not static keys
Enforce scan limits on a client     -> assign the workgroup explicitly
```

---

# Gaps

- [ ] **Pass `workgroup="jobmarket"` so the scan cutoff actually applies**
- [ ] **Parameterize queries instead of f-string interpolation**
- [ ] `app.py` has a hardcoded date; wire up `ingest_dates()` as a picker
- [ ] `skill_trend` full-scans; needs a pre-aggregated summary table
- [ ] Nothing joins to `postings`, so role_category and salary are unused
- [ ] Check how the VPS authenticates: static keys or Roles Anywhere
- [ ] `dashboard-reader.json` should be read-only, verify
- [ ] `ats_job_id` isn't globally unique; the real key is (ats, company, id)