# Security

Who can do what, where credentials live, and what the framework trusts. The
access model is implemented in
[`infra/terraform/stacks/governance`](../infra/terraform/stacks/governance)
and the job permissions in [`databricks.yml`](../databricks.yml); this page
explains the reasoning.

## Identities

Three groups for people and two service principals for automation. Nobody
gets a privilege as an individual.

| Identity | Kind | Purpose |
| --- | --- | --- |
| `etl-platform-admins` | Group | Own the catalog, the jobs and the secret scope |
| `etl-data-engineers` | Group | Develop and debug pipelines |
| `etl-data-analysts` | Group | Query published data |
| `etl-runner-<env>` | Service principal | The identity every job runs as |
| `etl-deployer-<env>` | Service principal | CI/CD: deploys the bundle from GitHub Actions |

The groups are account-wide and shared by every environment; what differs per
environment is what they may do there. Terraform creates them or looks them
up when they already exist, which is the usual case where groups are synced
from Microsoft Entra ID. Group membership is never managed in this repository.

Workspace entitlements are explicit as well: engineers get workspace and SQL
access, analysts get SQL access only, and the service principals get workspace
access and nothing more.

Splitting automation into a **runner** and a **deployer** is the core of the
model. The deployer can change what runs but holds no data privilege. The
runner can read and write data but has no credential anyone could use from
outside: no secret and no federation policy exist for it, so it only ever
acts as the identity of a job. Both exist per environment, so nothing issued
for `test` is valid in `prod`.

This narrows what a compromised CI pipeline can do, but does not make it
harmless: whoever can deploy a job that runs as the runner can make the
runner do things. That is why production deployments are gated by required
reviewers on the GitHub environment, and why the federation policy trusts one
repository and one environment and nothing else.

## Data access

One Unity Catalog catalog per environment (`etl_dev`, `etl_test`, `etl_prod`),
each with the same schemas: `bronze`, `silver`, `gold`, `quarantine`,
`etl_audit` and `etl_state`. Each catalog, with its storage credential and
external locations, is bound to its own environment's workspace, so a grant
alone is not enough to reach production data from the dev workspace.

| Principal | Non-production | Production |
| --- | --- | --- |
| Platform admins | All privileges on the catalog | All privileges on the catalog |
| Runner | Read, write and create tables in every schema | Same |
| Data engineers | Read, write and create tables in every schema | Read-only in every schema, including the audit trail |
| Data analysts | Read `gold` | Read `gold` |
| Deployer | None | None |

The grants are authoritative: Terraform owns the complete grant list of the
catalog and of every schema, so a privilege granted by hand is removed on the
next apply and this table is the access model, not a lower bound of it. It is
asserted by a test in the governance stack, in both modes.

Consequences worth stating:

- **No person writes to production data.** Production changes are pipeline
  changes, made in a pull request and executed by the runner.
- **Engineers can still debug production**: they can read every layer, the
  quarantine tables and the audit trail, and they can see job runs.
- **Analysts never see raw or intermediate data**, only what a pipeline
  published through a schema contract.

## Jobs and their configuration

| Environment | Admins | Engineers |
| --- | --- | --- |
| `test` | Manage | Run |
| `prod` | Manage | View |

A job runs with the runner's privileges and does whatever its YAML says, so
the folder the job reads its configuration from is as sensitive as the job
definition. The bundle is deployed to the deployer's own workspace folder,
not to `/Workspace/Shared`, where every workspace user could edit the files.
The runner is granted read access to that folder, which is all a job needs.

## Authentication

- **People** sign in through the workspace's identity provider.
- **CI/CD** uses GitHub OIDC workload identity federation. The deployer
  service principal has a federation policy that trusts tokens issued to one
  repository and one GitHub environment. The workflow exchanges that token
  for a short-lived Databricks token; there is no client secret or personal
  access token to store, leak or rotate.
- **Jobs** run as the runner service principal (`run_as` in the bundle). The
  deployer is allowed to assign that identity to the jobs it deploys, and
  nothing else.

## Secrets

Azure Key Vault is the system of record for source-system credentials, under
Azure RBAC and purge protection. Jobs do not read the vault. They read a
Databricks secret scope (`source-systems` by default) into which an operator
or a pipeline copies the values; Terraform never handles a secret value, so
none ends up in its state. The runner can read the scope, engineers can read
it in non-production only, and the deployer cannot read it at all.

The scope is Databricks-backed rather than Key Vault-backed on purpose: a
Key Vault-backed scope requires the vault to use access policies instead of
Azure RBAC, and gives every reader of the scope every secret in the vault.
The cost is that a rotated credential has to be copied again.

Pipelines carry references, `${secret:<scope>/<key>}`, never values. The
framework resolves a reference when the step that uses it executes and at no
other time. What that guarantees:

| Place | Contains |
| --- | --- |
| The YAML file, Git history, pull requests | The reference |
| Validation, dry-run plan, generated docs | The reference; no secret is read |
| Run summary returned to the orchestrator, lineage, audit tables | The reference |
| Structured log lines, error messages, raised exceptions | Resolved values replaced by `***` |
| The DataFrame reader or writer of the one step that needs it | The value |

A failure deserves its own line. When a step that used a secret fails, or any
failure quotes a resolved value, the error that leaves the framework carries
its type and its masked message and nothing else: no chained exception and no
traceback of the failing step. Both can hold the value, the chain in the text
of the Spark error and the traceback in the local variables of its frames,
which error reporters collect.

Limits you should know about:

- Masking recognizes a resolved value exactly as it was resolved (and as it
  appears inside a JSON string). A value that a driver or service transforms
  before echoing it (percent-encoded in a URL, base64 in a header, a trailing
  newline trimmed) is not recognized.
- Masking replaces text, so a secret that is also an ordinary word is masked
  wherever that word occurs in a log line. Values shorter than four
  characters are not masked at all, because they occur inside other words.
  Keep only real credentials behind secret references; a user name is a
  parameter.
- The run summary is not masked. It is built from the configuration as
  written, so it carries references, and its names have to reach the
  orchestrator unchanged.
- Runtime parameters are not secrets. They are substituted into the
  configuration as text and appear wherever they were used: in the plan, in
  lineage, in the summary. The `parameters` block of the summary masks names
  that look like credentials, as a safety net and not as a feature to rely
  on.

## What the framework trusts

- **Configurations are code.** They are reviewed, versioned, and deployed
  from Git. `yaml.safe_load` semantics are used throughout, so a YAML file
  cannot construct arbitrary Python objects.
- **Runtime parameters are trusted input.** They are substituted into the
  configuration as text, including into SQL expressions such as
  `condition: "order_date = '${business_date}'"`. They must come from the
  orchestrator. Never wire a value typed by an end user into a parameter.
- **Free-form SQL is opt-in** per pipeline (`settings.allow_sql: true`),
  which makes its use visible in review.
- **Operation plugins are code.** A package that declares an
  `etl_framework.operations` entry point runs inside the job. Treat it like
  any other dependency of the job's environment.

## Audit trail

- Per pipeline, the framework records every run, step and error in Delta
  tables in `etl_audit`, and stamps loaded rows with `_run_id` and
  `_load_timestamp` when `settings.audit_columns` is set, so a row in a table
  can be traced to the run that wrote it.
- Platform-level events (who queried what, who changed a grant, who ran a
  job) are recorded by Unity Catalog and the workspace, outside the
  framework.

## Supply chain

- One runtime dependency (PyYAML). Spark and Delta come from the Databricks
  runtime.
- GitHub Actions are pinned to commit SHAs and updated by Dependabot;
  workflows declare the minimum token permissions they need.
- Releases are built in CI, and the wheel's build provenance is attested.
- CodeQL analyses the Python code on every pull request once the repository
  is public.

## Settings to switch on in GitHub

These are repository settings, which a file in the repository cannot apply:

- Branch protection on `main`: pull request required, review by a code owner,
  CI passing.
- Required reviewers on the `prod` environment, so a release waits for
  approval before it deploys.
- Private vulnerability reporting, secret scanning and push protection.
- Restrict who may push version tags, since a tag starts a release.
