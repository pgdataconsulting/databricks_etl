# Terraform: platform and governance

Infrastructure and access control for the ETL framework on Azure Databricks, split by
ownership:

| Terraform (this directory) | Databricks Asset Bundle (`databricks.yml`) |
|---|---|
| Azure resources: resource group, data lake, access connector, workspace, Key Vault | Jobs |
| Unity Catalog: storage credential, external locations, catalog, schemas | The `etl_framework` wheel |
| Identities: groups, service principals, workspace assignment, entitlements, CI/CD federation | |
| Grants, the secret scope and its ACLs, an optional SQL warehouse | |

Nothing here defines a job, pipeline, notebook or wheel.

This code is checked statically: formatted, validated against the provider schemas, tested
with mocked providers and scanned (see [Verification](#verification)). It has not been
applied to an Azure subscription from this repository, so treat the first `plan` in a real
environment as the remaining test.

## Layout

```
infra/terraform/
├── README.md
├── .gitignore                         ignores generated *.tfvars.json
└── stacks/
    ├── platform/                      azurerm, one state per environment
    │   ├── versions.tf  providers.tf  variables.tf  locals.tf
    │   ├── main.tf  storage.tf  workspace.tf  key_vault.tf  outputs.tf
    │   ├── backend.hcl.example
    │   ├── environments/{dev,test,prod}.tfvars.example
    │   └── tests/platform.tftest.hcl
    └── governance/                    databricks (workspace and account), one state per environment
        ├── versions.tf  providers.tf  variables.tf  locals.tf
        ├── identity.tf  unity_catalog.tf  grants.tf  secrets.tf  sql_warehouse.tf  outputs.tf
        ├── backend.hcl.example
        ├── environments/{dev,test,prod}.tfvars.example
        └── tests/access_model.tftest.hcl
```

There are two root stacks and no shared modules. The stacks have different owners in
practice (a cloud platform team and a data platform team), need different credentials and
change at different rates, so they keep separate state. Inside a stack every resource type
is used in one place and repetition is handled with `for_each`; a module would only add
pass-through variables.

## What each stack creates

### `stacks/platform`

| Resource | Name | Notes |
|---|---|---|
| Resource group | `rg-<prefix>-<env>` | |
| Storage account (ADLS Gen2) | `st<prefix><env><suffix>` | Hierarchical namespace, TLS 1.2, HTTPS only, no anonymous access, Shared Key access disabled (Microsoft Entra ID is the only way to authorize a request), local users disabled, soft delete for blobs and containers |
| Containers | `unity-catalog`, `landing` | Managed storage for the catalog; raw landing zone for source files |
| Databricks access connector | `dbac-<prefix>-<env>` | System-assigned managed identity |
| Role assignment | | `Storage Blob Data Contributor` for the access connector on the storage account |
| Databricks workspace | `dbw-<prefix>-<env>` | Premium, secure cluster connectivity (no public IPs on cluster nodes), public network access configurable |
| Key Vault | `kv-<prefix>-<env>-<suffix>` | Azure RBAC authorization, purge protection, 90 days soft delete |
| Role assignments | | `Key Vault Secrets Officer` for the principals in `key_vault_secrets_officer_principal_ids` |

Every resource carries the tags `project`, `environment` and `managed_by = "terraform"`,
plus whatever is passed in `tags`.

### `stacks/governance`

| Resource | Name | Level |
|---|---|---|
| Groups (created or looked up) | `<prefix>-platform-admins`, `<prefix>-data-engineers`, `<prefix>-data-analysts` | Account |
| Service principals | `<prefix>-runner-<env>`, `<prefix>-deployer-<env>` | Account |
| Workspace assignments with entitlements | | Account |
| Service Principal User role for the deployer on the runner | | Account |
| GitHub Actions OIDC federation policy on the deployer | `github-actions-<env>` | Account |
| Storage credential | `<prefix>_<env>_access_connector` | Workspace |
| External locations | `<prefix>_<env>_managed`, `<prefix>_<env>_landing` | Workspace |
| Catalog | `<prefix>_<env>` | Workspace |
| Schemas | `bronze`, `silver`, `gold`, `quarantine`, `etl_audit`, `etl_state` | Workspace |
| Grants on the catalog, each schema and the landing location | | Workspace |
| Secret scope and ACLs | `source-systems` | Workspace |
| Serverless SQL warehouse and its permissions (optional, off by default) | `<prefix>-<env>-analytics` | Workspace |

## Access model

`data_engineer_access` is the one variable behind every difference between production and
non-production. Its default is `read_only`, so leaving it out never widens access.

| Principal | Workspace | Catalog | Schemas | Landing zone | Secret scope | SQL warehouse (optional) |
|---|---|---|---|---|---|---|
| `<prefix>-platform-admins` | Admin | `ALL_PRIVILEGES` | Inherited from the catalog | none | `MANAGE` | Manage, as workspace admins |
| `<prefix>-runner-<env>` (jobs run as it) | Workspace access | `USE_CATALOG` | `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE` on all six | `READ_FILES` | `READ` | none |
| `<prefix>-data-engineers`, `read_write` (dev, test) | Workspace access, Databricks SQL access | `USE_CATALOG` | `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE` on all six | `READ_FILES` | `READ` | `CAN_USE` |
| `<prefix>-data-engineers`, `read_only` (prod) | Workspace access, Databricks SQL access | `USE_CATALOG` | `USE_SCHEMA`, `SELECT` on all six, including `etl_audit` | none | none | `CAN_USE` |
| `<prefix>-data-analysts` | Databricks SQL access | `USE_CATALOG` | `USE_SCHEMA`, `SELECT` on `gold` only | none | none | `CAN_USE` |
| `<prefix>-deployer-<env>` (CI/CD) | Workspace access | none | none | none | none | none |

The deployer holds no data privilege. It can deploy jobs that run as the runner because it
has the Service Principal User role on the runner, and it signs in from GitHub Actions
through OIDC, so no secret is created for it.

The runner cannot create schemas. The two schemas the framework writes its own tables to
(`etl_audit`, `etl_state`) are created here for that reason.

This table is asserted by `stacks/governance/tests/access_model.tftest.hcl`.

## Prerequisites

### Tools

- Terraform 1.9 or later (checked with 1.9.8 and 1.17.2). Provider versions are pinned by
  the committed lock files: `azurerm` 5.14.0 and `databricks` 1.142.0, for Linux, macOS
  (Intel and Apple silicon) and Windows.
- Azure CLI. Both providers use the `az login` session; no credential is configured in
  any file.
- Databricks CLI 0.205 or later, only to load secrets into the secret scope.

### Remote state

A storage account and blob container for Terraform state must exist before the first
`init`. They are not managed by these stacks. The backend is configured with
`use_azuread_auth = true`, so the identity that runs Terraform needs
`Storage Blob Data Contributor` on that container and no account key is involved. Each
stack and environment uses its own state key.

### Who can apply the platform stack

An Azure identity with, on the subscription:

- `Contributor`, to create the resources (creating a Databricks workspace needs it at
  subscription level, or a custom role with the equivalent permissions).
- `Owner` or `User Access Administrator`, because the stack creates role assignments.
- Permission to register resource providers, or `register_resource_providers = false`
  when they are registered already. Version 5 of the `azurerm` provider registers nothing
  by default; the stack registers `Microsoft.Compute`, `Microsoft.Databricks`,
  `Microsoft.KeyVault`, `Microsoft.ManagedIdentity`, `Microsoft.Network` and
  `Microsoft.Storage`.

### Who can apply the governance stack

One identity that is all of the following:

1. **Databricks account admin.** The stack creates account-level service principals (and
   groups, when `create_groups = true`), assigns principals to the workspace, grants a
   role on the runner and creates a federation policy. Federation policies are
   configured by account admins. The first account admin of a tenant is established by a
   Microsoft Entra ID Global Administrator signing in to the account console.
2. **Workspace admin** of the target workspace. An identity with `Contributor` or `Owner`
   on the subscription gains workspace admin access by signing in to the workspace, so the
   identity that applied the platform stack qualifies.
3. **Able to create Unity Catalog objects.** Workspaces created after 9 November 2023 are
   attached to a metastore automatically, and their workspace admins receive
   `CREATE CATALOG`, `CREATE EXTERNAL LOCATION` and `CREATE STORAGE CREDENTIAL` on it by
   default. In that case nothing more is needed and a metastore admin is optional. If a
   metastore admin has removed those defaults, the identity needs the three privileges
   granted on the metastore, or the metastore admin role.

Use the same identity for every apply of an environment, preferably a dedicated automation
identity rather than a personal account. Whoever creates the storage credential, external
locations, catalog and schemas owns them, and a workspace admin cannot change grants on
objects it does not own; that takes the owner or a metastore admin.

Secret scope ACLs and entitlements need the Premium plan, which the platform stack
deploys.

## Deploy

Run the platform stack first, then governance, once per environment. The commands use
`dev`; replace it with `test` or `prod`.

### 1. Platform

```bash
cd infra/terraform/stacks/platform

cp backend.hcl.example dev.backend.hcl                       # state account, container, key
cp environments/dev.tfvars.example environments/dev.tfvars   # subscription, region, suffix

az login
terraform init -backend-config=dev.backend.hcl
terraform plan -var-file=environments/dev.tfvars -out=dev.tfplan
terraform apply dev.tfplan
```

### 2. Governance

The governance stack takes the workspace, access connector and storage locations from the
platform stack as a generated variable file. Nothing is typed by hand.

```bash
cd ../governance

cp backend.hcl.example dev.backend.hcl
cp environments/dev.tfvars.example environments/dev.tfvars   # account ID, GitHub repository

terraform -chdir=../platform output -json governance_inputs > environments/dev.platform.tfvars.json

terraform init -backend-config=dev.backend.hcl
terraform plan \
  -var-file=environments/dev.tfvars \
  -var-file=environments/dev.platform.tfvars.json \
  -out=dev.tfplan
terraform apply dev.tfplan
```

`*.tfvars`, `*.backend.hcl`, `*.tfplan` and `*.tfvars.json` are ignored by Git; only the
`.example` files and the lock files are committed.

To work on another environment from the same directory, re-initialise against its state:
`terraform init -reconfigure -backend-config=test.backend.hcl`. The generated file records
which environment it came from, and the governance stack refuses to plan when that does
not match `environment`.

### 3. Groups across environments

The three role groups are account-wide and shared by every environment; only their rights
differ per workspace. With `create_groups = true` the groups belong to that environment's
state, and creating a group whose name already exists fails. So set it to `true` in at most
one environment (the examples use dev) and to `false` everywhere else. When an identity
provider syncs the groups, set it to `false` everywhere and use `group_name_overrides` if
the synced names differ. Group membership is never managed here.

### 4. Load secrets

The secret scope starts empty. Credentials are kept in the Key Vault and copied into the
scope by a person or pipeline that can read the vault (see
`key_vault_secrets_officer_principal_ids`) and has `WRITE` or `MANAGE` on the scope (the
platform-admins group has `MANAGE`):

```bash
printf '%s' "$(az keyvault secret show \
    --vault-name "$(terraform -chdir=../platform output -raw key_vault_name)" \
    --name sales-db-password --query value --output tsv)" \
  | databricks secrets put-secret source-systems sales-db-password
```

The command substitution strips the newline that ends the `az` output and `printf '%s'`
adds none, so no stray newline is sent with the value. It travels over standard input
rather than as a command-line argument. A pipeline then references it as
`${secret:source-systems/sales-db-password}`.

### 5. Wire GitHub Actions and the bundle

All values come from `terraform output` in `stacks/governance`. None is a secret, so the
ones GitHub needs are stored as environment variables, not secrets.

| Terraform output | Where it goes |
|---|---|
| `workspace_url` | GitHub environment variable `DATABRICKS_HOST`: the workspace the CLI and the bundle target |
| `deployer_application_id` | GitHub environment variable `DATABRICKS_CLIENT_ID`: the identity the deploy job signs in as |
| `runner_application_id` | GitHub environment variable `RUNNER_APPLICATION_ID`. The deploy workflow passes it to the bundle as `BUNDLE_VAR_runner_service_principal`, which sets `run_as` |
| `catalog_name` | `variables.catalog` of the target in `databricks.yml`. The values there (`etl_dev`, `etl_test`, `etl_prod`) are what the default `prefix` produces; change them if you changed the prefix |
| `group_names` | `admins_group` and `engineers_group` in `databricks.yml`, if the names are not the defaults |
| `secret_scope_name` | The scope in `${secret:<scope>/<key>}` references in pipeline YAML |
| `github_oidc_subject` | The subject the deploy job must present (troubleshooting) |

CI deploys the `test` and `prod` environments; `dev` is deployed from a workstation with
a CLI profile and needs no GitHub variables. From `stacks/governance`, initialised against
the environment's state:

```bash
for pair in \
  DATABRICKS_HOST=workspace_url \
  DATABRICKS_CLIENT_ID=deployer_application_id \
  RUNNER_APPLICATION_ID=runner_application_id
do
  gh variable set "${pair%%=*}" --env test --body "$(terraform output -raw "${pair##*=}")"
done
```

The GitHub environment name must equal `github_environment` (by default the Terraform
`environment`), because the federation policy trusts that environment and no other.
[`deploy.yml`](../../.github/workflows/deploy.yml) declares the environment and
`permissions: id-token: write`, and sets `DATABRICKS_AUTH_TYPE=github-oidc` next to
`DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`; the Databricks CLI then exchanges the job's
GitHub token for a Databricks token.

Because the bundle is deployed by one identity and runs as another, it may contain jobs
and pipelines only. That matches the split above. The bundle lives in the deployer's
workspace folder and grants the runner read access to it, so the jobs can read their
configuration and wheel but nobody else can change them.

### 6. Check the federation subject

Databricks compares the policy's subject with the `sub` claim of the GitHub token exactly,
and GitHub has two formats for it:

- `repo:<organization>/<repository>:environment:<environment>` for repositories created
  before 15 July 2026 and not renamed or transferred since. This is what the stack
  composes from `github_organization`, `github_repository` and `github_environment`.
- `repo:<organization>@<owner id>/<repository>@<repository id>:environment:<environment>`
  for repositories created, renamed or transferred after that date, and for earlier ones
  that opted in.

If sign-in from the workflow is rejected, or the repository may be on the second format,
read the real claim from a job that runs in the target GitHub environment with
`permissions: id-token: write`:

```yaml
- name: Show the OIDC subject
  run: |
    curl -sS -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
      "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=databricks" \
      | jq -r '.value | split(".")[1] | gsub("-"; "+") | gsub("_"; "/") | @base64d | fromjson | .sub'
```

Any audience works for reading the subject. Put the printed value in
`github_oidc_subject` and apply; it replaces the composed subject entirely.

The policy accepts the Databricks account ID as audience. That is what Databricks
recommends, and the documented GitHub workflow sets no audience of its own. To use another
value, set `github_oidc_audiences` here and `DATABRICKS_TOKEN_AUDIENCE` in the workflow to
the same string.

## Design decisions

**Authoritative grants.** Grants use `databricks_grants`, one resource per catalog, schema
and external location. That resource owns the complete grant list of its securable: a
privilege granted by hand is removed on the next apply, so the table above is the access
model rather than a lower bound of it. The alternative, `databricks_grant`, manages one
principal per securable and leaves everyone else's grants alone, which suits securables
shared with other teams. These securables are created by this stack for this workload, so
nothing else should be granting on them. Grants on individual tables are not managed and
are not touched.

**Explicit entitlements.** Workspace entitlements are no longer inherited from the
built-in `users` group, but a principal added to a workspace without an explicit list
still receives what that group used to grant (by default workspace access and Databricks
SQL access). The stack therefore assigns every non-admin principal with
`databricks_account_iam_workspace_assignment_v2`, which sets exactly the listed
entitlements: analysts get Databricks SQL access and nothing else. The admin group is
assigned with `databricks_mws_permission_assignment` and the documented `ADMIN`
permission.

**Databricks-backed secret scope.** A Key Vault-backed scope would let the scope read the
vault directly, but Azure Databricks supports it only for vaults that use the access
policy permission model, and this vault uses Azure RBAC. Creating one also requires an
Azure-specific authentication method for the Databricks provider (Azure CLI, a managed
identity or an Entra ID service principal; a personal access token is refused), and it
gives every reader of the scope every secret in the vault. The scope is therefore
Databricks-backed: the Key Vault stays the system of record under Azure RBAC and purge
protection, and values are copied into the scope outside Terraform, which keeps them out
of state. The cost is the copy step in [Load secrets](#4-load-secrets), which has to be
repeated when a credential is rotated. ACLs apply to the whole scope, not to single
secrets.

**Workspace isolation.** Environments normally share one metastore per region. With
`isolate_to_workspace = true` (the default) the catalog, the storage credential and the
external locations are bound to the environment's own workspace, so a grant alone is not
enough to reach production data from the dev workspace.

**Service principals per environment, Databricks-managed.** The runner and deployer exist
only in Databricks. There is no Entra ID application, client secret or certificate to
rotate, and each one is assigned to its own environment's workspace only.

## Network exposure

The stack creates no virtual network, so the defaults rely on identity, not on network
location:

- **Data lake.** The public endpoint is enabled; every request needs Entra ID
  authorization. `storage_firewall` switches the account to deny by default with an allow
  list of IP ranges and subnets. The access connector is always allowed as a resource
  instance, and the "trusted Azure services" bypass is off, because it would admit every
  access connector in the tenant. Only enable the firewall when Databricks compute reaches
  the account through a listed subnet or a private endpoint. Serverless compute, including
  the optional SQL warehouse, comes from the Databricks serverless compute plane and needs
  its own firewall configuration.
- **Key Vault.** `key_vault_firewall` restricts the data plane to listed networks. Nothing
  in Databricks reads the vault at run time, so this only has to admit the people and
  pipelines that maintain secrets.
- **Workspace.** `workspace_public_network_access_enabled = false` makes the workspace
  reachable through Private Link only. That requires a VNet-injected workspace
  (`workspace_vnet_injection`, validated) and private endpoints; the network, the
  endpoints and DNS are not created here.

## Teardown

Destroy in reverse order. Destroying an environment deletes its data.

1. Remove the workloads: `databricks bundle destroy --target <target>`.
2. Governance. Catalogs and schemas that contain tables are not deleted unless
   `force_destroy = true` has been applied first:

   ```bash
   cd infra/terraform/stacks/governance
   terraform apply   -var-file=environments/dev.tfvars -var-file=environments/dev.platform.tfvars.json -var force_destroy=true
   terraform destroy -var-file=environments/dev.tfvars -var-file=environments/dev.platform.tfvars.json -var force_destroy=true
   ```

   The service principals are deleted from the account, not just deactivated. If this
   environment created the groups, destroying it deletes them for every environment; to
   keep them, run `terraform state rm 'databricks_group.this'` before the destroy.
3. Platform:

   ```bash
   cd ../platform
   terraform destroy -var-file=environments/dev.tfvars
   ```

   The Key Vault has purge protection, so it is soft-deleted and its name stays reserved
   for 90 days. Applying the stack again within that time recovers the vault instead of
   creating a new one. The resource group is only deleted when nothing outside this stack
   is left in it.

## Verification

Everything below runs without cloud credentials, from `stacks/platform` and
`stacks/governance`:

```bash
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
terraform test
```

`terraform test` uses mocked providers. It asserts resource names, tags and input
validation for the platform stack, and for the governance stack the access model above in
both `read_write` and `read_only` mode, the group lookup path, the federation subject and
audience, and the rejection of mismatched inputs.

A static scan with Checkov 3.4.5 (`checkov -d infra/terraform --framework terraform`)
reports 19 passed and no failed checks. Nine checks are skipped on purpose, each with its
reason next to the resource. Checkov has no checks for Databricks provider resources, so
the scan covers the platform stack only.

Not verified, because it needs a subscription and a Databricks account: `terraform plan`
and `apply`, and with them every behaviour of the Azure and Databricks APIs.

## Not included

- Networking: virtual network, private endpoints, private DNS and serverless network
  connectivity configurations.
- Diagnostic settings and a Log Analytics workspace.
- Customer-managed keys.
- The Unity Catalog metastore and its assignment; the stack relies on automatic
  enablement.
- Group membership and identity provider synchronisation.
- The state storage account.
