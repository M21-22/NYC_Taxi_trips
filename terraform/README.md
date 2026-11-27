# NYC Taxi dev deployment

Run these PowerShell commands from the repository root on `main`. Use Databricks CLI 0.287.0 or later for the bundle schema grants. The provisioning steps assume a fresh dev catalog. If the previous Terraform configuration already manages Bronze, Silver, or Gold in that catalog, a Terraform plan can propose deleting them. Preserve existing data by transferring state ownership before applying, or use a fresh dev catalog if the existing data is disposable.

## Local checks

The checks use local Python and provider downloads. They require no Azure or Databricks login.

```powershell
$env:SPARK_LOCAL_IP = '127.0.0.1'
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
if ($LASTEXITCODE -ne 0) { throw 'Tests failed.' }

terraform fmt -recursive -check terraform
if ($LASTEXITCODE -ne 0) { throw 'Terraform formatting check failed.' }
foreach ($root in @('terraform', 'terraform/unity-catalog')) {
    terraform "-chdir=$root" init -backend=false -input=false -lockfile=readonly
    if ($LASTEXITCODE -ne 0) { throw "Terraform initialization failed in $root." }
    terraform "-chdir=$root" validate
    if ($LASTEXITCODE -ne 0) { throw "Terraform validation failed in $root." }
}
```

GitHub Actions checks the Spark tests, both Terraform roots, and the YAML against the Databricks CLI bundle schema. Workspace-aware bundle validation runs in the dev window.

## Recreate the Terraform backend

Run this section if the state resource group, storage account, and container were deleted after teardown. Both Terraform roots use the same backend storage but separate blob keys, `terraform.tfstate` and `unity-catalog-dev.tfstate`. A fresh deployment starts with empty state. Use the subscription where you have permission to create resources and role assignments.

```powershell
az login
az account set --subscription '<subscription-id>'
$env:ARM_SUBSCRIPTION_ID = az account show --query id -o tsv

$location = 'northeurope'
$stateGroup = 'spark-nyctrips-rg'
$stateAccount = 'sparknyctripssa' # Pick a globally unique name if this name is unavailable.
$stateContainer = 'spark-nyctrips-sc'
$operatorId = az ad signed-in-user show --query id -o tsv

az group create --name $stateGroup --location $location
$stateAccountId = az storage account create --resource-group $stateGroup --name $stateAccount --location $location --sku Standard_LRS --kind StorageV2 --allow-shared-key-access false --allow-blob-public-access false --query id -o tsv
if ($LASTEXITCODE -ne 0) { throw 'Could not create the state storage account.' }

az role assignment create --assignee-object-id $operatorId --assignee-principal-type User --role 'Storage Blob Data Contributor' --scope $stateAccountId
if ($LASTEXITCODE -ne 0) { throw 'Could not grant state storage access.' }
```

Allow the Azure role assignment to propagate, then create the private container and enable recovery. The Terraform operator needs blob data access even if they can administer the resource group.

```powershell
az storage container create --account-name $stateAccount --name $stateContainer --auth-mode login
if ($LASTEXITCODE -ne 0) { throw 'Could not create the state container. Check Azure RBAC propagation.' }

az storage account blob-service-properties update --resource-group $stateGroup --account-name $stateAccount --enable-delete-retention true --delete-retention-days 7 --enable-container-delete-retention true --container-delete-retention-days 7 --enable-versioning true
```

Use these backend options for both Terraform roots. The tracked `backend.hcl` files supply the two different keys and enable Entra authentication. The overrides also work if you chose different storage names. Keep the state storage outside the project resource group.

```powershell
$backendOptions = @(
    '-backend-config=backend.hcl'
    "-backend-config=resource_group_name=$stateGroup"
    "-backend-config=storage_account_name=$stateAccount"
    "-backend-config=container_name=$stateContainer"
)
```

## Provision Azure

```powershell
terraform -chdir=terraform init -reconfigure @backendOptions
terraform -chdir=terraform plan -out=dev.tfplan "-var=LOCATION=$location"
terraform -chdir=terraform apply dev.tfplan
```

Run each command after the preceding one succeeds and review the plan before applying. Azure Terraform grants the Access Connector blob access and grants the Azure apply identity upload access to the trip and customer input containers. A different Azure uploader needs its own blob data role on the relevant container. Uploads through Databricks Volumes use Unity Catalog grants.

## Configure the new Databricks workspace

1. Verify the new workspace has a Unity Catalog metastore in its region. If it has no assignment, have a Databricks account admin assign it to the regional metastore.
2. Reuse the account-level `nyc-taxi-pipeline` and `nyc-taxi-customer-job` service principals if they still exist, or create replacements. Assign both to the new workspace with workspace access. Record their application IDs.
3. Reuse or create the account-level groups `nyc-taxi-staging`, `nyc-taxi-analytics`, and `nyc-taxi-customer-admins`. Assign them to the workspace and add the people who need each role. The analytics user needs membership in the analytics group to query Gold with their own credentials.
4. In workspace **Settings → Identity and access → Service principals → Manage**, open **Permissions** for each run identity and grant the deployer **Service principal: User**. An identity with **Service principal: Manager** can grant this role.
5. Use a Unity Catalog provisioning identity that can create the catalog, storage credential, and external locations. Wait for the Access Connector's Azure role assignments to propagate before applying Unity Catalog.

## Provision Unity Catalog

Use the tracked `terraform/unity-catalog/dev.tfvars` if its IDs and group names still match. If principals were recreated, copy it to a local file, set `$devVars` to that file, and update `principals.pipeline`, `principals.customer_job`, and any changed group names. Use the same values in the bundle configuration.

```powershell
$devVars = 'dev.tfvars' # Relative to terraform/unity-catalog, or use an absolute path.
$azureInputs = terraform -chdir=terraform output -json unity_catalog_inputs
if ($LASTEXITCODE -ne 0) { throw 'Could not read the Azure Terraform outputs.' }

$azureInputFile = Join-Path ([IO.Path]::GetTempPath()) "nyc-taxi-azure-$([guid]::NewGuid()).tfvars.json"
[IO.File]::WriteAllText($azureInputFile, ($azureInputs -join "`n"), [Text.UTF8Encoding]::new($false))
$env:DATABRICKS_HOST = ($azureInputs -join "`n" | ConvertFrom-Json).workspace_host
$env:DATABRICKS_AUTH_TYPE = 'azure-cli'

terraform -chdir=terraform/unity-catalog init -reconfigure @backendOptions
terraform -chdir=terraform/unity-catalog plan "-var-file=$devVars" "-var-file=$azureInputFile" -out=dev.tfplan
terraform -chdir=terraform/unity-catalog apply dev.tfplan
```

Keep `$azureInputFile` and `$devVars` available for teardown. The workspace host comes from the new Azure deployment. If a local environment file or CLI profile still points to the deleted workspace, update it before running bundle or secret commands.

Review each saved plan before applying it. Terraform grants catalog access and manages the `landing` and `restricted` schema and volume grants. The bundle applies the Bronze, Silver, and Gold schema grants when deployed.

| Identity | Project access |
| --- | --- |
| Pipeline service principal | Reads the trip volume and publishes to Bronze, Silver, and Gold |
| Staging group | Reads and writes the trip volume |
| Analytics group | Reads Gold |
| Customer encryption service principal | Reads the restricted input and AES key and writes the managed encrypted table |
| Customer administrators | Stage customer inputs, query the encrypted table, and read the AES key |

The grants apply to these principals. Keep broader Azure RBAC assignments and Databricks administrative roles out of the identities used to verify access.

## Deploy the bundle and publish inputs

Create the `/Workspace/nyc-taxi-bundles` workspace folder. Give the deployer permission to manage it and both run identities `CAN VIEW` on it so they can read their deployed notebooks or wheel. The deployer needs `USE CATALOG` and `CREATE SCHEMA` on the dev catalog and will own the bundle-created schemas. Use the same pipeline service principal and analytics group as in `dev.tfvars`. Choose or create a dev SQL warehouse with auto-stop enabled. Grant the dashboard's analytics group `CAN USE` and use the new warehouse ID for the bundle.

```powershell
. .\.venv\Scripts\Activate.ps1
$env:BUNDLE_VAR_catalog = terraform -chdir=terraform/unity-catalog output -raw catalog_name
$env:BUNDLE_VAR_pipeline_principal = '<pipeline-application-id>'
$env:BUNDLE_VAR_customer_job_principal = '<customer-job-application-id>'
$env:BUNDLE_VAR_analytics_group = '<analytics-account-group>'
$env:BUNDLE_VAR_warehouse_id = '<dev-sql-warehouse-id>'
$env:BUNDLE_VAR_notification_email = '<your-email>'

databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Use the staging identity to copy January's trip files and zone lookup. Set `$batch` to the enriched batch identified by its `manifest.json`. Keep the final filename unique within its feed so Auto Loader can track it as a new path.

```powershell
$batch = ".\data\enriched\batches\<batch-id>"
$tripVolume = "dbfs:/Volumes/${env:BUNDLE_VAR_catalog}/landing/trips"
databricks fs mkdir "$tripVolume/yellow"
databricks fs mkdir "$tripVolume/green"
databricks fs mkdir "$tripVolume/reference"
databricks fs cp "$batch\yellow\yellow_tripdata_2026-01.parquet" "$tripVolume/yellow/yellow_tripdata_2026-01.parquet"
databricks fs cp "$batch\green\green_tripdata_2026-01.parquet" "$tripVolume/green/green_tripdata_2026-01.parquet"
databricks fs cp .\data\reference\taxi_zone_lookup.csv "$tripVolume/reference/taxi_zone_lookup.csv"
```

For the customer workflow, follow the [customer encryption runbook](../docs/customer-encryption.md). It covers staging the synthetic customer pool in the restricted volume, seeding `aes-k1` in the new `<catalog>-pii` secret scope, and running the encryption job. Terraform creates the scope and its ACLs. Enter the key value separately using the Databricks CLI prompt.

Publish the taxi-zone Delta table before starting the trip pipeline in a new or upgraded deployment. The refresh job uses the pipeline service principal and its existing access to the trip volume and Bronze schema.

```powershell
databricks bundle run -t dev refresh_taxi_zones
```

After that job succeeds, run the trip pipeline using the deployment identity. Stage February and March under their own filenames and run the pipeline again for the incremental update.

```powershell
databricks bundle run -t dev nyc_taxi_medallion
```

For a revised zone lookup, replace the CSV in the same reference path, run `refresh_taxi_zones`, and run a normal trip update so Gold sees the revised lookup. On an upgraded deployment, verify the pipeline succeeds with `bronze.taxi_zones` before removing any leftover `bronze.taxi_zones_raw` materialized view. The upgrade uses the existing trip checkpoints.

Use the Lakeflow update history and event log to inspect Auto Loader progress, expectation results, failures, and row or flow metrics. Capture January and January–March dashboard results. Verify analytics can open the dashboard with their own credentials and query Gold, and verify both analytics and the pipeline principal are denied access to the restricted volume.

## Control spend and tear down

Use triggered updates and leave scheduling disabled during validation. Decide the maximum spend for the dev window and set an alert threshold in Azure Cost Management before the run. Budget alerts report spend and leave workloads running. Inspect Databricks usage by workspace or pipeline in `system.billing.usage` if you have access. Stop the SQL warehouse after capturing evidence.

When the dev work is complete, retain any required evidence before destroying the deployed bundle and the Terraform roots. Bundle destruction deletes its Bronze, Silver, and Gold schemas, including their datasets. Review each destruction prompt. The state account remains available for future deployments.

Before destroying the bundle, drop the job-created lookup table as its owner or an authorized administrator. Substitute the deployed catalog if it differs from `nyc_taxi_dev`.

```sql
DROP TABLE IF EXISTS nyc_taxi_dev.bronze.taxi_zones;
```

Then destroy the bundle. Before destroying Unity Catalog, follow the [customer table cleanup instructions](../docs/customer-encryption.md#clean-up-the-dev-table) if the encryption job ran. Use the same backend and inputs you used to deploy.

```powershell
databricks bundle destroy -t dev
terraform -chdir=terraform/unity-catalog destroy "-var-file=$devVars" "-var-file=$azureInputFile"
terraform -chdir=terraform destroy "-var=LOCATION=$location"
```

Run the commands one at a time and stop on any error. Recently dropped managed tables can still block deletion of the managed external location. If you are deleting the dedicated dev storage account and have saved any evidence you need, force-delete only the project's managed external location, then retry Unity Catalog destroy before destroying Azure.

```powershell
databricks external-locations delete "${env:BUNDLE_VAR_catalog}_managed" --force
```

Unity Catalog cannot purge files through a force-deleted location. If you are retaining the storage account, leave the location in place and allow Unity Catalog cleanup to finish.
