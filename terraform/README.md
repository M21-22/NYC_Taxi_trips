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

## Provision the dev environment

Use a short deployment window in an Azure region that supports the workspace's serverless workloads. Use an existing Terraform state storage account outside the project resource group. Check that state blobs are recoverable through Azure Blob soft delete and, if supported by that state account, versioning. Both roots authenticate to state storage through Microsoft Entra ID and use distinct blob keys. The Terraform operator needs Azure permissions to create resources and role assignments, and needs `Storage Blob Data Contributor` on the state container. If uploading directly through Azure Storage, the uploader also needs blob data access on the relevant input containers. The Access Connector's role assignments apply to its managed identity. Keep the existing Azure state key if you are reusing its backend. Use Terraform's state migration procedure if moving an existing state.

```powershell
az login
az account set --subscription '<subscription-id>'
$env:ARM_SUBSCRIPTION_ID = az account show --query id -o tsv

Copy-Item terraform/backend.example.hcl terraform/backend.hcl
Copy-Item terraform/unity-catalog/backend.example.hcl terraform/unity-catalog/backend.hcl
Copy-Item terraform/unity-catalog/dev.tfvars.example terraform/unity-catalog/dev.tfvars
```

Fill both backend files with the state storage details. In `dev.tfvars`, set `principals.pipeline` and `principals.customer_job` to their respective service principal application IDs. Set `principals.staging`, `principals.analytics`, and `principals.customer_admin` to the account group names. Assign these identities to the workspace and enroll the people who will stage, analyze, or administer the data in the appropriate groups. The deployer needs authorization to create the Unity Catalog objects and the Service Principal User role on both run identities.

```powershell
terraform -chdir=terraform init -reconfigure "-backend-config=backend.hcl"
$location = 'northeurope'
terraform -chdir=terraform plan -out=dev.tfplan -var "LOCATION=$location"
terraform -chdir=terraform apply dev.tfplan
```

Choose the correct region before saving the plan. After Azure provisions the workspace, verify its Unity Catalog metastore assignment. Allow time for the Access Connector's Azure role assignments to propagate before creating the storage locations.

```powershell
$azureInputs = terraform -chdir=terraform output -json unity_catalog_inputs
if ($LASTEXITCODE -ne 0) { throw 'Could not read the Azure Terraform outputs.' }
$inputFile = Join-Path (Get-Location) 'terraform\unity-catalog\azure.auto.tfvars.json'
[IO.File]::WriteAllText($inputFile, ($azureInputs -join "`n"), [Text.UTF8Encoding]::new($false))
$env:DATABRICKS_HOST = ($azureInputs -join "`n" | ConvertFrom-Json).workspace_host
$env:DATABRICKS_AUTH_TYPE = 'azure-cli'

terraform -chdir=terraform/unity-catalog init -reconfigure "-backend-config=backend.hcl"
terraform -chdir=terraform/unity-catalog plan "-var-file=dev.tfvars" -out=dev.tfplan
terraform -chdir=terraform/unity-catalog apply dev.tfplan
```

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

Create the `/Workspace/nyc-taxi-bundles` workspace folder. Give the deployer permission to manage it and both run identities `CAN VIEW` on it so they can read their deployed notebooks or wheel. The deployer needs `USE CATALOG` and `CREATE SCHEMA` on the dev catalog and will own the bundle-created schemas. Use the same pipeline service principal and analytics group as in `dev.tfvars`. Choose a dev SQL warehouse with auto-stop enabled, and grant the dashboard's analytics group `CAN USE` on it.

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

For the customer workflow, follow the [customer encryption runbook](../docs/customer-encryption.md). It covers staging the synthetic customer pool in the restricted volume, seeding the secret, and running the encryption job.

Run the pipeline using the deployment identity. Then stage February and March under their own filenames and run the same command again for the incremental update.

```powershell
databricks bundle run -t dev nyc_taxi_medallion
```

Use the Lakeflow update history and event log to inspect Auto Loader progress, expectation results, failures, and row or flow metrics. Capture January and January–March dashboard results. Verify analytics can open the dashboard with their own credentials and query Gold, and verify both analytics and the pipeline principal are denied access to the restricted volume.

## Control spend and tear down

Use triggered updates and leave scheduling disabled during validation. Decide the maximum spend for the dev window and set an alert threshold in Azure Cost Management before the run. Budget alerts report spend and leave workloads running. Inspect Databricks usage by workspace or pipeline in `system.billing.usage` if you have access. Stop the SQL warehouse after capturing evidence.

When the dev work is complete, retain any required evidence before destroying the deployed bundle and the Terraform roots. Bundle destruction deletes its Bronze, Silver, and Gold schemas, including their datasets. Review each destruction prompt. The state account remains available for future deployments.

Run the bundle destroy first. Before destroying Unity Catalog, follow the [customer table cleanup instructions](../docs/customer-encryption.md#clean-up-the-dev-table) if the encryption job ran. Use the same backend and inputs you used to deploy.

```powershell
databricks bundle destroy -t dev
terraform -chdir=terraform/unity-catalog destroy "-var-file=dev.tfvars"
terraform -chdir=terraform destroy "-var=LOCATION=$location"
```

Run the commands one at a time and stop on any error. Recently dropped managed tables can still block deletion of the managed external location. If you are deleting the dedicated dev storage account and have saved any evidence you need, force-delete only the project's managed external location, then retry Unity Catalog destroy before destroying Azure.

```powershell
databricks external-locations delete "${env:BUNDLE_VAR_catalog}_managed" --force
```

Unity Catalog cannot purge files through a force-deleted location. If you are retaining the storage account, leave the location in place and allow Unity Catalog cleanup to finish.
