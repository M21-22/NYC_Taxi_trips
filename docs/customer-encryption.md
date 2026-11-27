# Synthetic customer encryption

The on-demand `encrypt_customers` Lakeflow job reads the customer pool
produced by `enrich_trip_data.py` from the restricted customers volume. It
validates the uploaded Parquet and pool metadata, applies native Spark
AES-256-GCM to the uploaded names and emails, and writes customer IDs, pool
metadata, key IDs, and ciphertext to the managed Delta table
`<catalog>.restricted.customers_encrypted`.

Upload the plaintext enrichment output only to the restricted customers
volume. Trips and the dashboard continue through their own pipeline. For
existing trip inputs, look up the pool version in the enrichment batch
manifest. Customer IDs are only unique within a pool version.

## Configure and run

Use the [deployment runbook](../terraform/README.md) for workspace provisioning and bundle deployment.

1. Assign a dedicated service principal to the dev workspace. Add its
   application ID as `principals.customer_job` in the Unity Catalog
   `dev.tfvars`. The bundle deployer must be allowed to use this service
   principal as a job identity. Give the principal read access to the
   deployed bundle files so it can install the wheel.
2. Apply the Unity Catalog Terraform configuration. It creates the workspace
   secret scope `<catalog>-pii`, grants secret read access to the job identity
   and customer admin group, and gives the job identity permission to read the
   customer volume and create its table. Customer admins can upload files to
   the customer volume and query the restricted schema.
3. As the scope manager, check for `aes-k1`. If it is absent, add a 32-byte
   AES key encoded as exactly 64 hex characters using the interactive prompt.
   Keep the existing value when reusing the same key ID.

   ```powershell
   $scope = "${env:BUNDLE_VAR_catalog}-pii"
   databricks secrets list-secrets $scope
   # Run only if aes-k1 is absent.
   databricks secrets put-secret $scope aes-k1
   ```

4. As a member of the customer-admin group, upload the two files from the
   same pool directory. Run from the repo root. Keep the files for a staged
   pool version together and unchanged while the job can read them.

   ```powershell
   $pool = '.\data\enriched\customer_pools\v1'
   $target = "dbfs:/Volumes/${env:BUNDLE_VAR_catalog}/restricted/customers/incoming/v1"
   databricks fs mkdir $target
   databricks fs cp "$pool\customers.parquet" "$target/customers.parquet"
   databricks fs cp "$pool\pool.json" "$target/pool.json"
   databricks bundle run -t dev encrypt_customers --params 'pool_version=v1,key_id=k1'
   ```

The job atomically replaces rows for the requested pool version. It rejects a
different seed or count for a published version. To rotate a key, add
`aes-k2` and rerun the same pool with `key_id=k2`. Keep old keys while older
Delta versions or exports that depend on them are retained.

## Verify access in dev

As an authorized customer admin, check the row count against the staged
pool metadata. The dev run for `v1` with key `k1` returned 10,000 rows and
10,000 distinct IDs. Repeat this query after a rerun if you are testing
replacement of the same pool version.

```sql
SELECT pool_version, key_id, count(*) AS row_count,
       count(DISTINCT customer_id) AS distinct_customers
FROM nyc_taxi_dev.restricted.customers_encrypted
WHERE pool_version = 'v1'
GROUP BY pool_version, key_id;
```

The following query returns a Boolean without displaying the key or plaintext
email. Substitute your catalog name in the SQL examples.

```sql
SELECT
  CAST(aes_decrypt(customer_email_ciphertext,
                   unhex(secret('nyc_taxi_dev-pii', 'aes-k1')),
                   'GCM') AS STRING) = 'customer-1@example.com' AS decrypt_ok
FROM nyc_taxi_dev.restricted.customers_encrypted
WHERE pool_version = 'v1' AND customer_id = 1 AND key_id = 'k1';
```

Use the trip pipeline and analytics identities to verify that they cannot
query the restricted table. Review the secret scope ACLs for those identities.

## Clean up the dev table

Save any required evidence, then drop the generated demo table as its owner
or an authorized administrator before destroying the Unity Catalog Terraform
root. Substitute your catalog name.

```sql
DROP TABLE IF EXISTS nyc_taxi_dev.restricted.customers_encrypted;
```

Unity Catalog Terraform destroy also deletes the secret scope. Complete the
remaining steps in the [deployment runbook](../terraform/README.md#control-spend-and-tear-down).
