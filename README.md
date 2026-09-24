# Netflix Data Engineering on Azure Databricks

An end-to-end data engineering pipeline built on Azure. It ingests the public Netflix titles dataset into a **medallion architecture (Bronze / Silver / Gold)** on ADLS Gen2, using Databricks Auto Loader, PySpark, Delta Lake, Unity Catalog and Delta Live Tables.


## Architecture

```
GitHub / CSV files
      |  Azure Data Factory (adf-netflix-ism)
      v
  raw  container  ──►  Auto Loader (cloudFiles)  ──►  bronze  container
                                                            |
                                        PySpark notebooks (parameterized, Workflow for-each)
                                                            v
                                                     silver  container (Delta)
                                                            |
                                                 Delta Live Tables + expectations
                                                            v
                                                    gold tables (Unity Catalog)
```

## Azure environment

Everything lives in the resource group `IC-NetflixProject` (East US).

| Resource | Name | Purpose |
|---|---|---|
| Storage account (ADLS Gen2) | `netflixprojectdlism` | Containers: `raw`, `bronze`, `silver`, `metastore` |
| Azure Databricks workspace | `netflix-adb-ism` | Notebooks, Workflows, Delta Live Tables |
| Access Connector for Azure Databricks | `access_netflix` | Managed identity used by Unity Catalog to reach the storage |
| Azure Data Factory | `adf-netflix-ism` | Copies the CSV files into the `raw` container |
| Unity Catalog metastore | `metastore_azure_eastus` | Auto-created with the workspace |

## Repository structure

```
data/         Source CSV files (titles, cast, category, countries, directors)
notebooks/    Databricks notebooks exported as .py (source format)
original/     Original .dbc archive from the tutorial (import it directly into Databricks)
```

## Notebooks

| Notebook | Description |
|---|---|
| `1_Autoloader.py` | Incremental load from `raw` to `bronze` with Auto Loader |
| `2_silver.py` | Parameterized notebook (`sourcefolder`, `targetfolder`): bronze CSV to silver Delta |
| `3_lookupNotebook.py` | Returns the list of lookup tables as a job task value, used by a for-each loop |
| `4_Silver.py` | Transformations on `netflix_titles`: null handling, casting, `Shorttitle`, cleaned `rating`, `type_flag`, `duration_ranking` |
| `5_lookupNotebook.py` / `6_falsenotebook.py` | Demo of passing task values between Workflow tasks |
| `7_DLT_Notebook.py` | Gold layer with Delta Live Tables and data quality expectations (`show_id IS NOT NULL`) |

Storage paths in the notebooks point to `abfss://<container>@netflixprojectdlism.dfs.core.windows.net`. Data is written under the catalog `netflix_catalog`, schema `net_schema`.

## Setup

1. Create the storage account with hierarchical namespace enabled and the containers `raw`, `bronze`, `silver`, `metastore`.
2. Create an Access Connector and give it the **Storage Blob Data Contributor** role on the storage account.
3. In the Databricks workspace, create a storage credential from the access connector, then register an external location for the container (for example `abfss://metastore@netflixprojectdlism.dfs.core.windows.net/`).
4. Create the catalog: `CREATE CATALOG netflix_catalog MANAGED LOCATION 'abfss://metastore@netflixprojectdlism.dfs.core.windows.net/';`
5. Upload the files from `data/` to the `raw` container (or copy them with Azure Data Factory).
6. Import the notebooks (or `original/Netflix Project.dbc`) into Databricks and run them in order. Create a Workflow with a for-each loop over the output of `3_lookupNotebook` calling `2_silver`, and a Delta Live Tables pipeline for `7_DLT_Notebook`.

Note: the `.dbc` in `original/` is the untouched tutorial archive and still references the tutorial author's storage account name. The `.py` notebooks in `notebooks/` are already updated to `netflixprojectdlism`.

## Challenges & solutions

**"Cannot create a second metastore in the same region."**
Unity Catalog allows one metastore per region per account. Creating the workspace in East US had already created `metastore_azure_eastus` automatically. Solution: reuse the existing metastore instead of creating a new one.

**`storage_root does not specify a URI scheme`.**
The ADLS Gen2 path for the metastore storage must be a full URI. Wrong: `metastore@netflixprojectdlism.dfs.core.windows.net/`. Right: `abfss://metastore@netflixprojectdlism.dfs.core.windows.net/`.

**`Parent external location for your path does not exist`.**
Unity Catalog can only use a storage path that is registered as an external location. Solution: create a storage credential from the access connector `access_netflix`, register an external location for the container, then set the storage path.

## Tech stack

Azure Databricks · Azure Data Lake Storage Gen2 · Azure Data Factory · PySpark · Delta Lake · Delta Live Tables · Unity Catalog · Auto Loader
