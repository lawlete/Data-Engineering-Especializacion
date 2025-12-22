::: {.cell .markdown}
# Building a Data Lakehouse with AWS Lake Formation and Apache Iceberg

In this lab, you will create a Data Lakehouse with a medallion
architecture using LakeFormation and Apache Iceberg tables. Your source
system will be a relational database instantiated as MySQL database in
Amazon RDS (Relational Database Service) and a streaming service that
stores data in Amazon S3. You will first populate the medallion
architecture with AWS Glue jobs, using Apache Iceberg tables for certain
tables. You will finally query the data stored in the latest layer using
Amazon Athena. To define and configure the components of this data
pipeline example, you will use Terraform as Infrastructure as Code (IaC)
service.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction and Setup](#1)
    -   [1.1 - Introduction](#1-1)
    -   [1.2 - Setting up the Data Lakehouse](#1-2)
-   [2 - Architecture of the Data Lakehouse](#2)
-   [3 - Landing Zone](#3)
    -   [3.1 - RDS and Streaming Landing](#3-1)
    -   [3.2 - Deployment](#3-2)
-   [4 - Curated Zone](#4)
    -   [4.1 - CSV Transformation](#4-1)
    -   [4.2 - JSON Transformation and Apache Iceberg](#4-2)
    -   [4.3 - Deployment](#4-3)
-   [5 - Presentation zone](#5)
-   [6 - \[Optional and Not Graded\] - Some Features of Iceberg
    Format](#6)
    -   [6.1 - Schema Evolution](#6-1)
    -   [6.2 - Versioning with Iceberg](#6-2)
-   [7 - \[Optional and Not Graded\] - Granting Permissions with Lake
    Formation](#7)
-   [8 - Enviroment Clean Up](#8)
:::

::: {.cell .markdown}
First, let\'s import some libraries that you need for the lab.
:::

::: {.cell .code}
``` python
import json

import boto3
import subprocess
import pandas as pd
import awswrangler as wr
from IPython.display import HTML
from scripts import lf_utils
```
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction and Setup {#1---introduction-and-setup}
:::

::: {.cell .markdown}
`<a id='1-1'>`{=html}`</a>`{=html}

### 1.1 - Introduction {#11---introduction}

Assume you work as a Data Engineer at a retailer of scale models of
classic cars and other transportation media. The retailer stores its
historical purchases and its customers\' information in a relational
database that consists of the following tables: customers, products,
productlines, orders, orderdetails, payments, employees, offices. You
will use the same database example as in some previous labs: [MySQL
Sample
Database](https://www.mysqltutorial.org/mysql-sample-database.aspx)
:::

::: {.cell .markdown}
The new web page of the company had a streaming service setup to quickly
retrieve the ratings that users give to different products, the company
has decided to migrate data architecture to the cloud. The relational
database was recreated in RDS and the setup of a streaming sink to
output the data was done in S3 bucket. You need to set up a Data
Lakehouse, using medallion architecture to organize the data in three
different zones:

-   **Landing**: Zone where raw data arrives from the RDS and the
    streaming service.
-   **Curated**: The raw data is enriched, curated and added to the Glue
    Data Catalog.
-   **Presentation**: Business objects are built on top of the curated
    data.
:::

::: {.cell .markdown}
`<a id='1-2'>`{=html}`</a>`{=html}

### 1.2 - Setting up the Data Lakehouse {#12---setting-up-the-data-lakehouse}

The Data Lake will be based on the S3 bucket, the name of the bucket is
one of the outputs in CloudFormation (*Data Lake Bucket*). Your first
job is to create folders to separate the bucket by the three zones:

-   `landing_zone`
-   `curated_zone`
-   `presentation_zone`

Next, you will be using LakeFormation to manage the Data Lakehouse, most
of the setup has been done to assign the admins of the Data Lakehouse.
You will have to grant permission to the Glue Job role
(`de-c3w2lab2-glue-role`) to be able to work inside the Data Lakehouse.
A simple way to set up the permissions is using `boto3`. Start from the
setup for the permissions using the first role.
:::

::: {.cell .markdown}
1.2.1. Set up some boto3 clients and required variables.
:::

::: {.cell .code}
``` python
lf_client = boto3.client('lakeformation', region_name='us-east-1')
iam_client = boto3.client('iam', region_name='us-east-1')
glue_client = boto3.client("glue", region_name='us-east-1')

AWS_ACCOUNT_ID = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'], capture_output=True, text=True).stdout.strip()
DATA_LAKE_BUCKET_NAME = f'de-c3w2a1-{AWS_ACCOUNT_ID}-us-east-1-data-lake'
SCRIPTS_BUCKET_NAME = f'de-c3w2a1-{AWS_ACCOUNT_ID}-us-east-1-scripts'
CURATED_DATABASE_NAME = 'curated_zone'
PRESENTATION_DATABASE_NAME = 'presentation_zone'
DATA_LOCATION_ARN = f"arn:aws:s3:::{DATA_LAKE_BUCKET_NAME}"
VOCLABS_ARN = f'arn:aws:iam::{AWS_ACCOUNT_ID}:role/voclabs'
```
:::

::: {.cell .markdown}
1.2.2. You will need access to the AWS console. Run the following code
to get the link.

*Note*: For security reasons, the URL to access the AWS console will
expire every 15 minutes, but any AWS resources you created will remain
available for the 2 hour period. If you need to access the console after
15 minutes, please rerun this code cell to obtain a new active link.
:::

::: {.cell .code}
``` python
with open('../.aws/aws_console_url', 'r') as file:
    aws_url = file.read().strip()

HTML(f'<a href="{aws_url}" target="_blank">GO TO AWS CONSOLE</a>')
```
:::

::: {.cell .markdown}
*Note:* If you see the window like in the following printscreen, click
on **logout** link, close the window and click on console link again.

![AWSLogout](images/AWSLogout.png)
:::

::: {.cell .markdown}
1.2.3. You need the ARN of the roles or users whom you are granting
permissions. The required functions are in the Python file
`scripts/lf_utils.py`.

You will use the `iam_client` and the `lf_utils.get_role_arn` function.
You first will grant data location access on the *Data Lake Bucket*
passing the ARN from the bucket to the
`lf_utils.grant_data_location_access`, then you will grant access to the
databases using `lf_utils.grant_database_access`. The two databases have
already been set up in the AWS Glue Catalog.
:::

::: {.cell .code}
``` python
glue_job_arn = lf_utils.get_role_arn(iam_client,'de-c3w2a1-glue-role')
lf_utils.grant_data_location_access(lf_client, glue_job_arn, DATA_LOCATION_ARN)
lf_utils.grant_database_access(lf_client, glue_job_arn, CURATED_DATABASE_NAME)
lf_utils.grant_database_access(lf_client, glue_job_arn, PRESENTATION_DATABASE_NAME)
```
:::

::: {.cell .markdown}
1.2.4. Run the following command in the terminal to set up the
environment:

``` bash
source scripts/setup.sh
```
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Architecture of the Data Lakehouse {#2---architecture-of-the-data-lakehouse}

Here\'s the proposed architecture of the Data Lakehouse. This
architecture can serve as a solution to the problem introduced in this
lab. It extracts the data from both source systems and uses AWS Glue
jobs to transform and store the data into the different medallion zones,
making it queryable for the data analyst through Amazon Athena.

![lake_diagram](images/Datalakehouse.png)
:::

::: {.cell .markdown}
Here is a brief description of the components:

-   **Data Sources:** You have already interacted with the source
    database in previous labs, the streaming service brings `ratings`
    data into an S3 bucket which you will call `source_bucket`.

-   **Medallion Layers:** As discussed previously, the architecture will
    be separated into three layers. The Glue jobs will be in charge of
    loading, transforming and storing the data from the source systems
    already located in AWS until the curated_zone.

    -   **Landing layer:** This step involves extracting data from the
        OLTP database in RDS and from the streaming service\'s S3 bucket
        and storing the raw data in the `landing_zone`.
        -   RDS Landing: An AWS Glue Job is used to connect to the RDS
            database and retrieve the data in CSV format.
        -   Streaming Landing: An AWS Glue Job is used to ingest the
            data from the `source_bucket` in S3, which is stored in JSON
            format.
    -   **Curated layer:** After extracting the data, Glue jobs will
        perform data transformation on top of the raw data. In addition
        to the transformation, the new curated sources will be stored in
        the `curated_zone` and added to the Glue Catalog.
        -   CSV Transformation: For the RDS tables, they will be
            enriched with metadata, the schema will be enforced and then
            the data will be stored in parquet format.
        -   JSON Transformation: While the streaming data, as the JSON
            data containing ratings, will be joined with the data from
            the OLTP system to create a table to be used for the ML
            team. This data will be stored as an Apache Iceberg table.
    -   **Presentation layer:** [Amazon
        Athena](https://aws.amazon.com/en/athena/), a serverless,
        interactive analytics service, is used to query the data stored
        in the `curated_zone` using the Glue Catalog. It enables
        SQL-like queries without needing to extract the data from S3 and
        load it into a traditional database. You will create
        presentation tables in this layer to solve some of the reporting
        and analytical needs using SQL queries, storing the data in
        Apache Iceberg format in the `presentation_zone`.

-   **End Users:** You will also be in charge of managing the Data
    Lakehouse in the role of administration, you will have to grant
    access to the data to one user: an employee from the Machine
    Learning team (`ML_User`).
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Landing Zone {#3---landing-zone}

To start the Data Lakehouse you need to bring the raw data into the Data
Lake. You will use Glue Jobs to do it. The scripts for these jobs are
located in `terraform/assets/landing_etl_jobs`. You will use Terraform
to deploy them.
:::

::: {.cell .markdown}
`<a id='3-1'>`{=html}`</a>`{=html}

### 3.1 - RDS and Streaming Landing {#31---rds-and-streaming-landing}

3.1.1. Open the script
`terraform/assets/landing_etl_jobs/de_c3w2a1_batch_ingress.py`. The code
is complete, try to understand it using the comments.
:::

::: {.cell .markdown}
3.1.2. To deploy the corresponding AWS Glue Job with Terraform, create
the Glue Connection to the RDS database. Open the file
`terraform/modules/landing_etl/glue.tf` and search for
`resource "aws_glue_connection" "rds_connection"`. In the
`connection_properties`, complete the [map
object](https://developer.hashicorp.com/terraform/language/expressions/types)
with the following key-value pairs:

-   Set `JDBC_CONNECTION_URL` to
    `"jdbc:mysql://${var.host}:${var.port}/${var.database}"`
-   Set `USERNAME` to `var.username`
-   Set `PASSWORD` to `var.password`

Make sure that you save the file after making changes in all of the
steps for this lab.
:::

::: {.cell .markdown}
3.1.3. Open the script
`terraform/assets/landing_etl_jobs/de_c3w2a1_json_ingress.py`. The code
is complete, go through the lines of the code with the comments to
understand it generally.
:::

::: {.cell .markdown}
3.1.4 Copy the glue scripts into the Glue script S3 bucket by running
the following cells:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/landing_etl_jobs/de_c3w2a1_batch_ingress.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_batch_ingress.py
```
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/landing_etl_jobs/de_c3w2a1_json_ingress.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_json_ingress.py
```
:::

::: {.cell .markdown}
`<a id='3-2'>`{=html}`</a>`{=html}

### 3.2 - Deployment {#32---deployment}

3.2.1. Open the `terraform/main.tf` file and make sure that only the
`landing_zone` module is uncommented (lines 1-17).
:::

::: {.cell .markdown}
3.2.2. Go to the `terraform` folder in the terminal, initialize
Terraform and deploy the resources running the following commands:
:::

::: {.cell .markdown}
``` bash
cd terraform
terraform init
terraform plan
terraform apply
```
:::

::: {.cell .markdown}
*Note*: You will need to type in `yes` and press `Enter` to perform
actions.
:::

::: {.cell .markdown}
You will get the outputs from Terraform, including two job names:
`glue_bucket_ingestion_job` and `glue_rds_ingestion_job`.
:::

::: {.cell .markdown}
*Note*: If there are errors in the commands or Terraform configuration
files, the terminal may crash. When this happens, you will see the
following message:

![etl_diagram](images/terminal_crash.png)

You can reopen the terminal by pressing `<code>`{=html}Ctrl +
\``</code>`{=html} (or `<code>`{=html}Cmd + \``</code>`{=html}) or by
navigating to View \> Terminal. In the terminal, go again to the
Terraform folder (`cd terraform`) and then try rerunning the required
commands. The error should now appear in the terminal. If the terminal
continues to crash, run the following command instead:
`terraform apply -no-color  2> errors.txt` This will create a text file
containing the error message without causing the terminal to crash.
:::

::: {.cell .markdown}
3.2.3. Use the Glue Job names (`de-c3w2a1-bucket-ingestion-etl-job` and
`de-c3w2a1-rds-ingestion-etl-job`) from the Terraform output to run each
job:
:::

::: {.cell .markdown}
``` bash
aws glue start-job-run --job-name <GLUE-JOB-NAME> | jq -r '.JobRunId'
```
:::

::: {.cell .markdown}
You should get `JobRunID` in the output. Check the status of the AWS
Glue Jobs exchanging the placeholder `<JOB-RUN-ID>` with the output from
the previous step:
:::

::: {.cell .markdown}
``` bash
aws glue get-job-run --job-name <GLUE-JOB-NAME> --run-id <JOB-RUN-ID> --output text --query "JobRun.JobRunState"
```
:::

::: {.cell .markdown}
When each run job has a `SUCCEEDED` status, you can continue with the
rest of the lab. Each of them should take around 2 minutes to complete.
You can run them at the same time.
:::

::: {.cell .markdown}
*Note*: If the Glue job fails, you can check its status in the AWS Glue
console, where an error message will be displayed. This message can help
you debug issues in the Glue scripts. After updating the scripts, be
sure to rerun the commands in step 3.1.4 to upload the updated scripts
to the scripts bucket.
:::

::: {.cell .markdown}
3.2.4. You can inspect the result of your Glue Jobs by running the
`aws cli` command in the following cells.
:::

::: {.cell .code}
``` python
!aws s3 ls s3://{DATA_LAKE_BUCKET_NAME}/landing_zone/rds/
```
:::

::: {.cell .code}
``` python
!aws s3 ls s3://{DATA_LAKE_BUCKET_NAME}/landing_zone/json/
```
:::

::: {.cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - Curated Zone {#4---curated-zone}

Once you have checked that the data is available in the `landing_zone`.
You can continue with the transformations that will be performed over
your data to be stored at the `curated_zone`.
:::

::: {.cell .markdown}
`<a id='4-1'>`{=html}`</a>`{=html}

### 4.1 - CSV Transformation {#41---csv-transformation}

4.1.1. Go to the `terraform/assets/transform_etl_jobs` and there you
will find the three scripts with which you will work to feed the data
into the second layer of your Data Lake. The script
`de_c3w2a1_batch_transform.py` will be used to perform the
transformation job: add metadata and enforce the schema, stored in CSV
format. The file already has the majority of the logic you need, you
just need to complete a couple of functions.
:::

::: {.cell .markdown}
4.1.2. **Add Metadata**. Metadata refers to data that describes data, it
can be related to lineage, source, history or version. Complete the
function `add_metadata` to add the required metadata following the
instructions in the code.
:::

::: {.cell .markdown}
4.1.3. **Enforce schema**. Schema enforcement is essential in data
engineering as it ensures data consistency and integrity by enforcing
predefined data types, structures and constraints. It helps to prevent
errors and facilitates seamless data integration and analysis processes.
In this case, the Glue Job contains the schema for each table as a
`StructType` object. For the columns that are not of type `StringType`,
you want to verify that the corresponding column in the `source_pd`
Dataframe has the same type. Complete the function `enforce_schema`
following the instructions in the code.
:::

::: {.cell .markdown}
`<a id='4-2'>`{=html}`</a>`{=html}

### 4.2 - JSON Transformation and Apache Iceberg {#42---json-transformation-and-apache-iceberg}

For the transformation of the data that arrives in JSON format, you will
execute two Glue Jobs:

-   One to join the JSON data with the CSV files that you stored in the
    landing zone.
-   A second job will take the JSON data and store it in another file
    format.
:::

::: {.cell .markdown}
For the two jobs, the result will be stored in the Apache Iceberg
format. The Apache Iceberg format is an open standard designed for
handling large analytical datasets efficiently. It\'s designed to
provide fast, consistent, and reliable access to large datasets in cloud
storage. Under the hood, Iceberg leverages a combination of file
formats, metadata management, and transactional capabilities to provide
efficient access to large datasets. It typically stores data using
columnar file formats such as Parquet or ORC. These formats are
optimized for analytical queries. On the other hand, Iceberg maintains
extensive metadata alongside the actual data files. This metadata
includes information about the table schema, partitioning strategy, file
locations, and transaction state. By storing metadata separately from
the data files, Iceberg ensures efficient management and access to table
metadata.

Some of the main features of the Apache Iceberg format are:

-   **Schema Flexibility**: Iceberg facilitates schema evolution,
    enabling the addition or removal of table columns without
    necessitating a full dataset rewrite. This ensures seamless backward
    compatibility as data structures evolve over time.
-   **Transactional Integrity**: Iceberg tables ensure atomic commits,
    guaranteeing that all table changes are transactional. This ensures
    that updates are either fully executed or entirely rolled back,
    maintaining data consistency and reliability.
-   **Data Partitioning**: Iceberg supports data partitioning based on
    one or more columns, enhancing query performance by minimizing data
    scan volumes. Particularly beneficial for large datasets.
-   **Comprehensive Metadata Management**: Iceberg maintains
    comprehensive metadata alongside the data, including schema details,
    partitioning specifications, and file locations. This metadata is
    stored separately from the data files, simplifying management and
    queries.
:::

::: {.cell .markdown}
Let\'s prepare the scripts for the Glue Job.
:::

::: {.cell .markdown}
4.2.1. First, you will save the data that is in JSON format into Apache
Iceberg. Open the file
`terraform/assets/transform_etl_jobs/de_c3w2a1_ratings_to_iceberg.py`
and search for the variable named `SqlQuery0`. Complete the provided
query with the following:
:::

::: {.cell .markdown}
``` sql
select * 
from ratings 
where ingest_ts = (select max(ingest_ts) from ratings)
;
```
:::

::: {.cell .markdown}
It will pull out the ratings with the most recent `ingest_ts` timestamp.
:::

::: {.cell .markdown}
4.2.2. Have a look at the query stored in the variable `SqlQuery1`. You
will have to modify it in a later stage in the lab, but in the meantime,
try to understand the purpose of it.
:::

::: {.cell .markdown}
4.2.3. Open the file located at
`terraform/modules/transform_etl/glue.tf`. Search for the
`resource "aws_glue_job" "ratings_to_iceberg_job"` resource. Complete
the following parameters:

-   Set the `timeout` parameter to 7.
-   Set the number of workers to 2.
-   In `default_arguments`, set the `--job-language` argument to
    `"python"`. Create another parameter named `--datalake-formats` and
    set the value to `"iceberg"` in the last line of the default
    parameters.
:::

::: {.cell .markdown}
4.2.4. Open the glue script
`terraform/assets/transform_etl_jobs/de_c3w2a1_json_transform.py`. This
script is in charge of unifying some data that you extracted from the
RDS Database with the ratings in JSON format so the ML Team can use it
to create some models. Search for the variable named `SqlQuery1` and
complete the query following the instructions in the code.
:::

::: {.cell .markdown}
4.2.5 Copy the glue scripts into the Glue script S3 bucket by run the
following cells:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_etl_jobs/de_c3w2a1_json_transform.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_json_transform.py
```
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_etl_jobs/de_c3w2a1_batch_transform.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_batch_transform.py
```
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_etl_jobs/de_c3w2a1_ratings_to_iceberg.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_ratings_to_iceberg.py
```
:::

::: {.cell .markdown}
`<a id='4-3'>`{=html}`</a>`{=html}

### 4.3 - Deployment {#43---deployment}

4.3.1. Open the file `terraform/main.tf` and uncomment the module named
`transform_etl` (lines 19-33).
:::

::: {.cell .markdown}
4.3.2. Open the file `terraform/outputs.tf` and uncomment the outputs
associated with the `transform_etl` module (lines 15-25).
:::

::: {.cell .markdown}
4.3.3. Deploy the Glue Jobs following the same steps as in section
[3.2](#3.2). You will need to execute three Glue Jobs:
`glue_csv_transform_job`, `glue_ratings_transform_job`,
`glue_ratings_to_iceberg_job` (job names
`de-c3w2a1-csv-transformation-job`,
`de-c3w2a1-ratings-transformation-job` and
`de-c3w2a1-ratings-to-iceberg-job`). Each of them should take around 3
minutes, you can run them at the same time.
:::

::: {.cell .markdown}
*Note*: If the Glue job fails, you can check its status in the AWS Glue
console, where an error message will be displayed. This message can help
you debug issues in the Glue scripts. After updating the scripts, be
sure to rerun the commands in step 4.2.5 to upload the updated scripts
to the scripts bucket.
:::

::: {.cell .markdown}
4.3.4. When each run job has a `SUCCEEDED` status, you can continue with
the rest of the lab. Use the `aws s3 ls` command to inspect again the
result of your transformations.
:::

::: {.cell .code}
``` python
!aws s3 ls s3://{DATA_LAKE_BUCKET_NAME}/curated_zone/ratings/iceberg
```
:::

::: {.cell .code}
``` python
!aws s3 ls s3://{DATA_LAKE_BUCKET_NAME}/curated_zone/customers
```
:::

::: {.cell .markdown}
`<a id='5'>`{=html}`</a>`{=html}

## 5 - Presentation zone {#5---presentation-zone}

For the presentation zone, you will use AWS Athena. You will be using
the `awswrangler` library to run `CREATE TABLE AS` queries on the
`presentation_zone`, this is an example of how it works.

*Note*: `awswrangler` uses the `pyarrow` library that has some functions
returning a `FutureWarning` due to deprecation, this doesn\'t affect
what you are trying to do so you can filter out those warnings.
:::

::: {.cell .code}
``` python
import warnings
warnings.simplefilter('ignore', FutureWarning)
```
:::

::: {.cell .markdown}
5.1. First, grant access to the tables in the `curated_zone` to the
voclabs role (the one used for the lab). You will use a Glue client with
`boto3` to obtain the table names in the catalog.
:::

::: {.cell .code}
``` python
curated_tables = glue_client.get_tables(DatabaseName=CURATED_DATABASE_NAME)
curated_tables_list = curated_tables["TableList"]
curated_table_names = [tableDict["Name"] for tableDict in curated_tables_list]
for table in curated_table_names:
    lf_utils.grant_table_access(lf_client, VOCLABS_ARN, CURATED_DATABASE_NAME, table)
```
:::

::: {.cell .markdown}
5.2. This is an example of creating an Iceberg Table based on a query
using Athena and the `curated_zone` tables.
:::

::: {.cell .code}
``` python
ctas_query = f"""CREATE TABLE ratings WITH (
	table_type = 'ICEBERG',
	location = 's3://{DATA_LAKE_BUCKET_NAME}/presentation_zone/ratings',
	is_external = false
) AS
SELECT *
FROM {CURATED_DATABASE_NAME}.ratings;"""
response = wr.athena.start_query_execution(
    sql=ctas_query,
    database=PRESENTATION_DATABASE_NAME,
    wait=True,
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/ratings'
)
print(response['Status']['State'])
```
:::

::: {.cell .markdown}
5.3. In the previous example, you made the `ratings` table available in
the `presentation` layer for data analysts to use. Bring the table
`ratings_for_ml` from the `curated zone` into the `presentation` layer,
cast the `process_ts` column to `varchar`.
:::

::: {.cell .code}
``` python
ctas_query = f"""CREATE TABLE ratings_for_ml WITH (
	table_type = 'ICEBERG',
	location = 's3://{DATA_LAKE_BUCKET_NAME}/presentation_zone/ratings_for_ml',
	is_external = false
) AS
SELECT customerNumber, city, state, postalCode, country, creditLimit, productCode,productLine, productScale, quantityinstock, buyprice, msrp, productRating, cast(process_ts as varchar) as process_ts
FROM {CURATED_DATABASE_NAME}.ratings_for_ml;"""
response = wr.athena.start_query_execution(
    sql=ctas_query,
    database=PRESENTATION_DATABASE_NAME,
    wait=True,
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/ratings_for_ml'
)
print(response['Status']['State'])
```
:::

::: {.cell .markdown}
5.4. Now you will create tables that may involve running aggregations or
joins across multiple tables from the curated zone. The first one will
involve returning the average sales per month and year and storing them
in the table `sales_report`.
:::

::: {.cell .code}
``` python
ctas_query = f"""CREATE TABLE sales_report WITH (
	table_type = 'ICEBERG',
	location = 's3://{DATA_LAKE_BUCKET_NAME}/presentation_zone/sales_report',
	is_external = false
) AS select year(orderdate) as sales_year, month(orderdate) as sales_month, sum(od.quantityordered * od.priceeach) as sales_total
from {CURATED_DATABASE_NAME}.orders o
left join {CURATED_DATABASE_NAME}.orderdetails od
on o.ordernumber = od.ordernumber
group by year(orderdate), month(orderdate)
order by year(orderdate), month(orderdate);"""
response = wr.athena.start_query_execution(
    sql=ctas_query,
    database=PRESENTATION_DATABASE_NAME,
    wait=True,
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/sales_report'
)
print(response['Status']['State'])
```
:::

::: {.cell .markdown}
5.5. For the second business object, create a table called
`ratings_per_product`, where you calculate the average rating and review
count per each product. The table must return the product code, product
name, average rating and count of reviews. Order the table by review
count in descending order and then by average count.
:::

::: {.cell .code}
``` python
ctas_query = f"""
CREATE TABLE ratings_per_product WITH (
	table_type = 'ICEBERG',
	location = 's3://{DATA_LAKE_BUCKET_NAME}/presentation_zone/ratings_per_product',
	is_external = false
) AS SELECT 	
p.productcode,
p.productname,
avg(productrating) as avg_rating,
count(*) as review_count
FROM {CURATED_DATABASE_NAME}.products p
LEFT JOIN {CURATED_DATABASE_NAME}.ratings r
ON p.productcode = r.productcode
GROUP BY p.productcode,p.productname
ORDER BY review_count DESC,avg_rating;
"""
response = wr.athena.start_query_execution(
    sql=ctas_query,
    database=PRESENTATION_DATABASE_NAME,
    wait=True,
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/ratings_per_product'
)
print(response['Status']['State'])
```
:::

::: {.cell .markdown}
5.6. Let\'s check on the new tables. Grant permissions to the voclabs
role on all the tables in the `presentation_zone` database and then
query each table, bringing 10 records for each.
:::

::: {.cell .code}
``` python
presentation_tables = glue_client.get_tables(DatabaseName=PRESENTATION_DATABASE_NAME)
presentation_tables_list = presentation_tables["TableList"]
presentation_table_names = [tableDict["Name"] for tableDict in presentation_tables_list]
for table in presentation_table_names:
    lf_utils.grant_table_access(lf_client, VOCLABS_ARN, PRESENTATION_DATABASE_NAME, table)
```
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/sample_ratings'
)
df.head()
```
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings_for_ml LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/sample_ratings_for_ml'
)
df.head()
```
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM sales_report LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/sample_sales_report'
)
df.head()
```
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings_per_product LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    s3_output = f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/sample_ratings_per_product'
)
df.head()
```
:::

::: {.cell .markdown}
*Note*: **The next sections 6 and 7 are both optional and not graded. If
you choose to skip them, please submit the lab by clicking on
`Submit assignment` (top right corner). Section 8 outlines the clean up
process in case you want to rerun some Terraform commands (also not
graded).**
:::

::: {.cell .markdown}
`<a id='6'>`{=html}`</a>`{=html}

## 6 - \[Optional and Not Graded\] - Some Features of Iceberg Format {#6---optional-and-not-graded---some-features-of-iceberg-format}
:::

::: {.cell .markdown}
`<a id='6-1'>`{=html}`</a>`{=html}

### 6.1 - Schema Evolution {#61---schema-evolution}

Now, you will see some of the features that the Iceberg format has. You
will focus mostly on the schema evolution and querying data from
different versions. The team that serves the data in the S3 source
bucket has told you that they added a new column holding the date at
which the customers performed the rating to the product, named
`ratingtimestamp`. The data is hosted in the same bucket, but at the S3
key named `ratings_with_timestamp`. This will be the folder where all
new data will continue to be stored. Your task is to add the new column
to your `ratings` table at the `curated_zone` database and then extract
the new data. Let\'s use the `awswrangler` package again to perform some
queries over AWS Athena.
:::

::: {.cell .markdown}
6.1.1. Explore the current available tables in your `curated_zone`
database:
:::

::: {.cell .code}
``` python
wr.catalog.tables(database=CURATED_DATABASE_NAME)
```
:::

::: {.cell .markdown}
6.1.2. You can see the `ratings` table there with some of the current
columns. Query the table `ratings` at its current state. Create a query
to select all the columns from the `ratings` table; limit your search to
10 rows. Save this query as a string into the `sql` variable and then
execute the query.
:::

::: {.cell .code}
``` python
sql = "SELECT * FROM ratings LIMIT 10;"
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .markdown}
6.1.3. Now, let\'s add the new column to the `iceberg` table. Open the
file `terraform/assets/alter_table_job/de_c3w2a1_alter_ratings_table.py`
and search for the function named `add_column()`. There you will see an
SQL Statement that performs an `ALTER TABLE` to add a new column. The
column name and type are passed as parameters to the Glue Job.
:::

::: {.cell .markdown}
6.1.4. Open the file `terraform/main.tf` and uncomment the module named
`alter_table` (lines 35-50).
:::

::: {.cell .markdown}
6.1.5. Open the file `terraform/outputs.tf` and uncomment the outputs
associated with the `alter_table` module (lines 28-30).
:::

::: {.cell .markdown}
6.1.6 Copy the glue job using the following cell:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/alter_table_job/de_c3w2a1_alter_ratings_table.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_alter_ratings_table.py
```
:::

::: {.cell .markdown}
6.1.6. Deploy the Glue Job following the same steps as in section 3.2.
The Glue Job name will be in the new output `glue_alter_table_job` (job
name `de-c3w2a1-alter-table-job`). It will take about 2 minutes to run.
:::

::: {.cell .markdown}
6.1.7. Once your job has finished with a `SUCCEEDED` status, run the
following cell to check that the new column has been added.
:::

::: {.cell .code}
``` python
sql = "SELECT * FROM ratings LIMIT 10;"
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .markdown}
6.1.8. The table schema has been changed, you will need to modify your
previous Glue Job to take into account such new changes. Open the Glue
Job script
`terraform/assets/landing_etl_jobs/de_c3w2a1_json_ingress.py`. Search
for the creation of the object named `source_ratings_json`. You can see
that in the `connection_options` there is a `"paths"` parameter. Change
the key in the source data lake from `ratings` to
`ratings_with_timestamp`, save the file. Run the following cell to copy
the new version of the script:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/landing_etl_jobs/de_c3w2a1_json_ingress.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_json_ingress.py
```
:::

::: {.cell .markdown}
6.1.9. Run again the corresponding Glue Job associated with the
terraform output `glue_bucket_ingestion_job`. Wait until the job has
finished with the `SUCCEEDED` status (it will take about 2 minutes).
:::

::: {.cell .markdown}
6.1.10. Now that the new ratings with the new column have arrived in
your `landing_zone`, it is time to use them in your next jobs. For the
sake of simplicity, you will only run the job that transforms the data
from JSON format into `iceberg`. Open the
`terraform/assets/transform_etl_jobs/de_c3w2a1_ratings_to_iceberg.py`
file and search for the query that performs a merge into your database.
In the `UPDATE SET` statement, add the `ratingtimestamp` column using
the same syntax that you can see for the other two columns. Run the
following cell to update the script file in the bucket:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_etl_jobs/de_c3w2a1_ratings_to_iceberg.py s3://{SCRIPTS_BUCKET_NAME}/de_c3w2a1_ratings_to_iceberg.py
```
:::

::: {.cell .markdown}
6.1.11. Execute again the `glue_ratings_to_iceberg_job`. It will take
about 3 minutes to run.
:::

::: {.cell .markdown}
6.1.12. Once the job has finished successfully, execute again a query
over the `ratings` table to see the new results.
:::

::: {.cell .code}
``` python
sql = "SELECT * FROM ratings WHERE ratingtimestamp IS NOT NULL LIMIT 10;"
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .markdown}
Now that you have realized that the schema of your table has evolved and
you were able to add data with a new schema you will explore another
feature of the `iceberg` format.
:::

::: {.cell .markdown}
`<a id='6-2'>`{=html}`</a>`{=html}

### 6.2 - Versioning with Iceberg {#62---versioning-with-iceberg}

One of the advantages of the `iceberg` format is that it allows you to
save different versions of your tables. Those versions are stored as
snapshots. To check the number of versions/snapshots of your `ratings`
table, which is the one that has changed, you can execute the following
query:
:::

::: {.cell .code}
``` python
sql = 'SELECT CAST(committed_at AS VARCHAR) as committed_at, snapshot_id,  parent_id, operation, manifest_list FROM "ratings$snapshots" order by committed_at asc;'
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .markdown}
You can see snapshot ID, the time at which that snapshot was created
(`committed_at`) and also a `parent_id`, which corresponds to the
previous version of the `snapshot_id`. To check the data that was stored
at a particular version of your table, you can add the
`FOR VERSION AS OF` statement at the end of your query, followed by the
snapshot ID that you want to query. Complete the following query with
the snapshot ID of your first table version (this is the one that has no
`parent_id`) replacing the placeholder `<FIRST-SNAPSHOT-ID>`. Then
complete another cell with the last snapshot ID (replacing
`<LAST-SNAPSHOT-ID>`); to request the data of the last version of your
data. You can execute the two of them to compare.
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings FOR VERSION AS OF <FIRST-SNAPSHOT-ID>;'
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings FOR VERSION AS OF <LAST-SNAPSHOT-ID>;'
df = wr.athena.read_sql_query(sql, database=CURATED_DATABASE_NAME, s3_output=f's3://{DATA_LAKE_BUCKET_NAME}/athena_output/')
df.head()
```
:::

::: {.cell .markdown}
With this exercise, you have used the data versioning feature of Apache
Iceberg which allows users to track changes to datasets over time by
creating immutable snapshots of the data at different points in time.
Two important points to take into account are:

-   When a user creates a new version of a dataset in Iceberg, Iceberg
    creates an immutable snapshot of the dataset\'s state at that point
    in time. This snapshot includes all the data files and metadata
    associated with the dataset.
-   On the other hand, Iceberg maintains metadata to track the history
    of dataset versions. Each version is assigned a unique identifier,
    typically a timestamp or version number, and includes information
    about the changes made in that version.
:::

::: {.cell .markdown}
`<a id='7'>`{=html}`</a>`{=html}

## 7 - \[Optional and Not Graded\] - Granting Permissions with Lake Formation {#7---optional-and-not-graded---granting-permissions-with-lake-formation}

In your company there is a Machine Learning team that will use the
results of your pipelines, in particular, they will use the
`ratings_for_ml` table that is stored in the `presentation_zone`
database. They use the AWS user `ml_data_lake_user` to interact with AWS
resources and query the data from the `ratings_for_ml` table. Your task
is to provide them only with the necessary access to the information,
which means, they will only be able to query the `ratings_for_ml` table
and shouldn\'t have access to any other table in the `presentation_zone`
database. In the same way, they shouldn\'t be able to modify the data in
that table.

You have access to the credentials of `ml_data_lake_user` to test the
permissions that you will grant in this section. Run the following cell
to grab the credentials from the AWS Secret Manager service.
:::

::: {.cell .code}
``` python
secrets_client = boto3.client('secretsmanager')
response = secrets_client.get_secret_value(SecretId='/datalake/credentials/ml_data_lake_user')
crendentials = json.loads(response['SecretString'])
```
:::

::: {.cell .markdown}
Now, you will create a `boto3` session with those credentials to access
AWS resources as `ml_data_lake_user`.
:::

::: {.cell .code}
``` python
session = boto3.Session(aws_access_key_id = crendentials['ACCESS_KEY'], aws_secret_access_key = crendentials['SECRET_KEY'])
```
:::

::: {.cell .markdown}
Pass the `boto3` session you just created to the `read_sql_query` method
below through the `boto3_session` parameter and execute the query. The
`ml_data_lake_user` user doesn\'t have permission over the Data Lake
Bucket so you will use an AWS Athena workgroup to execute the query.
This query is expected to fail due to insufficient Lakeformation
Permissions.
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM ratings_for_ml LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    workgroup='de-c3w2a1-workgroup', 
    boto3_session=session)
df.head()
```
:::

::: {.cell .markdown}
Now, you will provide access to the `presentation_zone` database and the
`ratings_for_ml` table. For that, you require the `arn` of the
`ml_data_lake_user`. Execute the following command with the
corresponding user name to grab its arn. Save this value in the `ml_arn`
variable in the subsequent cell replacing the placeholder
`<ML_USER_ARN>` and execute it.
:::

::: {.cell .code}
``` python
!aws iam get-user --user-name ml_data_lake_user
```
:::

::: {.cell .code}
``` python
ml_arn = '<ML_USER_ARN>'
lf_utils.grant_database_access(lf_client, ml_arn, PRESENTATION_DATABASE_NAME)
```
:::

::: {.cell .markdown}
Finally, although the user has access to the `presentation_zone`
database, it wouldn\'t be able to query the `ratings_for_ml` table
directly. You have to provide direct access to that table using the
`lf_utils.grant_table_access()` function. Remember that the
`ml_data_lake_user` should be able to only read the data. Pass the table
name and the list \[\'SELECT\'\] to the function below and execute it.
After that, run the query again to check that the user has the
appropriate access.
:::

::: {.cell .code}
``` python
lf_utils.grant_table_access(lf_client, ml_arn, PRESENTATION_DATABASE_NAME, 'ratings_for_ml', ['SELECT'])
```
:::

::: {.cell .code}
``` python
session = boto3.Session(aws_access_key_id = crendentials['ACCESS_KEY'], aws_secret_access_key = crendentials['SECRET_KEY'])

sql = 'SELECT * FROM ratings_for_ml LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    workgroup='de-c3w2a1-workgroup', 
    boto3_session=session)
df.head()
```
:::

::: {.cell .markdown}
As a final health check, you can run the following query with another
table from the `presentation_zone` database to check that the user does
not have read access to it. Change the placeholder
`<PRESENTATION_LAYER_TABLE>` with the table name that you want to
choose.
:::

::: {.cell .code}
``` python
sql = 'SELECT * FROM <PRESENTATION_LAYER_TABLE> LIMIT 10;'
df = wr.athena.read_sql_query(
    sql, 
    database=PRESENTATION_DATABASE_NAME, 
    workgroup='de-c3w2a1-workgroup', 
    boto3_session=session)
df.head()
```
:::

::: {.cell .markdown}
The medallion architecture offers several advantages for organizing and
managing a data lakehouse effectively. This architecture enables the
decoupling of storage and computing, allowing users to scale each layer
independently based on their requirements. With this separation,
organizations can optimize costs by utilizing cost-effective storage
solutions while leveraging powerful computing resources for data
processing and analytics tasks. In this particular case, you leveraged
Amazon S3 as your data lake and storage solution and Amazon Glue for the
computing required to transform the data, at the end you used Athena to
create the business objects required by the organization to perform
crucial analytics.

Lakeformation, a service provided by AWS, complements the medallion
architecture by offering robust tools for securing and managing access
to data within the data lake. Data governance is crucial for ensuring
data integrity, compliance, and security, and Lakeformation plays a
pivotal role by providing tools and capabilities to enforce governance
policies effectively across a data lakehouse environment.

Please submit the lab by clicking on **Submit assignment** (top right
corner).
:::

::: {.cell .markdown}
`<a id='8'>`{=html}`</a>`{=html}

## 8 - Enviroment Clean Up {#8---enviroment-clean-up}

*Note*: **This section is required only if you want to rerun the
Terraform commands. To receive a proper grade for this lab, please make
sure that you have submitted the lab by clicking on `Submit assignment`
(top right corner) before performing the clean up steps outlined in this
section.**

There might be cases when you will want to rerun Terraform commands. To
avoid any issues with the already created resources, it is recommended
to clean up environment before that. To start the cleanup, run the
following command inside the `terraform` folder:

``` bash
terraform destroy
```

As you have seen in this lab, AWS LakeFormation manages access to
several data locations, at the end of the lab you need to restore the
LakeFormation permissions to the default and revoke all the granted
permissions. For this, you will call a Lambda function to perform the
reset operation, use the following command in the terminal:
:::

::: {.cell .markdown}
``` bash
aws lambda invoke --function-name de-c3w2a1-Lambda-Setup --cli-binary-format raw-in-base64-out --payload '{ "RequestType": "Delete" }' --invocation-type Event response.json
```
:::

::: {.cell .markdown}
You can check the status of your execution by using the following steps:
:::

::: {.cell .markdown}
-   First, get the log stream name with the following command:
:::

::: {.cell .markdown}
``` bash
aws logs describe-log-streams --log-group-name '/aws/lambda/de-c3w2a1-Lambda-Setup' --query logStreams[*].logStreamName
```
:::

::: {.cell .markdown}
-   This will return you the stream names associated with your
    execution. Copy the last one and paste it in the following command
    replacing the `<LOG-STREAM-NAME>` placeholder:
:::

::: {.cell .markdown}
``` bash
aws logs get-log-events --log-group-name '/aws/lambda/de-c3w2a1-Lambda-Setup' --log-stream-name '<LOG-STREAM-NAME>'
```
:::

::: {.cell .markdown}
-   You will be able to inspect the content of your log stream. The last
    part of the message should look similar to the following output:
:::

::: {.cell .markdown}
``` json
    {
        "timestamp": 1716912975732,
        "message": "END RequestId: 8dc59dd1-cbca-4c2a-912b-3bcfc5049962\n",
        "ingestionTime": 1716912984756
    },
    {
        "timestamp": 1716912975732,
        "message": "REPORT RequestId: 8dc59dd1-cbca-4c2a-912b-3bcfc5049962\tDuration: 22984.81 ms\tBilled Duration: 22985 ms\tMemory Size: 128 MB\tMax Memory Used: 92 MB\t\n",
        "ingestionTime": 1716912984756
    }
```
:::

::: {.cell .markdown}
If you see the `END` of the request and the corresponding `REPORT`, that
means that your lambda has finished. While you wait for your lambda
execution to finish.
:::

::: {.cell .code}
``` python
```
:::
