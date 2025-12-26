::: {.cell .markdown}
# Capstone Project Part 1 - ETL and Data Modeling

During this capstone project, you will develop a data pipeline as part
of a new project in the company DeFtunes. You will put into practice all
the tools you have been using during the whole specialization.

**Important guidelines:**

-   If you\'re redoing the lab, make sure to repeat the lab steps as
    they are stated. Please avoid skipping any step.
-   Please avoid rebooting the lab within the 3-hour lab session, as
    this will make the Coursera lab session out of sync with the AWS
    session. Only reboot the lab at the beginning if needed or at the
    end of the 3-hour session to create another session. You can reboot
    by clicking on the ![](images/reboot.png) button and clicking on the
    Reboot button.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction](#1)
-   [2 - Data Sources](#2)
-   [3 - Exploratory Data Analysis](#3)
-   [4 - ETL Pipeline with AWS Glue and Terraform](#4)
    -   [4.1 - Landing Zone](#4-1)
    -   [4.2 - Transformation Zone](#4-2)
    -   [4.3 - Serving Zone](#4-3)
-   [5 - Data Modeling with dbt and Redshift Spectrum](#5)
    -   [5.1 - Redshift Setup](#5-1)
    -   [5.2 - Redshift Test](#5-2)
    -   [5.3 - dbt Setup](#5-3)
    -   [5.4 - Data Modeling](#5-4)
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction {#1---introduction}

DeFtunes is a new company in the music industry, offering a
subscription-based app for streaming songs. Recently, they have expanded
their services to include digital song purchases. With this new retail
feature, DeFtunes requires a data pipeline to extract purchase data from
their new API and operational database, enrich and model this data, and
ultimately deliver the comprehensive data model for the Data Analysis
team to review and gain insights. Your task is to develop this pipeline,
ensuring the data is accurately processed and ready for in-depth
analysis.

Here is the diagram with the main requirements for this project:

![Capstone_Diagram](images/Capstone-diagram.png)

1.  The pipeline has to follow a medallion architecture with a landing,
    transform and serving zone.
2.  The data generated in the pipeline will be stored in the company\'s
    data lake, in this case, an S3 bucket.
3.  The silver layer should use Iceberg tables, and the gold layer
    should be inside Redshift.
4.  The pipeline should be reproducible, you will have to implement it
    using Terraform.
5.  The data should be modelled into a star schema in the serving layer,
    you should use dbt for the modelling part.

Before starting, you will need to import some required libraries and
modules for the capstone development.
:::

::: {.cell .code}
``` python
import json
import requests
import pandas as pd

from IPython.display import HTML

%load_ext sql
```
:::

::: {.cell .markdown}
In the terminal run the following command to set up the environment:

<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 50%;">

```bash
source scripts/setup.sh
```

</div>
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Data Sources {#2---data-sources}

The first data source you will be using is the DeFtunes operational
database, which is running in RDS with a Postgres engine. This database
contains a table with all the relevant information for the available
songs that you can purchase. You will connect to the table using the
`%sql` magic, but first you need to set up the connection to the AWS
console.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.1.`</b>`{=html}`</span>`{=html}
Get the link to the AWS console

Run the following code to get the link to the AWS console. If everything
works well, you should get a link to the AWS Console. Click on it.

**Note**: For security reasons, the URL to access the AWS console will
expire every 15 minutes, but any AWS resources you created will remain
available for the 3 hour period. If you need to access the console after
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
**Note:** If you see the window like in the following printscreen, click
on **logout** link, close the window and click on console link again.

![AWSLogout](images/AWSLogout.png)
:::

::: {.cell .markdown}
Go to **CloudFormation** in the AWS console. Click on the alphanumeric
stack name and search for the **Outputs** tab. You will see the keys
`PostgresEndpoint` and `ScriptsBucket`, copy the corresponding
**Values** and replace the placeholders in the cells below (please,
replace the whole placeholder including the brackets `<>`). Then run
each cell code.

**Note:** If you\'re redoing the lab, make sure to check
**CloudFormation** to get the updated values for `SCRIPTS_BUCKET_NAME`
and `RDSDBHOST` in the cells below.
:::

::: {.cell .code}
``` python
SCRIPTS_BUCKET_NAME = '<SCRIPTS_BUCKET>'
```
:::

::: {.cell .code}
``` python
RDSDBHOST = '<POSTGRES_ENDPOINT>'
RDSDBPORT = '5432'
RDSDBNAME = 'postgres'
RDSDBUSER = 'postgresuser'
RDSDBPASSWORD = 'adminpwrd'

postgres_connection_url = f'postgresql+psycopg2://{RDSDBUSER}:{RDSDBPASSWORD}@{RDSDBHOST}:{RDSDBPORT}/{RDSDBNAME}'
%sql {postgres_connection_url}
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.2.`</b>`{=html}`</span>`{=html}
Test that the connection works by running the following query.
:::

::: {.cell .code}
``` python
%%sql
SELECT schema_name
FROM information_schema.schemata;
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.3.`</b>`{=html}`</span>`{=html}
Inside the `deftunes` schema there is a table `songs`. Let\'s query a
sample from it:

**Note**: The `songs` table is based on the Million Song Dataset, more
information can be found [here](http://millionsongdataset.com/).
:::

::: {.cell .code}
``` python
%%sql
SELECT *
FROM deftunes.songs
LIMIT 5;
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.4.`</b>`{=html}`</span>`{=html}
The second data source is a new API designed for the song purchase
process. This API contains information on the purchases done by the
users of the App and also contains relevant information about each user.
Copy the API endpoint value from the CloudFormation outputs tab and
replace the placeholder `<API_ENDPOINT>` with it.

**Note:** If you\'re redoing the lab, make sure to check
**CloudFormation** to get the updated value for `API_ENDPOINT` in the
cell below.
:::

::: {.cell .code}
``` python
API_ENDPOINT = "<API_ENDPOINT>"
```
:::

::: {.cell .markdown}
You can also access the documentation to the API by opening a new
browser tab, pasting the API endpoint value and adding `/docs` at the
end. You will see an interactive interface to test the API.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.5.`</b>`{=html}`</span>`{=html}
The first endpoint you\'ll use, relative to the base API URL you just
copied, is the sessions path. This endpoint retrieves the transactional
sessions. Let\'s test the API by sending a `GET` request in the next
cell. If the request is successful, the `sessions_response` object
should return a `200` status code.
:::

::: {.cell .code}
``` python
request_start_date = "2020-01-01"
request_end_date = "2020-03-01"
sessions_response = requests.get(f'http://{API_ENDPOINT}/sessions?start_date={request_start_date}&end_date={request_end_date}')
print(sessions_response.status_code)
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.6.`</b>`{=html}`</span>`{=html}
You can get the content of the response in JSON format using the
`.json()` method, let\'s print the first record with the following cell.
:::

::: {.cell .code}
``` python
sessions_json = sessions_response.json()
print(json.dumps(sessions_json[0], indent=4))
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}2.7.`</b>`{=html}`</span>`{=html}
The second endpoint is to the `users` path in the API, it retrieves the
transactional sessions. Perform a GET request to the endpoint with the
next cell, then print a sample with the cell after that one.
:::

::: {.cell .code}
``` python
users_request = requests.get(f'http://{API_ENDPOINT}/users')
print(users_request.status_code)
```
:::

::: {.cell .code}
``` python
users_json = users_request.json()
print(json.dumps(users_json[0], indent=4))
```
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Exploratory Data Analysis {#3---exploratory-data-analysis}

To better understand the data sources, start analyzing the data types
and values that come from each source. You can use the pandas library to
perform Exploratory Data Analysis (EDA) on samples of data.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}3.1.`</b>`{=html}`</span>`{=html}
Let\'s begin with the `songs` table in the source RDS database, we will
take advantage of the `%sql` magic to bring a sample with SQL and
convert it into a pandas dataframe.
:::

::: {.cell .code}
``` python
songs_result = %sql SELECT *FROM deftunes.songs LIMIT 5
songs_df = songs_result.DataFrame()
songs_df.head()
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}3.2.`</b>`{=html}`</span>`{=html}
Use Pandas info() method to print out a summary of information about the
dataframe, including information about the columns such as their data
types.
:::

::: {.cell .code}
``` python
print(songs_df.info())
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}3.3.`</b>`{=html}`</span>`{=html}
Use the describe() method to generate a summary of statistics about the
numerical columns in the DataFrame. The describe() method can also
generate descriptive statistics for the categorical columns but by
default only numerical columns are returned.
:::

::: {.cell .code}
``` python
songs_df.describe()
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}3.4.`</b>`{=html}`</span>`{=html}
Convert JSON objects `sessions_json` and `users_json` into pandas
dataframes, and display the first few rows.
:::

::: {.cell .code}
``` python
session_df = pd.json_normalize(sessions_json)
session_df.head()
```
:::

::: {.cell .code}
``` python
user_df = pd.json_normalize(users_json)
user_df.head()
```
:::

::: {.cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - ETL Pipeline with AWS Glue and Terraform {#4---etl-pipeline-with-aws-glue-and-terraform}

Now you will start creating the required resources and infrastructure
for your data pipeline. Remember that you will use a medallion
architecture.

The pipeline will be composed by the following steps:

-   An extraction job to get the data from the PostgreSQL Database. This
    data will be stored in the landing zone of your Data Lake.
-   An extraction job to get the data from the two API endpoints. This
    data will be stored in the landing zone of your Data Lake in JSON
    format.
-   A transformation job that takes the raw data extracted from the
    PostgreSQL Database, casts some fields to the correct data types,
    adds some metadata and stores the dataset in Iceberg format.
-   A transformation that takes the JSON data extracted from the API
    endpoints, normalizes some nested fields, adds metadata and stores
    the dataset in Iceberg format.
-   The creation of some schemas in your Data Warehouse hosted in
    Redshift.
:::

::: {.cell .markdown}
`<a id='4-1'>`{=html}`</a>`{=html}

### 4.1 - Landing Zone {#41---landing-zone}

For the landing zone, you are going to create three Glue Jobs: one to
extract the data from the PostgreSQL database and two to get the data
from each API\'s endpoint. You are going to create those jobs using
Terraform to guarantee that the infrastructure for each job will be
always the same and changes can be tracked easily. Let\'s start by
creating the jobs and then creating the infrastructure.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.1.`</b>`{=html}`</span>`{=html}
Go to the `terraform/assets/extract_jobs` folder. You will find two
scripts

-   [`de-c4w4a1-extract-songs-job.py`](terraform/assets/extract_jobs/de-c4w4a1-extract-songs-job.py):
    this script extracts data from the PostgreSQL source database.
-   [`de-c4w4a1-api-extract-job.py`](terraform/assets/extract_jobs/de-c4w4a1-api-extract-job.py):
    this script extracts data from the API. Endpoints can be provided
    through parameters.

Go through each file to understand the details of each job. **You do not
need to change anything**.

In a later section, you will run the Glue Jobs. Note that data in the
landing zone of your Data Lake will be stored in subfolders named
according to the ingestion date, which defaults to the server\'s current
date in Pacific Time (UTC -7). This ensures consistent date alignment
with the server timezone used for job scheduling and data partitioning.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.2.`</b>`{=html}`</span>`{=html}
Open the `terraform/modules/extract_job/glue.tf` file and go through it.
In this file you will set all the required resources to create the Glue
Jobs. **You only need to change lines 76 and 111 with the API endpoint
from the CloudFormation outputs**.

**Note:** If you\'re redoing the lab, make sure to check
**CloudFormation** to get the updated value for `API_ENDPOINT`.
**Remember to only update the `API_ENDPOINT`**, and not the whole URL
structure.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.3.`</b>`{=html}`</span>`{=html}
Explore the rest of the files of the `extract_job` module to understand
the whole infrastructure. Avoid performing further changes to other
files. Here is the summary of what you can find in those files:

-   In
    [`terraform/modules/extract_job/iam.tf`](terraform/modules/extract_job/iam.tf)
    file you can find the creation of the role used to execute the Glue
    Jobs. There is also the attachment of a policy holding the
    permissions. Those permissions can be found directly in the
    [`terraform/modules/extract_job/policies.tf`](terraform/modules/extract_job/policies.tf)
    file.
-   The
    [`terraform/modules/extract_job/network.tf`](terraform/modules/extract_job/network.tf)
    has the definition of the private subnet and the source database
    security group used to create the Glue Connection used to allow the
    Glue Jobs to connect to the source PostgreSQL database.
-   The
    [`terraform/modules/extract_job/variables.tf`](terraform/modules/extract_job/variables.tf)
    contains the necessary input parameters for this module, while the
    [`terraform/modules/extract_job/outputs.tf`](terraform/modules/extract_job/outputs.tf)
    sets the possible outputs that terraform will show in the console
    from this module.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.4.`</b>`{=html}`</span>`{=html}
Open the [`terraform/main.tf`](terraform/main.tf) file and go through
the lines of the module named `extract_job` (lines 1 to 15). Open the
[`terraform/outputs.tf`](terraform/outputs.tf) file and go through the
lines of the the extract module (lines 5 to 20). You will first create
the resources of this module. **You do not need to change anything** in
these files or comment the other modules, as you\'ll specify the module
in the command line in 4.1.6..
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.5.`</b>`{=html}`</span>`{=html}
Run the following cells to copy the glue scripts into the Scripts
Bucket. With this, you are ready to deploy the first module of your
infrastructure.
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/extract_jobs/de-c4w4a1-api-extract-job.py s3://{SCRIPTS_BUCKET_NAME}/de-c4w4a1-api-extract-job.py
```
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/extract_jobs/de-c4w4a1-extract-songs-job.py s3://{SCRIPTS_BUCKET_NAME}/de-c4w4a1-extract-songs-job.py
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.6.`</b>`{=html}`</span>`{=html}
In the terminal, go to the `terraform` folder and deploy the
infrastructure with the following commands.

<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 80%;">

```bash
cd terraform
terraform init || echo "$?"
terraform plan || echo "$?"
terraform apply -target=module.extract_job || echo "$?"
```
</div>

**Note**: Remember that the command `terraform apply` will prompt you to
reply `yes`. Do not type anything other that `yes` or the apply command
will fail. `echo "$?"` prints that exit status to the terminal and
prevents the terminal from crashing (`$?` contains the exit status
(return code) of the executed command).
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.1.7.`</b>`{=html}`</span>`{=html}
You will get some outputs, in particular, you require the following
three: `glue_api_users_extract_job`, `glue_sessions_users_extract_job`
and `glue_rds_extract_job`. Those outputs correspond to the three glue
jobs that will extract the data from the API endpoints and the database
respectively. Run the following command three times in the terminal to
execute the three Glue jobs.

<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">

```bash
aws glue start-job-run --job-name <JOB-NAME> | jq -r '.JobRunId' || echo "$?"
```
</div>

For each run, replace `<JOB-NAME>` with one of the following values:

-   `de-c4w4a1-api-users-extract-job`
-   `de-c4w4a1-api-sessions-extract-job`
-   `de-c4w4a1-rds-extract-job`

You can run all three jobs in parallel, meaning you can execute the
commands one after the other without waiting for each to finish before
starting the next.

You should get `JobRunID` in the output of each command. Use this job
run ID to track each job status by using this command, replacing the
`<JOB-NAME>` and `<JobRunID>` placeholders.
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

``` bash
aws glue get-job-run --job-name <JOB-NAME> --run-id <JobRunID> --output text --query "JobRun.JobRunState" || echo "$?"
```

</div>

Wait until the statuses of those three jobs change to `SUCCEEDED` (each
job should take around 3 mins).
:::

::: {.cell .markdown}
**Note**: If the Glue job fails, you can check its status in the AWS
Glue console, where an error message will be displayed. This message can
help you debug issues in the Glue scripts. After updating the scripts,
be sure to rerun the commands in step 4.1.5 to upload the updated
scripts to the scripts bucket.
:::

::: {.cell .markdown}
`<a id='4-2'>`{=html}`</a>`{=html}

### 4.2 - Transformation Zone {#42---transformation-zone}

Once you have run the jobs that feed the first layer of your Data Lake
three-tier architecture, it is time to generate the jobs to transform
the data and store it in the second layer. The methodology will be
similar to the previous zone: you will create the Glue Job scripts to
take the data out of the landing layer, transform it and put it into the
transformation zone layer. Then you will create the necessary resources
in AWS using Terraform.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.1.`</b>`{=html}`</span>`{=html}
Go to the `terraform/assets/transform_jobs` folder. You will find two
scripts:

-   [`de-c4w4a1-transform-songs-job.py`](terraform/assets/transform_jobs/de-c4w4a1-transform-songs-job.py)
-   [`de-c4w4a1-transform-json-job.py`](terraform/assets/transform_jobs/de-c4w4a1-transform-json-job.py)

The two scripts will take the data out of the `landing_zone` layer in
the Data Lake and, after some transformations, will store the data into
the `transform_zone` layer in Apache Iceberg format.

Open each of them and go through the code. **You do not need to change
anything**.

If you want to know more about saving data into Apache Iceberg format
using the Glue Catalog, you can check the
[documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html#aws-glue-programming-etl-format-iceberg-insert).
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.2.`</b>`{=html}`</span>`{=html}
Open the `terraform/modules/transform_job/glue.tf` file. Go through the
code, **you do not need to change anything**.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.3.`</b>`{=html}`</span>`{=html}
Open the `terraform/main.tf` file and go through the lines of the module
named `transform_job` (lines 17 to 31). Open the `terraform/outputs.tf`
file and go through the lines of the the transform module (lines 22 to
34). You will now create the resources of this module. **You do not need
to change anything** in these files (you do not need to comment the
other modules, as you\'ll specify the module in the command line in
4.2.5.).
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.4`</b>`{=html}`</span>`{=html}
Copy the glue script into the Scripts bucket by running the following
cells:
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_jobs/de-c4w4a1-transform-json-job.py s3://{SCRIPTS_BUCKET_NAME}/de-c4w4a1-transform-json-job.py
```
:::

::: {.cell .code}
``` python
!aws s3 cp ./terraform/assets/transform_jobs/de-c4w4a1-transform-songs-job.py s3://{SCRIPTS_BUCKET_NAME}/de-c4w4a1-transform-songs-job.py
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.5.`</b>`{=html}`</span>`{=html}
In the terminal:

-   First, make sure you\'re in the `terraform` directory. If not, you
    can navigate to the directory by typing `cd terraform`.

-   Second, deploy the infrastructure with the following commands:
    `<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

    ``` bash
    terraform apply -target=module.transform_job || echo "$?"
    ```

    </div>

    **Note**: Remember that the command `terraform apply` will prompt
    you to reply `yes`. Do not type anything other that `yes` or the
    apply command will fail.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.2.6.`</b>`{=html}`</span>`{=html}
You will get some additional outputs, in particular, you require the
following two: `glue_json_transformation_job` and
`glue_songs_transformation_job`. Execute the two glue jobs, based on the
name:

-   `de-c4w4a1-json-transform-job`
-   `de-c4w4a1-songs-transform-job`

You can run those two jobs in parallel. Use the following command in the
terminal:
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

``` bash
aws glue start-job-run --job-name <JOB-NAME> | jq -r '.JobRunId' || echo "$?"
```

</div>

And based on the job run ID track each job status by using this command:

<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">

```bash
aws glue get-job-run --job-name <JOB-NAME> --run-id <JobRunID> --output text --query "JobRun.JobRunState" || echo "$?"
```
</div>

Wait until the jobs statuses change to `SUCCEEDED` (each job should take
around 3 mins).
:::

::: {.cell .markdown}
`<a id='4-3'>`{=html}`</a>`{=html}

### 4.3 - Serving Zone {#43---serving-zone}

For the last layer of your Three-tier Data Lake architecture, you are
going to use AWS Redshift as a Data Warehouse solution. The
transformations will be performed directly inside Redshift, but you need
to make the data available in that storage solution. For that, you will
use Redshift Spectrum, which is a feature that allows you to run queries
against data stored in S3 without having to load the data into Redshift
tables. For that, you are required to use a Glue Catalog which was
already created in the `transform` module.

Follow the instructions below to finish setting up your resources for
the `serving` module.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.3.1.`</b>`{=html}`</span>`{=html}
Open the file located at `terraform/modules/serving/iam.tf`. Go through
the code, **you do not need to change anything**.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.3.2.`</b>`{=html}`</span>`{=html}
Open the file `terraform/modules/serving/redshift.tf`.Go through the
code, **you do not need to change anything**.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.3.3.`</b>`{=html}`</span>`{=html}
Open the `terraform/main.tf` file and go through the lines associated
with the module named `serving` (lines 33 to 50). Also go through the
corresponding lines in the `terraform/outputs.tf` file (lines 37 to 44).
**You do not need to change anything**.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}4.3.4.`</b>`{=html}`</span>`{=html}
In the terminal,

-   First make sure you\'re in the `terraform` directory. If not, then
    navigate to the `terraform` directory by typing `cd terraform`.

-   Second, deploy the infrastructure for the last layer by running the
    command:
    `<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

    ``` bash
    terraform apply -target=module.serving || echo "$?"
    ```

    </div>

    **Note**: Remember that the command `terraform apply` will prompt
    you to reply `yes`. Do not type anything other that `yes` or the
    apply command will fail.

With that, you have deployed the required infrastructure for your
three-tier data lake. The next step consists of modelling the data in
your Redshift Data Warehouse to serve it.
:::

::: {.cell .markdown}
`<a id='5'>`{=html}`</a>`{=html}

## 5 - Data Modeling with dbt and Redshift Spectrum {#5---data-modeling-with-dbt-and-redshift-spectrum}
:::

::: {.cell .markdown}
`<a id='5-1'>`{=html}`</a>`{=html}

### 5.1 - Redshift Setup {#51---redshift-setup}

Before working with DBT to model the data in the transformation layer
into the serving layer, you need to use **Redshift Spectrum** to connect
Redshift with the Iceberg tables. Spectrum allows you to query files
from S3 directly from Redshift, it also has a special feature that
allows us to read directly from Iceberg tables just by creating an
external schema that points to the Glue database containing the tables.
For this initial setup, you will use Terraform to set up the external
schema and a normal schema for the serving layer.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.1.1.`</b>`{=html}`</span>`{=html}
Make sure you\'re in the `terraform` directory and run the serving
module with the following command:
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

``` bash
terraform apply -target=module.serving || echo "$?"
```

</div>

If you\'re not, then navigate to the `terraform` directory by typing
`cd terraform`.
:::

::: {.cell .markdown}
`<a id='5-2'>`{=html}`</a>`{=html}

### 5.2 - Redshift Test {#52---redshift-test}

To verify that the schemas were set up successfully, you will connect to
the target Redshift cluster using the `%sql` magic.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.2.1.`</b>`{=html}`</span>`{=html}
Let\'s start by configuring the credentials, you can obtain the
Redshift\'s cluster endpoint in the CloudFormation stack\'s outputs and
replace the placeholder `<REDSHIFT_ENDPOINT>` with it.

**Note:** If you\'re redoing the lab, make sure to check
**CloudFormation** to get the updated value for `<REDSHIFT_ENDPOINT>`
and update `REDSHIFTDBHOST` in the cell below.
:::

::: {.cell .code}
``` python
REDSHIFTDBHOST = '<REDSHIFT_ENDPOINT>'
REDSHIFTDBPORT = 5439
REDSHIFTDBNAME = 'dev'
REDSHIFTDBUSER = 'defaultuser'
REDSHIFTDBPASSWORD = 'Defaultuserpwrd1234+'

redshift_connection_url = f'postgresql+psycopg2://{REDSHIFTDBUSER}:{REDSHIFTDBPASSWORD}@{REDSHIFTDBHOST}:{REDSHIFTDBPORT}/{REDSHIFTDBNAME}'
%sql {redshift_connection_url}
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.2.2.`</b>`{=html}`</span>`{=html}
Test the connection and the setup, this query will return the list of
available schemas for the `dev` database, and the external schema and
gold layer schema should appear.
:::

::: {.cell .code exercise="[\"ex01\"]" tags="[\"graded\"]"}
``` python
%sql SHOW SCHEMAS FROM DATABASE dev 
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.2.3.`</b>`{=html}`</span>`{=html}
Now, let\'s verify that the Iceberg tables where automatically imported
into the external schema, let\'s query the tables available inside the
external schema.
:::

::: {.cell .code exercise="[\"ex02\"]" tags="[\"graded\"]"}
``` python
%sql SHOW TABLES FROM SCHEMA dev.deftunes_transform
```
:::

::: {.cell .markdown}
Query the Iceberg tables in the external schema to verify that Redshift
can read from them.
:::

::: {.cell .code exercise="[\"ex03\"]" tags="[\"graded\"]"}
``` python
%sql select * from deftunes_transform.songs limit 10
```
:::

::: {.cell .code exercise="[\"ex04\"]" tags="[\"graded\"]"}
``` python
%sql select * from deftunes_transform.sessions limit 10
```
:::

::: {.cell .code exercise="[\"ex05\"]" tags="[\"graded\"]"}
``` python
%sql select * from deftunes_transform.users limit 10
```
:::

::: {.cell .markdown}
`<a id='5-3'>`{=html}`</a>`{=html}

### 5.3 - dbt Setup {#53---dbt-setup}

Now that you have set up the target database in Redshift, you will
create a dbt project that connects to Redshift and allows you to model
the transform layer tables into the final data model in the serving
layer.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.3.1.`</b>`{=html}`</span>`{=html}
Create the new project using the following commands in the terminal.
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 90%;">`{=html}

``` bash
cd ~/project
dbt init dbt_modeling || echo "$?"
```

</div>
:::

::: {.cell .markdown}
After running the command, dbt will ask you the engine to run the
project on:

-   select the option for Redshift.

The CLI will ask you for the connection details,

-   for `host`, use the same endpoint you used before to configure the
    `%sql` magic (step 5.2.1)

Use:

-   for `port` leave empty (click `Enter`),
-   for `user` input `defaultuser`,
-   for `password` input `Defaultuserpwrd1234+`,
-   for `dbname` input `dev`,
-   for `schema` input `deftunes_serving`,
-   for `threads` input `1`.

This is how the process in the terminal should look like:

![dbt setup](images/dbt_config.png)
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.3.2.`</b>`{=html}`</span>`{=html}
To test the connection, run the following commands:
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 50%;">`{=html}

``` bash
cd dbt_modeling
dbt debug
```

</div>
:::

::: {.cell .markdown}
If everything was correctly configured, you should see the following
text at the end of the output:

``` bash
Connection test: [OK connection ok]
```

**Note**: If you had issues defining the connection details, you can use
the `profiles.yml` file in the scripts folder as a guide to define the
connection details, change the placeholder in the file with the Redshift
cluster endpoint and then copy it to the following path
`~/.dbt/profiles.yml` with this command:
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 60%;">`{=html}

``` bash
cp ../scripts/profiles.yml ~/.dbt/profiles.yml 
```

</div>

**You can now submit the lab for grading**.
:::

::: {.cell .markdown}
`<a id='5-4'>`{=html}`</a>`{=html}

### 5.4 - Data Modeling {#54---data-modeling}

**Note**: This section is optional and not graded.

`<span style="font-size:16px">`{=html}`<b>`{=html}5.4.1.`</b>`{=html}`</span>`{=html}
Now that the dbt project has the initial setup, create a new folder
named `serving_layer` in the models folder, this subfolder will contain
the models associated with the star schema.
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 50%;">`{=html}

``` bash
cd models
mkdir serving_layer
```

</div>

`<span style="font-size:16px">`{=html}`<b>`{=html}5.4.2.`</b>`{=html}`</span>`{=html}
In the [`dbt_modeling/dbt_project.yml`](dbt_modeling/dbt_project.yml),
change the `models` block to the following one:

``` yaml
models:
  dbt_modeling:
    serving_layer:
      +materialized: table
```

Save changes to the file.
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.4.3.`</b>`{=html}`</span>`{=html}
Now you can prepare files for data modeling into the star schema. You
will need to identify fact and dimensional tables, then create an SQL
model file for each. Finally, inside the new folder, create a
`schema.yml` file. You can look at the `example` folder if needed.

Once you are done modelling the data, use the following command to run
the models you created (make sure you are in the `dbt_modeling` project
folder):
`<div style="padding: 10px 2px 0px 4px; border-radius: 4px;border: 1px solid #99999955;box-shadow: 4px 4px 6px rgba(52, 51, 51, 0.1);width: 50%;">`{=html}

``` bash
dbt run -s serving_layer || echo "$?"
```

</div>

If all the model runs were successful, you should see an output like
this one, where X is the number of models you created.

``` bash
Completed successfully

Done. PASS=X WARN=0 ERROR=0 SKIP=0 TOTAL=X
```
:::

::: {.cell .markdown}
`<span style="font-size:16px">`{=html}`<b>`{=html}5.4.4.`</b>`{=html}`</span>`{=html}
The final test for your models will be for you to query them using the
`%sql` magic. Run the following query for each table to get a sample and
verify that the model definition was correct (replace the placeholder
`<TABLE_NAME>`).
:::

::: {.cell .code}
``` python
%sql SHOW TABLES FROM SCHEMA dev.deftunes_serving
```
:::

::: {.cell .code}
``` python
%sql SELECT * FROM deftunes_serving.<TABLE_NAME> limit 10
```
:::

::: {.cell .code}
``` python
%sql DROP SCHEMA deftunes_serving CASCADE;
```
:::

::: {.cell .markdown}
During the first part of the capstone, you set up a data architecture
for the new business operation of DeFtunes, you implemented a basic data
pipeline that can be improved later on with an iterative approach. In
the second part of the capstone, you will improve upon the existing data
architecture adding orchestration, data visualization and data quality
checks.
:::
