::: {.cell .markdown}
# Data Modeling with DBT

During this week\'s assignment, you will learn how to model a dataset
based on two multi-dimensional data models such as the Star Schema and
One Big Table (OBT).
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction and Setup](#1)
    -   [1.1 - Initiating dbt 101 Project](#1-1)
    -   [1.2 - Source Configuration](#1-2)
-   [2 - Modeling](#2)
-   [3 - Star Schema](#3)
    -   [3.1 - Description of the Approach](#3-1)
    -   [3.2 - Creating the Facts Table](#3-2)
    -   [3.3 - Creating the Customers Dimension Table](#3-3)
    -   [3.4 - Creating the Employees Dimension Table](#3-4)
    -   [3.5 - Creating the Office Dimension Table](#3-5)
    -   [3.6 - Creating the Product Dimension Table](#3-6)
    -   [3.7 - Creating the Date Dimension Table](#3-7)
    -   [3.8 - Running the Star Schema Model](#3-8)
-   [4 - One Big Table (OBT)](#4)
-   [5 - Performing Tests over the Data in the New Models](#5)
:::

::: {.cell .markdown}
Please, load the required libraries.
:::

::: {.cell .code execution_count="1"}
``` python
from IPython.display import HTML

%load_ext sql
```
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction and Setup {#1---introduction-and-setup}

Data modeling is one of the pillars of Data Engineering, it involves
organizing bits of data into defined models with their respective data
types and relationships between each other. Most of the work in data
modeling involves using predefined techniques or patterns on a raw
dataset based on the business\'s requirements. Data models like the
**Star Schema** and **One Big Table (OBT)** have become popular for
analytical workloads in recent years. In this lab, you will apply these
models to the `classicmodels` dataset.
:::

::: {.cell .markdown}
`<a id='1-1'>`{=html}`</a>`{=html}

### 1.1 - Initiating **dbt 101** Project {#11---initiating-dbt-101-project}

**dbt** is a transformation workflow command line tool based on SQL, it
consists of a compiler and a runner. A user writes `dbt` files and then
can invoke `dbt` to run these files on the data warehouse of their
choice. The compiler converts the `dbt` files into raw SQL scripts and
runs them for you.
:::

::: {.cell .markdown}
Let\'s start a `dbt` project.
:::

::: {.cell .markdown}
1.1.1. Run the following command in the terminal to check that `dbt`
Core is installed.
:::

::: {.cell .markdown}
``` bash
dbt --version
```
:::

::: {.cell .markdown}
1.1.2. Initiate the `classicmodels_modeling` project with the `init`
command:
:::

::: {.cell .markdown}
``` bash
dbt init classicmodels_modeling
```
:::

::: {.cell .markdown}
Select the `postgres` database by pressing `1` and then `Enter` when
prompted to. After that you will be prompted to enter other values, but
you should quit that with `Cmd + C` or `Ctrl + C` as you will configure
the rest of the connection details later. Check that the folder
`classicmodels_modeling` will be created.
:::

::: {.cell .markdown}
1.1.3. Copy the `packages.yml` file to the project folder:
:::

::: {.cell .markdown}
``` bash
cp ./scripts/packages.yml ./classicmodels_modeling/
```
:::

::: {.cell .markdown}
1.1.4. Navigate into your project\'s directory:
:::

::: {.cell .markdown}
``` bash
cd classicmodels_modeling
```
:::

::: {.cell .markdown}
1.1.5. Run the following command from the `classicmodels_modeling`
folder to fetch the latest stable versions of tools and libraries
specified in the `packages.yml` file.
:::

::: {.cell .markdown}
``` bash
dbt deps
```
:::

::: {.cell .markdown}
1.1.6. Open the main configuration file for the project
`./classicmodels_modeling/dbt_project.yml`. Go through the comments in
that file to understand its content.
:::

::: {.cell .markdown}
`<a id='1-2'>`{=html}`</a>`{=html}

### 1.2 - Source Configuration {#12---source-configuration}

When developing with `dbt Core`, `dbt` connects to your data warehouse
using a profile, which is a `YAML` file with all the connection details
to your warehouse. You are going to use a Postgres database.
:::

::: {.cell .markdown}
1.2.1. Run the following code to get the link to the AWS console.

*Note*: For security reasons, the URL to access the AWS console will
expire every 15 minutes, but any AWS resources you created will remain
available for the 2 hour period. If you need to access the console after
15 minutes, please rerun this code cell to obtain a new active link.
:::

:::: {.cell .code execution_count="2"}
``` python
with open('../.aws/aws_console_url', 'r') as file:
    aws_url = file.read().strip()

HTML(f'<a href="{aws_url}" target="_blank">GO TO AWS CONSOLE</a>')
```

::: {.output .execute_result execution_count="2"}
<a href="https://signin.aws.amazon.com/federation?Action=login&SigninToken=utpC1c55BFvLE--kTgZ5zwApfrxuBgk1cSBmheKkqTDa1x49A_P1wbZTUIn1zlZPcXbv831i4eHzL2rRpzvsJYGysHw4c4b2SSrtEpchW9tF4qboCT02u0Xf_vAiXtYMMjG5vEvrTnltIasvsUBfj4NG7DAFw4YDEdTY7yQPbP7couwyXfV2ZLxSVoPFX0Rn_T813YatOfso1Nk4GNTy3xu38A6pPB3-kim8vExhxDKFrSVgeAdJndwJW1zvfnBLobtOGszF5fJydKMr5XrHnbpe0PV4_OsBrFMBf53DnQcPKBsbeJhf6sjwdXiPiJZe1wLgXGYu3jNNecY1-gtlNW3kxADsEIQGKZYQ6IlgADuZgbN26Xh8shGYKDQPiyfWRLcYzuNS1oD7t7a3CCxU9Nv4GGwxN_wNkXL8IsZj61WXfAEYZ-FvRyi5rkWklG_QJN1zRy48_YHZPVuY8OZGXR3PRIwAPPUyFes7X4AW0ZqjFlVIjVUGcKXiJZqcbUVsR9c8A_Qm9LqkAbwWx1sm-zO5kC73JT5bINxFxWRfjyfg3km1mKoMvp-NVCAIPlJSIGrZP2t0IcNFizgSUkAUBLEPYcsbwyq3wvNeqxf2C6Uo7Ppr_UeFWAESlI43TEzqm8CRhLwV0-OSkQGX3ZhsIiMAosTZpGBaQ9SlGKuDx8P7C_Plw5RzzBodpyDkH_sAe9BMuIc8YqQEqKFkaWPnieI5HED2KPy6EYXhXsK9CpHEqp4zVSSuUwYjKJrNcmI-QJRzIbkhTdy7_Hghl72VctzuT0r226t6qZ1jB9ggkoJ-xfFOnqMBlsVV82EyAHHif7J5pKFNGiAQr5dYF37KmP_d94_sQToCW2q_IvPmKhFSXCZHoDtpvcokjgLs6VYYBwI99yogAvmOUpoYunOidL5EoIeU43SbldgSef39fS0qKOnf81FrZ8XaKJzJVpf5BsfRuSlmPcj-K1UomsB-HbebMTjvMX3dtqsI6WTvWvpudCt5FXm5PP0i7gxL1dIZMlZCBetUNPwq4DjdHvw8cnWUCJxKJ4WMA5ySEgGdqtK8u9BixFnXErkUcZanFO5VLnCRhzHOaZJ_9TeXQyW1Ia_94Rchda7pgGuaNxJ86dRn2ER6JodEEnEDHW9qT7tl4wUG42CbyeJvYU5oikQRK6LlrcBVefRxZdAHPMIm_yDdbiuVXS8vHzCi3gmf8Mo5e9n3qW14B_cHctHoVOrNd3yxDZyckvesKI_zTy_3ddxC2O81PVsqaIQ0Ipnn3xrBynj1rQ68aurNoO9vQbsE4ezpi4GRHvdm_nqX7wfyX02aWK6mvFqNHdJ0UsrQLLrJkXeUqYTGraPtw1pAjfTl2KpkJKnlwu7f8_okdu3qV7RkjKjH8UO1ypEMteYzCGWNjArnb36tOFp1wiekzwzOGb9iPm152QIBWN-T4SQMOTfNNpLdAEvZNw&Issuer=https%3A%2F%2Fapi.vocareum.com&Destination=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome%3Fregion%3Dus-east-1" target="_blank">GO TO AWS CONSOLE</a>
:::
::::

::: {.cell .markdown}
*Note:* If you see the window like in the following printscreen, click
on **logout** link, close the window and click on console link again.

![AWSLogout](images/AWSLogout.png)
:::

::: {.cell .markdown}
Go to **CloudFormation** in the AWS console. Click on the alphanumeric
stack name and search for the **Outputs** tab. You will see the key
`PostgresEndpoint`, copy the corresponding **Value**.
:::

::: {.cell .markdown}
1.2.2. Open the file located at `./scripts/profiles.yml`. Replace the
placeholders `<DATABASE_ENDPOINT>` with the endpoint value. Save
changes.
:::

::: {.cell .markdown}
1.2.3. Assuming you are already inside the `classicmodels_modeling`
folder in the terminal, run the following command to copy the
`profiles.yml` file to the invisible folder `.dbt` of the project:
:::

::: {.cell .markdown}
``` bash
cp ../scripts/profiles.yml ~/.dbt/profiles.yml 
```
:::

::: {.cell .markdown}
*Note*: If you received a message saying that there\'s no directory
labelled `.dbt`, make sure to re-run the command in step 1.1.2.
:::

::: {.cell .markdown}
1.2.4. Test the connection with the following command:
:::

::: {.cell .markdown}
``` bash
dbt debug
```
:::

::: {.cell .markdown}
It should return a `Connection test: OK connection ok` at the end of the
output.
:::

::: {.cell .markdown}
1.2.5. Load the connection configuration into the notebook with the
following cell:
:::

::: {.cell .code execution_count="3"}
``` python
import yaml

with open("./scripts/profiles.yml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
    
DBCONFIG = data_loaded["classicmodels_modeling"]["outputs"]["dev"]
DBHOST = DBCONFIG["host"]
DBPORT = int(DBCONFIG["port"])
DBNAME = DBCONFIG["dbname"]
DBUSER = DBCONFIG["user"]
DBPASSWORD = DBCONFIG["password"]
db_connection_url = f'postgresql+psycopg2://{DBUSER}:{DBPASSWORD}@{DBHOST}:{DBPORT}/{DBNAME}'

%sql {db_connection_url}
```
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Modeling {#2---modeling}

Inside the `classicmodels_modeling` project folder, you have the
`models` folder, which contains an example of a model definition. Let\'s
explore it.
:::

::: {.cell .markdown}
2.1. Open `./classicmodels_modeling/models/example/schema.yml` file and
go through its contents. The file contains a version in the header and
then a list of models. For each model, you will need to give a name, a
description and a list of columns. For each column, you can also add a
description and some tests to verify constraints such as a unique value
check and that no value is `NULL`.
:::

::: {.cell .markdown}
2.2. Open `my_first_dbt_model.sql` file from the same folder and have a
look at the SQL code. In the first line of the model\'s script, you
could override the project configuration and personalize the model
configuration. In this case this model will be materialized as a table
instead of a view. After that a model is based on a `SELECT` statement
over the source data, in this particular case it uses a `CTE` to create
some sample data. But you could just write a query that uses the
connector to bring data from the data warehouse.
:::

::: {.cell .markdown}
2.3. Have a look at another example of the model:
:::

::: {.cell .markdown}
``` sql
SELECT 
    {{ dbt_utils.generate_surrogate_key(['employeeNumber']) }} as employee_key, 
    employees.lastName as employee_last_name, 
    employees.firstName as employee_first_name, 
    employees.jobTitle as job_title, 
    employees.email as email
FROM classicmodels.employees
```
:::

::: {.cell .markdown}
This is an example of a model based on the Employees\' table in
`classicmodels`. Here you are using the function
`generate_surrogate_key` from `dbt_utils` to create a surrogate key
based on the natural key `employeeNumber`. This is a good practice in
data modeling because natural keys in your production data might change,
or they could cause issues with your data models. For that reason, it is
better to substitute natural keys and to perform the joins between
dimension tables and fact tables with surrogate keys.
:::

::: {.cell .markdown}
`<a id='2-4'>`{=html}`</a>`{=html} 2.4. Now that you have familiarized
yourself with the examples, create two new subfolders in the `models`
folder inside the project: `star_schema` and `obt`. You can also delete
the `example` subfolder.

``` bash
mkdir -p models/star_schema
mkdir -p models/obt
rm -rf models/example
```
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Star Schema {#3---star-schema}

`<a id='3-1'>`{=html}`</a>`{=html}

### 3.1 - Description of the Approach {#31---description-of-the-approach}

**A star schema** is composed of **fact** tables (containing an
identifier, numerical measures and foreign keys) and dimensional tables.
You are going to expand upon the star schema you used back in Course 1
Week 2 and implement the improved star schema as a `dbt` model in the
project.

Let\'s remember the Entity Relationship Diagram (ERD) for
[`classicmodels`](https://www.mysqltutorial.org/mysql-sample-database.aspx):

![erm](images/erm.png)

Verify the tables are loaded into the source database in Postgres:
:::

::::: {.cell .code execution_count="4"}
``` python
%%sql
SELECT * FROM information_schema.tables 
WHERE table_schema = 'classicmodels'
```

::: {.output .stream .stdout}
     * postgresql+psycopg2://postgresuser:***@de-c4w1a1-rds.c1msykoaetke.us-east-1.rds.amazonaws.com:5432/postgres
    8 rows affected.
:::

::: {.output .execute_result execution_count="4"}
<table>
    <tr>
        <th>table_catalog</th>
        <th>table_schema</th>
        <th>table_name</th>
        <th>table_type</th>
        <th>self_referencing_column_name</th>
        <th>reference_generation</th>
        <th>user_defined_type_catalog</th>
        <th>user_defined_type_schema</th>
        <th>user_defined_type_name</th>
        <th>is_insertable_into</th>
        <th>is_typed</th>
        <th>commit_action</th>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>employees</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>offices</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>customers</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>orderdetails</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>productlines</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>products</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>orders</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
    <tr>
        <td>postgres</td>
        <td>classicmodels</td>
        <td>payments</td>
        <td>BASE TABLE</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>None</td>
        <td>YES</td>
        <td>NO</td>
        <td>None</td>
    </tr>
</table>
:::
:::::

::: {.cell .markdown}
In the pre-lab exercise, you\'ve seen how you can use the four steps
proposed in the book [The Data Warehouse
Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)
by Ralph Kimball and Margy Ross. Feel free to skip this section, but if
you\'d like a review, here are the steps:

-   Select the Business process.
-   Declare the Granularity of your data.
-   Identify the Dimensions Tables.
-   Identify the Facts.
:::

::: {.cell .markdown}
Let\'s briefly describe each of those steps:

**Select the Business process**

The original ERM of the `classicmodels` database is where the company, a
specialist retailer in scale model toys, draws its data from. The most
important business activity for this company is the sales of their
products captured in the orders placed by customers, the business
process you will model.
:::

::: {.cell .markdown}
**Declare the Granularity of your data**

Each order contains some details that appear in the `orderdetails`
table. A customer can place an order for one or more product items. The
`orderNumber` field in the `orderdetails` table together with the
`productCode` links a product to the order; and the granularity of your
model would allow access to product information for each item on that
order.
:::

::: {.cell .markdown}
**Identify the dimensions**

It is important for a business to hold information about its customers,
and the employees who serve the customer, their branches/offices and the
products sold. These data would shed insights into the business's
performance. So, let's create dimensions tables in your start schema
with those aspects:

-   Customers Dimension
-   Employees Dimension
-   Offices Dimension
-   Products Dimension
:::

::: {.cell .markdown}
**Identify the Facts**

For each order placed by a customer, the unit price and quantity of a
product ordered are important as they are used to calculate the sale
total of this single order. Hence, this information from the
`orderdetails` table should be included as facts in the fact table.
:::

::: {.cell .markdown}
Under the considerations stated in the four-step process to develop the
star schema the idea is that you will implement the following model to
your data:

![star_schema](images/star_schema.png)

In this star schema, you have identified the orders table and order
details as the primary tables for modeling, as they contain a
business-critical process that involves **facts** such as transaction
amounts and quantities. The dimensional tables related to the fact table
are customers, products, employees and offices. You can examine and
perform aggregations to the fact table on either or multiples of these
dimensions.

Now, it\'s your turn to implement the proposed data model.
:::

::: {.cell .markdown}
`<a id='3-2'>`{=html}`</a>`{=html}

### 3.2 - Creating the Facts Table {#32---creating-the-facts-table}
:::

::: {.cell .markdown}
3.2.1. Review the query that extracts the data for the fact table.
:::

::::: {.cell .code execution_count="5"}
``` python
%%sql
SELECT 
    orders.orderNumber, orderdetails.orderLineNumber,
    customers.customerNumber AS customer_key, 
    employees.employeeNumber AS employee_key,
    offices.officeCode AS office_key,
    productCode AS product_key, 
    orders.orderDate AS order_date,
    orders.requiredDate AS order_required_date, 
    orders.shippedDate AS order_shipped_date,
    orderdetails.quantityOrdered AS quantity_ordered, 
    orderdetails.priceEach AS product_price
FROM classicmodels.orders
JOIN classicmodels.orderdetails ON orders.orderNumber = orderdetails.orderNumber
JOIN classicmodels.customers ON orders.customerNumber = customers.customerNumber
JOIN classicmodels.employees ON customers.salesRepEmployeeNumber = employees.employeeNumber
JOIN classicmodels.offices ON employees.officeCode = offices.officeCode
LIMIT 5
```

::: {.output .stream .stdout}
     * postgresql+psycopg2://postgresuser:***@de-c4w1a1-rds.c1msykoaetke.us-east-1.rds.amazonaws.com:5432/postgres
    5 rows affected.
:::

::: {.output .execute_result execution_count="5"}
<table>
    <tr>
        <th>ordernumber</th>
        <th>orderlinenumber</th>
        <th>customer_key</th>
        <th>employee_key</th>
        <th>office_key</th>
        <th>product_key</th>
        <th>order_date</th>
        <th>order_required_date</th>
        <th>order_shipped_date</th>
        <th>quantity_ordered</th>
        <th>product_price</th>
    </tr>
    <tr>
        <td>10100</td>
        <td>3</td>
        <td>363</td>
        <td>1216</td>
        <td>2</td>
        <td>S18_1749</td>
        <td>2003-01-06 00:00:00</td>
        <td>2003-01-13 00:00:00</td>
        <td>2003-01-10 00:00:00</td>
        <td>30</td>
        <td>136</td>
    </tr>
    <tr>
        <td>10100</td>
        <td>2</td>
        <td>363</td>
        <td>1216</td>
        <td>2</td>
        <td>S18_2248</td>
        <td>2003-01-06 00:00:00</td>
        <td>2003-01-13 00:00:00</td>
        <td>2003-01-10 00:00:00</td>
        <td>50</td>
        <td>55.09</td>
    </tr>
    <tr>
        <td>10100</td>
        <td>4</td>
        <td>363</td>
        <td>1216</td>
        <td>2</td>
        <td>S18_4409</td>
        <td>2003-01-06 00:00:00</td>
        <td>2003-01-13 00:00:00</td>
        <td>2003-01-10 00:00:00</td>
        <td>22</td>
        <td>75.46</td>
    </tr>
    <tr>
        <td>10100</td>
        <td>1</td>
        <td>363</td>
        <td>1216</td>
        <td>2</td>
        <td>S24_3969</td>
        <td>2003-01-06 00:00:00</td>
        <td>2003-01-13 00:00:00</td>
        <td>2003-01-10 00:00:00</td>
        <td>49</td>
        <td>35.29</td>
    </tr>
    <tr>
        <td>10101</td>
        <td>4</td>
        <td>128</td>
        <td>1504</td>
        <td>7</td>
        <td>S18_2325</td>
        <td>2003-01-09 00:00:00</td>
        <td>2003-01-18 00:00:00</td>
        <td>2003-01-11 00:00:00</td>
        <td>25</td>
        <td>108.06</td>
    </tr>
</table>
:::
:::::

::: {.cell .markdown}
3.2.2. Now you will need to create the `dbt` model for the `fact_orders`
table. Open the `./classicmodels_modeling/dbt_project.yml` file and
paste the following in the new line after `version: '1.0.0'`:
:::

::: {.cell .markdown}
``` yml
vars:
  source_schema: classicmodels
  star_schema: classicmodels_star_schema
  surrogate_key_treat_nulls_as_empty_strings: true
  "dbt_date:time_zone": "America/Los_Angeles"
```
:::

::: {.cell .markdown}
This is for creation of some
[variables](https://docs.getdbt.com/reference/dbt-jinja-functions/var)
that can be used throughout your `dbt` models during compilation.
:::

::: {.cell .markdown}
3.2.3. Find the key `classicmodels_modeling` at the end of the file
(nested under the `models` key). Delete the nested key `example`, which
is the following two lines:
:::

::: {.cell .markdown}
``` yml
    example:
      +materialized: view
```
:::

::: {.cell .markdown}
Then add the following nested key instead:
:::

::: {.cell .markdown}
``` yml
    star_schema:
      +materialized: table
      +schema: star_schema
```
:::

::: {.cell .markdown}
Save changes.
:::

::: {.cell .markdown}
3.2.4. Go to the folder `./classicmodels_modeling/models/star_schema/`
(which was created at step [2.4](#2.4)) and create an SQL file named
`fact_orders.sql`.
:::

::: {.cell .markdown}
3.2.5 Copy the previous query for the fact table (without the `%%sql`
and `LIMIT` clause) and paste it into a new file located at
`fact_orders.sql`. Make the following changes:

-   Replace every appearance of `classicmodels` with
    `{{var("source_schema")}}` (in 5 places). Each table reference in
    your query should now be in the format
    `{{var("source_schema")}}.<TABLE_NAME>`. This will use jinja
    templating to dynamically replace `source_schema` with the actual
    schema name, which is currently `classicmodels`.
-   Replace the default key columns with surrogate keys using the
    `{{ dbt_utils.generate_surrogate_key(['']) }}` function. This
    function accepts an array of column names to generate the surrogate
    keys:
    -   Replace `orders.orderNumber, orderdetails.orderLineNumber` with
        `{{ dbt_utils.generate_surrogate_key(['orders.orderNumber', 'orderdetails.orderLineNumber']) }} as fact_order_key`.
    -   Replace `customers.customerNumber` with
        `{{ dbt_utils.generate_surrogate_key(['customers.customerNumber']) }}`.
    -   Do the same for `employees.employeeNumber`, `offices.officeCode`
        and `productCode`.

Save changes.
:::

::: {.cell .markdown}
3.2.6. Copy the file located at `./scripts/schema.yml` into
`./classicmodels_modeling/models/star_schema/` folder:
:::

::: {.cell .markdown}
``` bash
cp ../scripts/schema.yml ./models/star_schema/schema.yml
```
:::

::: {.cell .markdown}
Open the `schema.yml` file. You will see that it is partially filled for
you with the schema definition for the `fact_orders` and `dim_customers`
tables.

With that, you have created the model for the fact table `fact_orders`.
Before you run the model against the database to create the tables, you
need to create the dimension tables. The process to create the models
for the dimension tables will be similar to what you did.
:::

::: {.cell .markdown}
`<a id='3-3'>`{=html}`</a>`{=html}

### 3.3 - Creating the Customers Dimension Table {#33---creating-the-customers-dimension-table}
:::

::: {.cell .markdown}
3.3.1. Here is the query to create the dimension table `dim_customers`.
The complete output has 122 rows, you will use `LIMIT` to avoid
extracting too many rows in the preview.
:::

::: {.cell .code}
``` python
%%sql
SELECT 
    customerNumber as customer_key, 
    customerName as customer_name,   
    contactLastName as customer_last_name, 
    contactFirstName as customer_first_name, 
    phone as phone, 
    addressLine1 as address_line_1, 
    addressLine2 as address_line_2, 
    postalCode as postal_code, 
    city as city, 
    state as state, 
    country as country,
    creditLimit as credit_limit
FROM classicmodels.customers
LIMIT 5
```
:::

::: {.cell .markdown}
Follow the same process to create this part in the model.
:::

::: {.cell .markdown}
3.3.2. Go to the folder `./classicmodels_modeling/models/star_schema/`
and create an SQL file `dim_customers.sql`.
:::

::: {.cell .markdown}
3.3.3. Copy the previous query without the `%%sql` and `LIMIT` clause,
paste it into the new file `dim_customers.sql`. Make the following
changes in the file:

-   Replace `classicmodels` with `{{var("source_schema")}}` (in 1
    place). The table reference now is in the format
    `{{var("source_schema")}}.<TABLE_NAME>`.
-   Replace `customerNumber` with
    `{{ dbt_utils.generate_surrogate_key(['customerNumber']) }}` to
    generate the surrogate key.

Save changes.
:::

::: {.cell .markdown}
3.3.4. Remember that the `schema.yml` file has been already configured
for the `dim_customers` table, but notice that for this table, you will
find the following:
:::

::: {.cell .markdown}
``` yml
- name: customer_key
    description: The primary key for this table
    data_tests:
      - unique
      - not_null
```
:::

::: {.cell .markdown}
This is the syntax to create tests over your columns. You will use this
later.
:::

::: {.cell .markdown}
`<a id='3-4'>`{=html}`</a>`{=html}

### 3.4 - Creating the Employees Dimension Table {#34---creating-the-employees-dimension-table}
:::

::: {.cell .markdown}
3.4.1. Here is the query to create the dimension table `dim_employees`.
You will use a `LIMIT` to avoid extracting too many rows. The complete
output has 23 rows.
:::

::: {.cell .code}
``` python
%%sql
SELECT
    employeeNumber as employee_key,
    lastName as employee_last_name, 
    firstName as employee_first_name, 
    jobTitle as job_title, 
    email as email
FROM classicmodels.employees
LIMIT 5
```
:::

::: {.cell .markdown}
3.4.2. Go to the folder `./classicmodels_modeling/models/star_schema/`
and create an SQL file `dim_employees.sql`.
:::

::: {.cell .markdown}
3.4.3. Copy the previous query without the `%%sql` and `LIMIT` clause,
and paste it into the new file `dim_employees.sql`. Make the following
changes in the file:

-   Replace `classicmodels` with `{{var("source_schema")}}` (in 1
    place).
-   Replace `employeeNumber` with
    `{{ dbt_utils.generate_surrogate_key(['employeeNumber']) }}` to
    generate the surrogate key.

Save changes.
:::

::: {.cell .markdown}
3.4.4. Open the `schema.yml` file and based on the `dim_customers`
schema, create the schema for the `dim_employees` table. The
`employee_key` should be the primary key for this table, so you can set
the same tests as in `dim_customers`. Make sure to use the appropriate
column names.
:::

::: {.cell .markdown}
`<a id='3-5'>`{=html}`</a>`{=html}

### 3.5 - Creating the Office Dimension Table {#35---creating-the-office-dimension-table}

3.5.1. This is the query to create the dimension table `dim_offices`.
:::

::: {.cell .code}
``` python
%%sql
SELECT 
    officeCode as office_key, 
    postalCode as postal_code, 
    city as city, 
    state as state, 
    country as country, 
    territory as territory
FROM classicmodels.offices
```
:::

::: {.cell .markdown}
3.5.2. Go to the folder `./classicmodels_modeling/models/star_schema/`
and create an SQL file `dim_offices.sql`.
:::

::: {.cell .markdown}
3.5.3. Copy the previous query, paste it into the new file
`dim_offices.sql`. Make the following changes in the file:

-   Replace `classicmodels` with `{{var("source_schema")}}` (in 1
    place).
-   Replace `officeCode` with
    `{{ dbt_utils.generate_surrogate_key(['officeCode']) }}` to generate
    the surrogate key.

Save changes.
:::

::: {.cell .markdown}
3.5.4. Open the `schema.yml` file and based on the `dim_customers`
schema, create the schema for the `dim_offices` table. The `office_key`
should be the primary key for this table, so you can set the same tests
as in `dim_customers`. Make sure to use the appropriate column names.
:::

::: {.cell .markdown}
`<a id='3-6'>`{=html}`</a>`{=html}

### 3.6 - Creating the Product Dimension Table {#36---creating-the-product-dimension-table}

3.6.1. This is the query to create the dimension table `dim_products`.
Use `LIMIT` as the total output has 110 rows.
:::

::: {.cell .code}
``` python
%%sql
SELECT 
    productCode as product_key, 
    productName as product_name, 
    products.productLine as product_line, 
    productScale as product_scale, 
    productVendor as product_vendor,
    productDescription as product_description, 
    textDescription as product_line_description
FROM classicmodels.products
JOIN classicmodels.productlines ON products.productLine=productlines.productLine
LIMIT 3
```
:::

::: {.cell .markdown}
3.6.2. Create an SQL file `dim_products.sql` in the folder
`./classicmodels_modeling/models/star_schema/`.
:::

::: {.cell .markdown}
3.6.3. Copy the previous query, paste it into the new file
`dim_products.sql`. Make the following changes in the file:

-   Replace `classicmodels` with `{{var("source_schema")}}` (in 2
    places).
-   Replace `productCode` with
    `{{ dbt_utils.generate_surrogate_key(['productCode']) }}` to
    generate the surrogate key.

Save changes.
:::

::: {.cell .markdown}
3.6.4. Open the `schema.yml` file and based on the `dim_customers`
schema, create the schema for the `dim_products` table. The
`product_key` should be the primary key for this table, so you can set
the same tests as in `dim_customers`. Make sure to use the appropriate
column names.
:::

::: {.cell .markdown}
`<a id='3-7'>`{=html}`</a>`{=html}

### 3.7 - Creating the Date Dimension Table {#37---creating-the-date-dimension-table}

As you may know, time is one of the most important dimensions in star
schemas, for this case, you will limit the time dimension to the dates
that appear in the `orders` table. Generating this dimension can be
cumbersome, so you are going to make use of the `dbt_date` package to
generate the date dimension.
:::

::: {.cell .markdown}
3.7.1. Create a `dates.sql` model file in the
`./classicmodels_modeling/models/star_schema/` folder.
:::

::: {.cell .markdown}
3.7.2. Inside of it, add the line to call the `get_date_dimension`
function from the `dbt_date` package. This function takes an initial and
final date, for `classicmodels` the dates are between the start of 2003
and the end of 2005. Here is the format of the function call:
:::

::: {.cell .markdown}
``` sql
{{ dbt_date.get_date_dimension("YYYY-MM-DD", "YYYY-MM-DD") }}
```
:::

::: {.cell .markdown}
3.7.3. Create a `dim_dates.sql` model file in the
`./classicmodels_modeling/models/star_schema/` folder.
:::

::: {.cell .markdown}
3.7.4. In `dim_dates.sql` write the following SQL query to select
required columns from the `date_dimension` model:
:::

::: {.cell .markdown}
``` sql
SELECT
    date_day,
    day_of_week,
    day_of_month,
    day_of_year,
    week_of_year,
    month_of_year,
    month_name,
    quarter_of_year,
    year_number
FROM
    date_dimension d
```
:::

::: {.cell .markdown}
To access the `date` model, add a CTE statement prior to that `SELECT`
statement (at the start of the file):
:::

::: {.cell .markdown}
``` sql
with date_dimension as (
    select * from {{ ref('dates') }}
)
```
:::

::: {.cell .markdown}
Save changes to the file.
:::

::: {.cell .markdown}
3.7.5. Open the `schema.yml` file and add the following schema for the
`dim_dates` table:
:::

::: {.cell .markdown}
``` yml
  - name: dim_dates
    columns:
      - name: date_day
        description: The primary key for this table
        data_tests:
          - unique
          - not_null
      - name: day_of_week
      - name: day_of_month
      - name: day_of_year
      - name: week_of_year
      - name: month_of_year
      - name: month_name
      - name: quarter_of_year
      - name: year_number
```
:::

::: {.cell .markdown}
`<a id='3-8'>`{=html}`</a>`{=html}

### 3.8 - Running the Star Schema Model {#38---running-the-star-schema-model}

Once you have created all the models for your star schema, it is time to
run `dbt` against your database to create the proposed star schema.
:::

::: {.cell .markdown}
3.8.1. In the terminal, make sure to set the `dbt` project folder as
your working directory:
:::

::: {.cell .markdown}
``` bash
cd /home/coder/project/classicmodels_modeling
```
:::

::: {.cell .markdown}
3.8.2. Once you are in the `~/project/classicmodels_modeling` folder in
the terminal, then run `dbt` with the following command:
:::

::: {.cell .markdown}
``` bash
dbt run -s star_schema
```
:::

::: {.cell .markdown}
This should run your models and perform the creation and population of
the tables in a new database named `classicmodels_star_schema` that
resides in the same RDS server. Given that you are going to create
several models with `dbt`, the `-s` (or `--select`) option allows you to
select the particular data model that you want to run.
:::

::: {.cell .markdown}
3.8.7. Now, it is time to check if the tables were created and
populated. Run the next cell to change the connection to the
`classicmodels_star_schema` database:
:::

::: {.cell .code}
``` python
%%sql
SELECT * FROM information_schema.tables 
WHERE table_schema = 'classicmodels_star_schema'
```
:::

::: {.cell .markdown}
And count the number of rows in each table to verify that they were
populated:
:::

::: {.cell .code exercise="[\"ex01\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.fact_orders;
```
:::

::: {.cell .markdown}
##### **Expected Output**

  **count**
  -----------
  2996
:::

::: {.cell .code exercise="[\"ex02\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.dim_customers;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  122
:::

::: {.cell .code exercise="[\"ex03\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.dim_employees;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  23
:::

::: {.cell .code exercise="[\"ex04\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.dim_offices;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  7
:::

::: {.cell .code exercise="[\"ex05\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.dim_products;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  110
:::

::: {.cell .code exercise="[\"ex06\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_star_schema.dim_dates;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  1095
:::

::: {.cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - One Big Table (OBT) {#4---one-big-table-obt}

As the name suggests, it means a large table containing all the relevant
data needed for analysis. It is similar to a fact table, but instead of
using dimensional tables and foreign keys, it contains the required
dimensional values for each row within. This approach ensures the data
warehouse doesn\'t have to perform any joins to query the relevant data
each time you need it. Here is an example of an OBT table focused on the
orders of `classicmodels`:

![image](images/obt.png)
:::

::: {.cell .markdown}
4.1. Create the file `orders_obt.sql` in the
`./classicmodels_modeling/models/obt/` folder. Here is the SQL query to
which you need to apply jinja templating like you did in [Section
3](#3). No need to create any surrogate keys there.
:::

::: {.cell .code}
``` python
%%sql
SELECT
    orderdetails.orderNumber as order_number,
    orderdetails.orderLineNumber as order_line_number,
    products.productName as product_name,
    products.productScale as product_scale,
    products.productVendor as product_vendor,
    products.productDescription as product_description,
    products.buyPrice as product_buy_price,
    products.MSRP as product_msrp,
    productlines.textDescription as product_line,
    orderdetails.quantityOrdered as quantity_ordered,
    orderdetails.priceEach as product_price,
    orders.orderDate as order_date,
    orders.requiredDate as order_required_date,
    orders.shippedDate as order_shipped_date,
    customers.customerName as customer_name,
    customers.city as customer_city,
    customers.state as customer_state,
    customers.postalCode as customer_postal_code,
    customers.creditLimit as customer_credit_limit,
    employees.firstName as sales_rep_first_name,
    employees.lastName as sales_rep_last_name,
    employees.jobTitle as sales_rep_title,
    orders.status as order_status,
    orders.comments as order_comments
FROM classicmodels.orderdetails
JOIN classicmodels.orders ON orderdetails.orderNumber =  orders.orderNumber
JOIN classicmodels.products ON orderdetails.productCode =  products.productCode
JOIN classicmodels.productlines ON products.productLine =  productlines.productLine
JOIN classicmodels.customers ON orders.customerNumber =  customers.customerNumber
JOIN classicmodels.employees ON customers.salesRepEmployeeNumber =  employees.employeeNumber
LIMIT 2
```
:::

::: {.cell .markdown}
4.2. Create the `schema.yml` file in the
`./classicmodels_modeling/models/obt/` folder and add the following
schema for the `orders_obt` table:

``` yaml
version: 2

models:
  - name: orders_obt
    description: "Orders OBT"
    columns:
      - name: order_number
        description: Part of the primary key for this table
      - name: order_line_number
        description: Part of the primary key for this table
      - name: product_name
      - name: product_scale
      - name: product_vendor
      - name: product_description
      - name: product_buy_price
      - name: product_msrp
      - name: product_line
      - name: quantity_ordered
      - name: product_price
      - name: order_date
      - name: order_required_date
      - name: order_shipped_date
      - name: customer_name
      - name: customer_city
      - name: customer_state
      - name: customer_postal_code
      - name: customer_credit_limit
      - name: sales_rep_first_name
      - name: sales_rep_title
      - name: order_status
      - name: order_comments
```
:::

::: {.cell .markdown}
For now, you don\'t need to add the tests for the primary key, just make
sure to list the names of the columns in the schema file. You will add
the required test in the next section.
:::

::: {.cell .markdown}
4.3. Open the `./classicmodels_modeling/dbt_project.yml` file, at the
end of it, under the `classicmodels_modeling` key (which is nested
inside the `models` key), add the following lines:

``` yml
    obt:
      +materialized: table
      +schema: obt
```

Save changes.
:::

::: {.cell .markdown}
4.4. Make sure you are in the `~/project/classicmodels_modeling` folder
in the terminal. Run the following command:
:::

::: {.cell .markdown}
``` shell
dbt run --select "obt"
```
:::

::: {.cell .markdown}
4.5. Once you run the dbt run statement, verify that the tables exist
and do a record count for each table:
:::

::: {.cell .code}
``` python
%%sql
SELECT * FROM information_schema.tables 
WHERE table_schema = 'classicmodels_obt'
```
:::

::: {.cell .code exercise="[\"ex07\"]" tags="[\"graded\"]"}
``` python
%sql SELECT count(*) FROM classicmodels_obt.orders_obt;
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **count**
  -----------
  2996
:::

::: {.cell .markdown}
`<a id='5'>`{=html}`</a>`{=html}

## 5 - Performing Tests over the Data in the New Models {#5---performing-tests-over-the-data-in-the-new-models}

You can perform some tests over the data that was populated in your new
star schema model. In the `schema.yml` file you have the definition for
each model and each column. You can place certain tests at the column
level or at the table level. As an example of tests at the table level,
you could define the following one for the `orders_obt` table:
:::

::: {.cell .markdown}
``` yml
- name: orders_obt
  description: "Orders OBT"
  columns:
    ...
  data_tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns:
        - order_number
        - order_line_number
```
:::

::: {.cell .markdown}
*Note*: The indentation in `.yml` files is crucial for the correct
interpretation of the file. Ensure that the indentation levels are
consistent to avoid errors in processing the `YAML` file.
:::

::: {.cell .markdown}
This test verifies that the combination of columns that form the primary
key is unique. Note that this primary key is composed by a combination
of 2 columns. To check for the primary key on other tables that are non
composed keys, you can define the test at the column level, such as:
:::

::: {.cell .markdown}
``` yml
- name: dim_customers
  description: "Customer dimension"
  columns:
    - name: customer_key
      description: The primary key for this table
      data_tests:
        - unique
        - not_null
```
:::

::: {.cell .markdown}
Add those tests to your model and run the `dbt test` command to check
them:
:::

::: {.cell .markdown}
``` bash
dbt test -s obt
```
:::

::: {.cell .markdown}
In this lab, you have learned about `dbt` and further your experience
with data modeling, each data model has its advantages and setbacks and
should be used based on the business and analytical requirements. OBT
performs faster in terms of data retrieval speed when compared against a
star schema, however updating OBT could be much more complex and a star
schema is better in terms of conceptualizing and sharing your data while
requiring less storage space.
:::
