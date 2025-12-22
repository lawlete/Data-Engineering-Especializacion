::: {.cell .markdown}
# Week 3 Lab 1: Comparing the Query Performance Between Row-Oriented and Column-Oriented Databases

In this lab, you will explore the performance differences between
row-based and column-based databases by performing analytical queries,
as well as update and delete queries, on both types of storage and
comparing their execution times. Understanding the differences between
these storage options will help you make informed decisions tailored to
your project query needs as well as performance, scalability and cost
requirements.

To open the solution notebook, follow these steps:

-   Go to the main menu and select `File -> Preferences -> Settings`.
-   Click on `Text Editor` on the left, then scroll down to the
    `Files: Exclude` section.
-   Remove the line
    `**/C3_W3_Lab_1_Row_vs_Column_Storage_Solution.ipynb`. The file will
    now appear in the explorer.
-   You can close the `Settings` tab.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction and Lab Setup](#1)
    -   [1.1 - Introduction](#1-1)
    -   [1.2 - Cloud Data Warehouse Benchmark](#1-2)
    -   [1.3 - Initial Imports](#1-3)
    -   [1.4 - Setting up the Databases and Loading the Data](#1-4)
-   [2 - Column-Based Database: Amazon Redshift](#2)
    -   [2.1 - Connecting to the Database](#2-1)
    -   [2.2 - Creating Queries](#2-2)
        -   [Exercise 1](#ex01)
        -   [Exercise 2](#ex02)
    -   [2.3 - Running the Benchmark Analytical Queries](#2-3)
        -   [First TPC-H Query](#2-3-1)
        -   [Second TPC-H Query - Optional](#2-3-2)
        -   [Third TPC-H Query - Optional](#2-3-3)
        -   [Fourth TPC-H Query - Optional](#2-3-4)
        -   [Fifth TPC-H Query - Optional](#2-3-5)
    -   [2.4 - Running the Insert and Delete Queries](#2-4)
        -   [Exercise 3](#ex03)
-   [3 - Row-Based Database: PostgreSQL](#3)
    -   [3.1 - Connecting to the Database](#3-1)
    -   [3.2 - Executing the Initial Queries](#3-2)
    -   [3.3 - Running the Benchmark Analytical Queries](#3-3)
        -   [First TPC-H Query](#3-3-1)
        -   [Second TPC-H Query - Optional](#3-3-2)
        -   [Third TPC-H Query - Optional](#3-3-3)
        -   [Fourth TPC-H Query - Optional](#3-3-4)
        -   [Fifth TPC-H Query - Optional](#3-3-5)
    -   [3.4 - Running the Insert and Delete Queries](#3-4)
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction and Lab Setup {#1---introduction-and-lab-setup}
:::

::: {.cell .markdown}
`<a id='1-1'>`{=html}`</a>`{=html}

### 1.1 - Introduction {#11---introduction}

In traditional transactional databases, the records are stored in rows
which makes the databases optimized for reading and writing rows
efficiently. Recently, there has been a shift towards column-oriented
databases that are optimized for analytical workloads, as they are more
efficient with the aggregating operations.

In this lab, you are provided with an Amazon Redshift database that
leverages columnar storage, and an Amazon RDS (Relational Database
Service) PostgreSQL data that leverages row-oriented storage. To assess
the execution time of the analytical queries, you will work with a
benchmarking dataset and you will run 5 analytical queries to query the
data from each store. The provided data and the corresponding SQL
queries are referred to as the TPC-H benchmark. You will also write 50
rows to one table of the provided database and compare the execution
time of the write query for both databases. Then you\'ll delete these
rows from both databases and again compare the execution time of the
delete query.
:::

::: {.cell .markdown}
`<a id='1-2'>`{=html}`</a>`{=html}

### 1.2 - Cloud Data Warehouse Benchmark {#12---cloud-data-warehouse-benchmark}

You will be using [The Cloud Data Warehouse
benchmark](https://github.com/awslabs/amazon-redshift-utils/tree/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCH),
which is derived from the TPC-H Benchmark. TPC-H is a decision-support
benchmark established by the Transaction Processing Performance Council
(TPC) to simulate a set of basic scenarios to examine a large dataset
and execute queries to answer business questions. It is designed to
evaluate the performance of various database systems in how they execute
complex queries. The Cloud Data Warehouse benchmark is composed of 22
queries and a database at different data scales that help simulate
different analytical needs and aggregation tasks across tables. For this
lab, you will be using the 10GB version of the data set, which is the
smaller data scale of this benchmark. This is the entity relationship
diagram of the database:

![image](./images/erd.png)

*Note*: The TPC Benchmark and TPC-H are trademarks of the Transaction
Processing Performance Council (<http://www.tpc.org>).
:::

::: {.cell .markdown}
`<a id='1-3'>`{=html}`</a>`{=html}

### 1.3 - Initial Imports {#13---initial-imports}

First, let\'s import the required packages.
:::

::: {.cell .code}
``` python
import os 
import random
import time
import uuid
import sqlparse
import pandas as pd

from datetime import datetime, timedelta

from dotenv import load_dotenv
from faker import Faker
from IPython.display import HTML
```
:::

::: {.cell .markdown}
To interact with the database through a Jupyter Notebook, you will use
again the magic command `%sql`. For that, run the following `load_ext`
magic to load the `ipython-sql` extension.
:::

::: {.cell .code}
``` python
# Loading the SQL module
%load_ext sql
```
:::

::: {.cell .markdown}
For each query you will test, you will need to format it using the
following function: `format_query`. This function will simply add a
comment with a unique random identifier for the query to be executed.
This helps avoid caching results in the database and ensures getting the
execution time of the query. Make sure to run the following cell to
define the function.
:::

::: {.cell .code}
``` python
def format_query(query: str='', path: str =None) -> str:
    """Takes a query or a .sql file and adds 
    a comment with a random ID to avoid DB caching

    Arguments:
        query (str): SQL query
    
        path (str): Path to .sql file with one query

    Returns:
        str: Formatted query with comment
    
    """
    raw_uuid = str(uuid.uuid4()).replace('-', '')
    query_uuid = f'view{raw_uuid}' 
    
    if path:
        with open(path, 'r') as file:
            sql_commands = sqlparse.split(file.read())
            query = sql_commands[0]
    
    query = query.replace(';', '')
    sql_command = f"/* Query ID {query_uuid} */{query};"    
    
    return sql_command
```
:::

::: {.cell .markdown}
To get the execution time of the queries, you will be using the
`%%timeit` magic command when running queries. This command provides the
execution time of the query without returning any data, and can have two
parameters:

-   `-n<N>`: Executes the given statement `<N>` times in a loop.
-   `-r<R>`: Number of repeats `<R>`, each consisting of `<N>` loops,
    and returns the average execution time.

You can use these parameters to execute each query several times so you
can get more reliable estimates for the execution times. However for
time and cost constraints, you will be asked to run the queries once
(`-n1` `-r1`). If you want to know more about the `timeit` package, you
can take a look at the
[documentation](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-timeit).

**Disclaimer:** For Redshift you will only execute the queries one time.
Abstain from increasing the value of the parameters `-r` and `-n` to
avoid waiting for extra time or incurring extra costs.
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Column-Based Database: Amazon Redshift {#2---column-based-database-amazon-redshift}

Amazon Redshift is a fully managed data warehousing service. It is
designed for large-scale data analytics and allows users to analyze
their data using standard SQL queries. Redshift is based on a columnar
storage architecture. This means that values from the same column are
stored together on disk, allowing for efficient data compression and
retrieval. It\'s particularly beneficial for analytics workloads where
queries often access only a subset of columns.

Columnar databases excel at aggregation operations (e.g., SUM, AVG,
COUNT) because they only need to access the columns involved in the
aggregation, rather than entire rows. This can significantly improve the
performance of analytical queries, such as those commonly used in data
warehousing environments.

Amazon Redshift\'s columnar architecture makes it well-suited for
analytical workloads, data warehousing, business intelligence, and
ad-hoc querying, offering high performance, scalability, and
cost-effectiveness when processing large volumes of data.

Both storage solutions are already set up, with connection details
available in the CloudFormation stack\'s output. The Amazon Redshift
database is pre-filled with the benchmark data. You\'ll begin the lab by
connecting to the Redshift cluster.
:::

::: {.cell .markdown}
`<a id='2-1'>`{=html}`</a>`{=html}

### 2.1 - Connecting to the Database {#21---connecting-to-the-database}
:::

::: {.cell .markdown}
Run the following code to get the link to the AWS console.

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
Let\'s load some environment variables that contain the credentials to
connect to the database:

-   Go to **CloudFormation** in the AWS console. Click on the
    alphanumeric stack name and search for the **Outputs** tab.
-   Copy the endpoints of the PostgreSQL and redshift databases that you
    can find under the **value** column. Paste the endpoints in the file
    `./src/env` and save the changes.
-   Run the following cell:
:::

::: {.cell .code}
``` python
load_dotenv('./src/env', override=True)

REDSHIFTDBHOST = os.getenv('REDSHIFTDBHOST')
REDSHIFTDBPORT = int(os.getenv('REDSHIFTDBPORT'))
REDSHIFTDBNAME = os.getenv('REDSHIFTDBNAME')
REDSHIFTDBUSER = os.getenv('REDSHIFTDBUSER')
REDSHIFTDBPASSWORD = os.getenv('REDSHIFTDBPASSWORD')
```
:::

::: {.cell .markdown}
Run the following cell to create the connection string:
:::

::: {.cell .code}
``` python
redshift_connection_url = f'redshift+psycopg2://{REDSHIFTDBUSER}:{REDSHIFTDBPASSWORD}@{REDSHIFTDBHOST}:{REDSHIFTDBPORT}/{REDSHIFTDBNAME}'
print(redshift_connection_url)
```
:::

::: {.cell .markdown}
Connect to the Redshift cluster by running the following cell:
:::

::: {.cell .code}
``` python
%sql {redshift_connection_url}
```
:::

::: {.cell .markdown}
Execute the following cell to disable caching results for this session
in Redshift:
:::

::: {.cell .code}
``` python
%sql SET enable_result_cache_for_session TO off;
```
:::

::: {.cell .markdown}
Now that you have the connection to the Redshift database established,
run the following two queries to explore the tables and schema of each
table in the database that you will use.
:::

::: {.cell .code}
``` python
%%sql
SELECT  distinct tablename
FROM PG_TABLE_DEF
WHERE schemaname='public'
;
```
:::

::: {.cell .code}
``` python
%%sql
SELECT  *
FROM PG_TABLE_DEF
WHERE schemaname='public'
AND tablename='lineitem'
;
```
:::

::: {.cell .markdown}
`<a id='2-2'>`{=html}`</a>`{=html}

### 2.2 - Creating Queries {#22---creating-queries}

Before you test the execution times of the TPC-H benchmarking queries,
you are going to issue two queries to the Redshift database. We
encourage you to first explore the dataset and then develop the query to
solve the following question.
:::

::: {.cell .markdown}
`<a id='ex01'>`{=html}`</a>`{=html}

### Exercise 1

Retrieve the order number, part key number, part name, customer key,
order status, order date, country name and region name of the orders
with the following keys: 1552449, 13620130 and 45619461.

You are provided with an initial template for your query.

*Note*: To see the results of the query, you will need to comment the
first line that contains the magic command `%%timeit`. Then to see the
execution time of the query, uncomment the magic command.

Follow the instructions to complete the query:

-   In the CTE, add to the list of the `IN` operator, the keys of the
    orders you need to inspect (1552449, 13620130 and 45619461).
-   In the query expression, complete the `SELECT` statement with the
    necessary columns only:
    -   `l_orderkey`, `l_partkey` from the `lineitemorders` table,
    -   `p_name` from the `part` table,
    -   `c_custkey` from the `customer` table,
    -   `o_orderstatus`, `o_orderdate` from the `orders` table,
    -   `n_name` from the `nation` table,
    -   `r_name` from the `region` table.
-   You need to perform 5 joins, based on the `lineitemorders` CTE:
    -   Join the `part` table on the `p_partkey` column and the
        `l_partkey` column from the `lineitemorders` table.
    -   Join the `orders` table on the `l_orderkey` column and the
        `o_orderkey` column from the `orders` table.
    -   Join the `customer` table on the `c_custkey` column and the
        `o_custkey` column from the `orders` table.
    -   Join the`nation` table on the `n_nationkey` column and the
        `c_nationkey` column from the `customer` table
    -   Join the `region` table on the `r_regionkey` column and the
        `n_regionkey` column from the `nation` table.
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1
raw_sql_statement = """    
    WITH lineitemorders AS (
        SELECT *
    FROM public.lineitem
    WHERE l_orderkey in (1552449, 13620130, 45619461)
    )
    
    SELECT DISTINCT lio.l_orderkey, lio.l_partkey, pt.p_name, ctr.c_custkey, ord.o_orderstatus, ord.o_orderdate, ntn.n_name, rgn.r_name
    FROM lineitemorders lio
    JOIN part pt ON pt.p_partkey = lio.l_partkey
    JOIN orders ord ON lio.l_orderkey = ord.o_orderkey
    JOIN customer ctr ON ctr.c_custkey = ord.o_custkey
    JOIN nation ntn ON ntn.n_nationkey = ctr.c_nationkey 
    JOIN region rgn ON rgn.r_regionkey = ntn.n_regionkey
    ;
"""

sql_statement = format_query(query=raw_sql_statement)
query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
##### **Expected Output**

As a guide, your result should show `15 rows affected` which means that
15 rows have been selected in this case. Here is an example of the
output.

*Note*: Not all of the records are shown.

  -----------------------------------------------------------------------------------------------------------------------------------
  **l_orderkey**   **l_partkey**   **p_name**         **c_custkey**   **o_orderstatus**   **o_orderdate**   **n_name**   **r_name**
  ---------------- --------------- ------------------ --------------- ------------------- ----------------- ------------ ------------
  13620130         733733          snow rose salmon   12461           F                   1993-03-25        ETHIOPIA     AFRICA
                                   azure saddle                                                                          

  45619461         90452           bisque orange      935563          O                   1995-09-07        UNITED       EUROPE
                                   black chiffon                                                            KINGDOM      
                                   orchid                                                                                

  45619461         110840          grey saddle        935563          O                   1995-09-07        UNITED       EUROPE
                                   firebrick tan cyan                                                       KINGDOM      
  -----------------------------------------------------------------------------------------------------------------------------------
:::

::: {.cell .markdown}
`<a id='ex02'>`{=html}`</a>`{=html}

### Exercise 2

In this exercise, you will create a query that is more analytical: how
many customers from the Middle East have a balance that is larger than
the average balance of all customers over the same region?

-   Create a CTE named `avg_balance_middle_east`. In that CTE you will
    need to compute the average (`AVG()` function) of the customer
    account balance (`c_acctbal`) from the `customer` table.
    -   Join the `nation` table on the `n_nationkey` column and the
        `c_nationkey` column from the `customer` table.
    -   Join the `region` table on the `r_regionkey` column and the
        `n_regionkey` column from the `nation` table.
    -   Filter by the region name (`region.r_name`) `'MIDDLE EAST'`.
-   In the query expression, do the following:
    -   From the `customer` table, `SELECT COUNT()` the number of
        `DISTINCT` customers (`customer.c_custkey`).
    -   Join again the `nation` and `region` tables on the same columns.
    -   Add two filter expressions:
        -   the first filter is to specify the region name as
            `'MIDDLE EAST'`;
        -   the second condition is to filter the customers that have a
            customer account balance (`customer.c_acctbal`) greater than
            the result that you computed in the
            `avg_balance_middle_east` CTE, so you will have to add a
            subquery to extract that value of `avg_balance` from the CTE
            in the filter.

*Note*: To see the results of the query, you will need to comment the
first line that contains the magic command `%%timeit`. Then uncomment
the magic command and run the cell to see the execution time.
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1
raw_sql_statement = """    
    WITH avg_balance_middle_east AS (
        SELECT AVG(c_acctbal) AS avg_balance
        FROM customer
        JOIN nation ON customer.c_nationkey = nation.n_nationkey
        JOIN region ON nation.n_regionkey = region.r_regionkey
        WHERE region.r_name = 'MIDDLE EAST'
    )
    SELECT COUNT(DISTINCT customer.c_custkey)
    FROM customer
    JOIN nation ON customer.c_nationkey = nation.n_nationkey
    JOIN region ON nation.n_regionkey = region.r_regionkey
    WHERE region.r_name = 'MIDDLE EAST'
      AND customer.c_acctbal > (SELECT avg_balance FROM avg_balance_middle_east);
"""

sql_statement = format_query(query=raw_sql_statement)
query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
`<a id='2-3'>`{=html}`</a>`{=html}

### 2.3 - Running the Benchmark Analytical Queries {#23---running-the-benchmark-analytical-queries}

As mentioned earlier, in this lab you are using a database from the
Cloud DW benchmark which is derived from TPC-H benchmark. In this part
of the lab, you will execute some of the complex queries that have been
developed under this benchmark. You are provided with 5 of those queries
in the `./sql` folder, each query is defined in a different file. You
will test the first query and the remaining four queries are optional.
:::

::: {.cell .markdown}
`<a id='2-3-1'>`{=html}`</a>`{=html}

### First TPC-H Query

The first analytical query that you are going to test measures the
percentage of revenue (`promo_revenue`) from promotional products for
orders shipped within a month. You can open the file
`./sql/pg_query_0_tcp_h_q14.sql` to have a look at the query.

Execute the following cell to run the query: it will read the file,
store the query as a string and execute the benchmark with the read
query.

Remember that if you want to see the actual data retrieved from this
query, you should create a new cell and copy the query without the
`%timeit` magic command so you will not lose any information about the
execution times.
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_0_tcp_h_q14.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
`<a id='2-3-2'>`{=html}`</a>`{=html}

### Second TPC-H Query - Optional

The second query is in the file `./sql/pg_query_1_tcp_h_q6.sql` and it
is known as the \"Forecasting Revenue Change Query\". It computes the
total revenue lost due to discounts on some line items over the year
1994.

Execute the following cell to run the second query:
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_1_tcp_h_q6.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
`<a id='2-3-3'>`{=html}`</a>`{=html}

### Third TPC-H Query - Optional

The third query finds the top 100 orders with the highest total price
where the total quantity of items is greater than 300. You can find it
in the file `./sql/pg_query_2_tcp_h_q18.sql`.

Run the following cell to execute the third query:
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_2_tcp_h_q18.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
`<a id='2-3-4'>`{=html}`</a>`{=html}

### Fourth TPC-H Query - Optional

The fourth query from the TPC-H benchmark identifies the supplier with
the highest total revenue over three months. The query is in the file
`./sql/pg_query_3_tcp_h_q15.sql`.

Let\'s execute it:
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_3_tcp_h_q15.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
`<a id='2-3-5'>`{=html}`</a>`{=html}

### Fifth TPC-H Query - Optional

The last TPC-H benchmark query summarizes and aggregates the line item
data over some time. Have a look at the content of the file
`./sql/pg_query_4_tcp_h_q1.sql`.

Let\'s execute the last TPC-H query:
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_4_tcp_h_q1.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
Record the time that each query took as you will compare it later with
the PostgreSQL (row) database. You can see that all those queries are
quite complicated and try to solve complex business questions. When you
compare with the row database, you will realize how a columnar database
is more efficient at handling analytical queries than a row database.
:::

::: {.cell .markdown}
`<a id='2-4'>`{=html}`</a>`{=html}

### 2.4 - Running the Insert and Delete Queries {#24---running-the-insert-and-delete-queries}

Now, you will insert and delete some data into the `lineitem` table.
Remember to always execute your queries with the magic command `%timeit`
to benchmark the time it took to perform the operations.
:::

::: {.cell .markdown}
`<a id='ex03'>`{=html}`</a>`{=html}

### Exercise 3

You are provided with code that creates two files:
`individual_row_inserts.sql` and `individual_row_deletes.sql`. The
`individual_row_inserts.sql` contains individual `INSERT` statements for
each row that you will insert, while the `individual_row_deletes.sql`
file contains the corresponding `DELETE` operations for each row that is
on the first file.

Follow the instructions to complete the code. You will use the
[Faker](https://faker.readthedocs.io/en/master/) library to generate
some mock data for your inserts.

In this code, the `leap` variable defines how many records will be
created. By default, you will generate 50 new rows. After completion of
the code review the `insert_statement` and `delete_statement`.
:::

::: {.cell .code}
``` python
# Set a random seed for reproducibility
random.seed(42)

# Generate fake data
fake = Faker()

# Define the range for l_orderkey
leap = 50
start_orderkey = 70000000
end_orderkey = start_orderkey + leap

# Generate SQL queries and write them to .sql files
with open("./sql/individual_row_inserts.sql", "w") as insert_file, open("./sql/individual_row_deletes.sql", "w") as delete_file:
    for orderkey in range(start_orderkey, end_orderkey):
        
        ### START CODE HERE ### (~ 15 lines of code)
        
        # Use the `random_int` method from the `fake` object to generate random integers
        # Set a range between 1 and 1000000
        partkey = fake.random_int(1, 1000000) # @REPLACE EQUALS None.None(None, None)
        suppkey = fake.random_int(1, 10000) # @KEEP
        
        
        # Use the `random_int` method from the `fake` object to generate random integers
        # Set a range between 1 and 10
        linenumber = fake.random_int(1, 10) # @REPLACE EQUALS None.None(None, None)
        
        quantity = round(fake.random_number(2), 2) # @KEEP
        
        extendedprice = round(fake.random_number(4), 2) # @KEEP
        
        discount = round(fake.random_number(2), 2) # @KEEP
        
        tax = round(fake.random_number(2), 2) # @KEEP
        
        returnflag = fake.random_element(elements=('N', 'R', 'A')) # @KEEP
        
        linestatus = fake.random_element(elements=('O', 'F')) # @KEEP
        
        
        # Use the `date_between` method from the `fake` object
        # Use start date as 1 year ago  as '-1y' and `today` as end date
        # Chain with the `strftime` method to return a string with the format '%Y-%m-%d'
        shipdate = fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') # @REPLACE EQUALS None.None(start_date='None', end_date='None').None('None')
        commitdate = (datetime.strptime(shipdate, '%Y-%m-%d') + timedelta(days=fake.random_int(1, 30))).strftime('%Y-%m-%d') # @KEEP
        
        receiptdate = (datetime.strptime(commitdate, '%Y-%m-%d') + timedelta(days=fake.random_int(1, 30))).strftime('%Y-%m-%d') # @KEEP
        
        
        # Use the `text` method of the `fake` object to generate a text of 25 characters
        shipinstruct = fake.text(max_nb_chars=25) # @REPLACE EQUALS None.None(max_nb_chars=None)
        
        # Use the `text` method of the `fake` object to generate a text of 10 characters
        shipmode = fake.text(max_nb_chars=10) # @REPLACE EQUALS None.None(max_nb_chars=None)
        
        # Use the `text` method of the `fake` object to generate a text of 44 characters
        comment = fake.text(max_nb_chars=44) # @REPLACE EQUALS None.None(max_nb_chars=None)
        
        ### END CODE HERE ###

        # Generate the SQL insert statement
        insert_statement = f"""
            INSERT INTO public.lineitem (
                l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
                l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            ) VALUES (
                {orderkey}, {partkey}, {suppkey}, {linenumber}, {quantity}, {extendedprice}, {discount}, {tax},
                '{returnflag}', '{linestatus}', '{shipdate}', '{commitdate}', '{receiptdate}', '{shipinstruct}', '{shipmode}', '{comment}'
            );
        """
        
        # Write the SQL insert statement to the file
        insert_file.write(insert_statement + "\n")

        # Generate the SQL delete statement
        delete_statement = f"""
            DELETE FROM public.lineitem
            WHERE l_orderkey = {orderkey} AND l_linenumber = {linenumber};
        """
        
        # Write the SQL delete statement to the file
        delete_file.write(delete_statement + "\n")
```
:::

::: {.cell .markdown}
Now that you have executed the previous cell, the two files should be in
your `./sql` folder. Let\'s execute the insertions with the following
two cells.
:::

::: {.cell .code}
``` python
with open('./sql/individual_row_inserts.sql', 'r') as file:
    sql_commands = file.read()
```
:::

::: {.cell .code}
``` python
%timeit -n1 -r1 %sql $sql_commands
```
:::

::: {.cell .markdown}
Do you think inserting these 50 rows to the row database will take less
or more time? Now, let\'s delete the data.
:::

::: {.cell .code}
``` python
with open('./sql/individual_row_deletes.sql', 'r') as file:
    sql_commands = file.read()
```
:::

::: {.cell .code}
``` python
%timeit -n1 -r1 %sql $sql_commands
```
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Row-Based Database: PostgreSQL {#3---row-based-database-postgresql}

Up to now, you have run some benchmarking analytical queries with a
columnar database. Now, let\'s perform the same queries with the row
database, which is more suitable for transactional operations such as
writes and deletes. The PostgreSQL database is loaded with most tables,
but due to the main table\'s size, we have invoked a lambda function to
load the main table while you started the lab. You will verify that the
table is fully loaded before running the following exercises.
:::

::: {.cell .markdown}
`<a id='3-1'>`{=html}`</a>`{=html}

### 3.1 - Connecting to the Database {#31---connecting-to-the-database}

Let\'s import the credentials from the environment file and create the
connection string to the PostgreSQL database.
:::

::: {.cell .code}
``` python
RDSDBHOST = os.getenv('RDSDBHOST')
RDSDBPORT = os.getenv('RDSDBPORT')
RDSDBNAME = os.getenv('RDSDBNAME')
RDSDBUSER = os.getenv('RDSDBUSER')
RDSDBPASSWORD = os.getenv('RDSDBPASSWORD')

postgres_connection_url = f'postgresql+psycopg2://{RDSDBUSER}:{RDSDBPASSWORD}@{RDSDBHOST}:{RDSDBPORT}/{RDSDBNAME}'
print(postgres_connection_url)
```
:::

::: {.cell .code}
``` python
%sql {postgres_connection_url}
```
:::

::: {.cell .markdown}
Let\'s test that the main table is fully loaded. Run the following
queries, the first will list the available tables in the public schema,
the main table is named `lineitem` and should appear. The following
query brings the number of items in the `lineitem` table and the count
should be `59986052`. If the queries don\'t return the expected results,
wait a few minutes and rerun the queries.
:::

::: {.cell .code}
``` python
%%sql
SELECT * FROM information_schema.tables 
WHERE table_schema = 'public'
```
:::

::: {.cell .code}
``` python
%%sql 
SELECT count(*) FROM public.lineitem;
```
:::

::: {.cell .markdown}
`<a id='3-2'>`{=html}`</a>`{=html}

### 3.2 - Executing the Initial Queries {#32---executing-the-initial-queries}

Now that you have established the connection to the PostgreSQL database,
you will execute the same queries that you created and executed with the
Amazon Redshift database in the same order. First, let\'s start by
executing the query that retrieves the information about the orders
1552449, 13620130 and 45619461. You do not have to modify any syntax of
your previous query as Amazon Redshift maintains a query syntax
compatibility with PostgreSQL. Insert the corresponding query in the
cell below, execute it to benchmark the time it takes and compare it
with the time it took in Redshift.

**Note:** When you execute your query, you will see two connection
strings in the output, make sure that it appears as:

``` bash
 * postgresql+psycopg2://postgresuser:***@<RDSENDPOINT>.rds.amazonaws.com:5432/dev
   redshift+psycopg2://defaultuser:***@<REDSHIFTENDPOINT>.redshift.amazonaws.com:5439/dev
```

The asterisk must be next to the `postgresql+psycopg2` connection string
to use the PostgreSQL connection. Otherwise, you will be using the
Redshift connection string; if that\'s the case, make sure to execute
the previous cell that has the command `%sql {postgres_connection_url}`.
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1
raw_sql_statement = """    
    WITH lineitemorders AS (
        SELECT *
    FROM public.lineitem
    WHERE l_orderkey in (1552449, 13620130, 45619461)
    )
    
    SELECT DISTINCT lio.l_orderkey, lio.l_partkey, pt.p_name, ctr.c_custkey, ord.o_orderstatus, ord.o_orderdate, ntn.n_name, rgn.r_name
    FROM lineitemorders lio
    JOIN part pt ON pt.p_partkey = lio.l_partkey
    JOIN orders ord ON lio.l_orderkey = ord.o_orderkey
    JOIN customer ctr ON ctr.c_custkey = ord.o_custkey
    JOIN nation ntn ON ntn.n_nationkey = ctr.c_nationkey 
    JOIN region rgn ON rgn.r_regionkey = ntn.n_regionkey
    ;
"""

sql_statement = format_query(query=raw_sql_statement)
query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
In this query, you\'re mainly selecting rows from the tables. How does
this executing time compare to that of the Redshift database? Now,
let\'s execute the second query which is about extracting the number of
customers from the `'MIDDLE EAST'` region:
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1
raw_sql_statement = """    
    WITH avg_balance_middle_east AS (
        SELECT AVG(c_acctbal) AS avg_balance
        FROM customer
        JOIN nation ON customer.c_nationkey = nation.n_nationkey
        JOIN region ON nation.n_regionkey = region.r_regionkey
        WHERE region.r_name = 'MIDDLE EAST'
    )
    SELECT COUNT(DISTINCT customer.c_custkey)
    FROM customer
    JOIN nation ON customer.c_nationkey = nation.n_nationkey
    JOIN region ON nation.n_regionkey = region.r_regionkey
    WHERE region.r_name = 'MIDDLE EAST'
      AND customer.c_acctbal > (SELECT avg_balance FROM avg_balance_middle_east);
"""

sql_statement = format_query(query=raw_sql_statement)
query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
In this case, you performed an analytical query that consist of several
joins alongside filters and aggregation functions. How much time did it
take with Amazon Redshift database?
:::

::: {.cell .markdown}
`<a id='3-3'>`{=html}`</a>`{=html}

### 3.3 - Running the Benchmark Analytical Queries {#33---running-the-benchmark-analytical-queries}

As mentioned earlier in the Amazon Redshift section, you will execute
again some TPC-H benchmark queries. Given that you already know them,
you will simply execute again each of the following cells. Remember to
make a copy of the query in another cell if you want to visualize the
results to avoid losing the execution times. In PostgreSQL, some of the
benchmark queries can last for more than a minute, it is recommended to
wait for the cell to finish before moving to the next one.
:::

::: {.cell .markdown}
`<a id='3-3-1'>`{=html}`</a>`{=html}

### First TPC-H Query {#first-tpc-h-query}
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_0_tcp_h_q14.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
`<a id='3-3-2'>`{=html}`</a>`{=html}

### Second TPC-H Query - Optional {#second-tpc-h-query---optional}
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_1_tcp_h_q6.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df)
```
:::

::: {.cell .markdown}
`<a id='3-3-3'>`{=html}`</a>`{=html}

### Third TPC-H Query - Optional {#third-tpc-h-query---optional}
:::

::: {.cell .code execution_count="34"}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_2_tcp_h_q18.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
`<a id='3-3-4'>`{=html}`</a>`{=html}

### Fourth TPC-H Query - Optional {#fourth-tpc-h-query---optional}
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_3_tcp_h_q15.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
`<a id='3-3-5'>`{=html}`</a>`{=html}

### Fifth TPC-H Query - Optional {#fifth-tpc-h-query---optional}
:::

::: {.cell .code}
``` python
%%timeit -n1 -r1

sql_statement = format_query(path='./sql/pg_query_4_tcp_h_q1.sql')

query = %sql {sql_statement}
df = pd.DataFrame(query)
display(df.head())
```
:::

::: {.cell .markdown}
Given that PostgreSQL is a row-based transactional database, the
analytical queries take longer to be executed in this system than the
Amazon Redshift columnar database.
:::

::: {.cell .markdown}
`<a id='3-4'>`{=html}`</a>`{=html}

### 3.4 - Running the Insert and Delete Queries {#34---running-the-insert-and-delete-queries}

Finally, let\'s execute the insertion and deletion operations that you
performed over the Amazon Redshift database. In this case, given the
transactional design of PostgreSQL, you will notice the difference in
times when writing and deleting data. Start by inserting the data that
you already generated.
:::

::: {.cell .code}
``` python
with open('./sql/individual_row_inserts.sql', 'r') as file:
    sql_commands = file.read()
```
:::

::: {.cell .code}
``` python
%timeit -n1 -r1 %sql $sql_commands
```
:::

::: {.cell .markdown}
How much time did it take compared to Amazon Redshift? You can see that
writes in a row-based database are faster in this case. Now let\'s see
how deletions perform.
:::

::: {.cell .code}
``` python
with open('./sql/individual_row_deletes.sql', 'r') as file:
    sql_commands = file.read()
```
:::

::: {.cell .code}
``` python
%timeit -n1 -r1 %sql $sql_commands
```
:::

::: {.cell .markdown}
Again, you can see that the write/delete operations are faster in a row
database than with a columnar one.
:::

::: {.cell .markdown}
**Well Done!** In this demonstrative laboratory, you conducted a series
of experiments to compare the performance of various queries in Amazon
Redshift, a columnar database, and PostgreSQL, a row-oriented database.

Amazon Redshift demonstrated a better performance in handling complex
aggregation queries. The columnar storage format allowed it to scan only
the relevant columns, significantly reducing I/O and speeding up query
execution. PostgreSQL, while competent, was generally slower for these
types of queries due to its row-oriented storage which necessitates
scanning entire rows.

PostgreSQL is well-suited for transactional operations and OLTP
environments due to its ACID compliance, concurrency control mechanisms,
rich data types, and extensibility. While Amazon Redshift excels in
analytical workloads, PostgreSQL remains a reliable choice for
applications that require robust transaction processing, data integrity,
and concurrency management.
:::

::: {.cell .code}
``` python
```
:::
