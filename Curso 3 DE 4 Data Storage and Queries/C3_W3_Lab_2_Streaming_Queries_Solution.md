::: {.cell .markdown}
# Week 3 Lab 2: Streaming Queries with Apache Flink

In this lab, you will learn how to perform queries on top of streaming
sources using Apache Flink.

To get the solution files, follow these steps:

-   Go to the main menu and select `File -> Preferences -> Settings`.
-   Click on `Text Editor` on the left, then scroll down to the
    `Files: Exclude` section.
-   Remove the lines `**/C3_W3_Lab_2_Streaming_Queries_Solution.ipynb`
    and `**/C3_W3_Lab_2_Streaming_Queries_Flink_Studio_Solution.zpln`.
    The files will now appear in the explorer.
-   You can close the `Settings` tab.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction](#1)
-   [2 - Apache Flink 101](#2)
    -   [2.1 - Table Environment](#2-1)
    -   [2.2 - Table Definition](#2-2)
    -   [2.3 - Table Definition with SQL](#2-3)
    -   [2.4 - SQL Queries](#2-4)
    -   [2.5 - Window Queries](#2-5)
        -   [Exercise 1](#ex01)
        -   [Exercise 2](#ex02)
    -   [2.6 - Output Tables](#2-6)
-   [3 - PyFlink with Kinesis](#3)
:::

::: {.cell .markdown}
Start from loading the required libraries.
:::

::: {.cell .code}
``` python
import os
import json
import pandas as pd
from datetime import datetime
from IPython.display import HTML
from pyflink.table.expressions import col, lit
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.window import Slide, Tumble
from pyflink.table.udf import udf
```
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction {#1---introduction}

In the current landscape of data engineering, one of the biggest
challenges is real-time data analysis, which is necessary for certain
enterprises that require immediate insights and quick responses to
operate their business efficiently. Apache Flink is designed to handle
data in real time, allowing you to process and analyze streaming data as
soon as it arrives. This enables timely insights and immediate reactions
to changing data.

Here you will work with a Kinesis data producer similar to the one
introduced in Course 1, Week 4. This producer is continuously generating
data related to orders made by customers on the
[`classicmodels`](https://www.mysqltutorial.org/mysql-sample-database.aspx)
website. The business team at Classic Models is eager to gain deeper,
real-time insights into the behavior of clients on their websites. By
analyzing this streaming data, they aim to understand user buying
habits, preferences, and other business metrics as they happen. Through
this lab, you will leverage Apache Flink to process and analyze this
data stream, providing the business team with valuable, actionable
insights in real time.

To learn the basics of Apache Flink and PyFlink, you will use a sample
dataset based on stock prices. You will follow similar steps with the
Classic Models data.
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Apache Flink 101 {#2---apache-flink-101}

Apache Flink is a robust framework and processing engine designed for
executing computations on data streams. These data streams can be either
unbounded, such as continuous streams of real-time data or bounded, like
a finite batch of data. Flink is structured around two major APIs: the
**DataStream API** and the **Table API & SQL**. The DataStream API is
tailored for complex event processing and real-time analytics, while the
Table API & SQL offers a more declarative approach to data processing,
akin to traditional database operations.

In this lab, you will be utilizing PyFlink, which is the Python API for
Apache Flink. PyFlink enables developers to harness the power of Flink
using Python, making it accessible to those familiar with the language.
Additionally, PyFlink integrates with `pandas`, allowing for seamless
data manipulation and analysis within the Flink ecosystem. You use the
PyFlink Table API to run queries on top of a Kinesis data stream, but
first, you will learn how to use PyFlink with some sample data we
provided. You can find more information
[here](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table_api_tutorial/).
:::

::: {.cell .markdown}
`<a id='2-1'>`{=html}`</a>`{=html}

### 2.1 - Table Environment {#21---table-environment}

To start using the PyFlink Table API, you first need to declare a table
environment. This environment acts as the primary gateway for all
interactions with the Flink runtime.
:::

::: {.cell .code}
``` python
table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
```
:::

::: {.cell .markdown}
`<a id='2-2'>`{=html}`</a>`{=html}

### 2.2 - Table Definition {#22---table-definition}

The core element of the Table API & SQL in Apache Flink is the entity
known as a **Table**. Tables act as both the input and output for
queries, serving as the fundamental data structures upon which all
operations are performed. In a Flink program, the first step involves
defining these tables, meaning defining the schema and the connection
details, a table can be connected to various source or target systems,
such as databases, files, message queues or data streams.

After establishing the tables, you can perform a wide range of
operations on them, such as filtering, aggregating, joining, and
transforming the data. These operations can be expressed using either
the Table API, which provides a programmatic way to manipulate tables,
or SQL queries, which offer a more declarative approach.

You have been provided two samples of the data produced by the Stock
Market and Classic Models Kinesis data streams, located at
`data/sample_stock.json` and `data/sample.json` respectively. You will
interact with the files using PyFlink before connecting to the Classic
Models stream. Load the files using the following cells and understand
what each contains:
:::

::: {.cell .code}
``` python
sample_stock_data = []
with open("data/sample_stock.json") as f:
    for line in f.readlines():
        sample_stock_data.append(json.loads(line))
print(json.dumps(sample_stock_data[0], indent=4))
```
:::

::: {.cell .code}
``` python
sample_data = []
with open("data/sample.json") as f:
    for line in f.readlines():
        sample_data.append(json.loads(line))
print(json.dumps(sample_data[0], indent=4))
```
:::

::: {.cell .markdown}
The Kinesis Data Streams, and in general streaming sources, are
unbounded sequences of records. In the Stock market sample, you have
information about the price of a certain ticker at a point in time.
Before loading these records to a `Table` in Flink, you will define the
schema from source stock data:
:::

::: {.cell .code}
``` python
row_stock_schema = DataTypes.ROW([
    DataTypes.FIELD("event_time", DataTypes.TIMESTAMP(0)),
    DataTypes.FIELD("ticker", DataTypes.STRING()),
    DataTypes.FIELD("price", DataTypes.DOUBLE())
    ])
```
:::

::: {.cell .markdown}
On the other hand, a record for the Classic Models stream seems to be an
order made by a customer in JSON format. Using the `DataTypes` classes,
define the schema for the sample data in the `row_source_schema`
variable.

*Note*: DataTypes includes the `INT`, `DATE` and `BOOLEAN` classes, for
`TIMESTAMP` we define a value `(p)` where p is the number of digits of
fractional seconds (precision), in our case 0.
:::

::: {.cell .code}
``` python
row_source_schema = DataTypes.ROW([
    DataTypes.FIELD("order_id", DataTypes.STRING()),
    DataTypes.FIELD("order_timestamp", DataTypes.TIMESTAMP(0)),
    DataTypes.FIELD("order_date", DataTypes.DATE()),
    DataTypes.FIELD("customer_number", DataTypes.INT()),
    DataTypes.FIELD("customer_visit_number", DataTypes.INT()),
    DataTypes.FIELD("customer_city", DataTypes.STRING()),
    DataTypes.FIELD("customer_country", DataTypes.STRING()),    
    DataTypes.FIELD("customer_credit_limit", DataTypes.INT()),
    DataTypes.FIELD("device_type",DataTypes.STRING()),
    DataTypes.FIELD("browser", DataTypes.STRING()),
    DataTypes.FIELD("operating_system", DataTypes.STRING()),
    DataTypes.FIELD("product_code",DataTypes.STRING()),
    DataTypes.FIELD("product_line", DataTypes.STRING()),
    DataTypes.FIELD("product_unitary_price", DataTypes.DOUBLE()),
    DataTypes.FIELD("in_shopping_cart", DataTypes.BOOLEAN()),
    DataTypes.FIELD("quantity", DataTypes.INT()),
    DataTypes.FIELD("total_price", DataTypes.DOUBLE()),
    DataTypes.FIELD("traffic_source", DataTypes.STRING())
    ])
```
:::

::: {.cell .markdown}
You will enforce this schema on the `sample_stock_data` and
`sample_data` arrays, as the JSON files has some of the column values
that are meant to be float and date types stored as strings.
:::

::: {.cell .code}
``` python
for record in sample_stock_data:
    record['event_time'] = datetime.fromisoformat(record['event_time'])
    record['price'] = float(record['price'])
```
:::

::: {.cell .markdown}
Perform the same enforcement on the numeric and date values of the
`sample_data` array.
:::

::: {.cell .code}
``` python
for record in sample_data:
    record['order_timestamp'] = datetime.fromisoformat(record['order_timestamp'])
    record['order_date'] = datetime.strptime(record['order_date'],'%Y-%m-%d')
    record['product_unitary_price'] = float(record['product_unitary_price'])
    record['total_price'] = float(record['total_price'])
```
:::

::: {.cell .markdown}
Now you can create the `Table` for each sample data using the
`from_elements` function, PyFlink has some predefined sources for
Python, such as `pandas` dataframe or collections.
:::

::: {.cell .code}
``` python
source_stock_table = table_env.from_elements(sample_stock_data, row_stock_schema)
```
:::

::: {.cell .code}
``` python
source_table = table_env.from_elements(sample_data, row_source_schema)
```
:::

::: {.cell .markdown}
To verify the data is loaded properly, you can convert the PyFlink
Tables to Pandas DataFrames and print the first records.
:::

::: {.cell .code}
``` python
source_stock_df = source_stock_table.to_pandas()
source_stock_df.head()
```
:::

::: {.cell .code}
``` python
source_table_df = source_table.to_pandas()
source_table_df.head()
```
:::

::: {.cell .markdown}
`<a id='2-3'>`{=html}`</a>`{=html}

### 2.3 - Table Definition with SQL {#23---table-definition-with-sql}

Now explore an alternative and more intuitive method to define a table
in Flink by using SQL and connectors. In this approach, you will define
an SQL table that points to the `filesystem` connector and configure
various properties such as format and path. Flink provides robust
support for connecting to a variety of data sources, including local
files, message queues like Kafka, data streams like Kinesis, and even
databases. This flexibility allows us to seamlessly integrate Flink with
diverse data ecosystems, making it a powerful tool for real-time data
processing.

You will also add a **WATERMARK** to the timestamp field in the table
definition, the watermark helps Flink to recognize the timestamp field
as the event time for each record. In Flink, data can be processed based
on two types of time columns: processing time and event time. Processing
time refers to the system time of the machine running Flink, while event
time refers to the actual time at which the events occurred within the
stream.
:::

::: {.cell .code}
``` python
stock_table_ddl = """
  CREATE TABLE stock_data (
    event_time TIMESTAMP(0),
    ticker STRING,
    price NUMERIC,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTES
    ) with (
        'connector' = 'filesystem',
        'format' = 'json',
        'path' = '{}',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""".format("data/sample_stock.json")
table_env.execute_sql(stock_table_ddl)
```
:::

::: {.cell .markdown}
Use SQL to define the `source_data` table based on the
`data/sample.json` file, don\'t forget to include the WATERMARK for the
`order_timestamp` field.
:::

::: {.cell .code}
``` python
table_env.execute_sql("DROP TABLE IF EXISTS source_data")
source_table_ddl = """
  CREATE TABLE source_data (
    order_id STRING,
    order_timestamp TIMESTAMP(0),
    order_date STRING,
    customer_number INT,
    customer_visit_number INT,
    customer_city STRING,
    customer_country STRING,
    customer_credit_limit INT,
    device_type STRING,
    browser STRING,
    operating_system STRING,
    product_code STRING, 
    product_line STRING,
    product_unitary_price NUMERIC,
    quantity INT, 
    total_price NUMERIC,
    traffic_source STRING,
    WATERMARK FOR order_timestamp AS order_timestamp - INTERVAL '5' MINUTES
    ) with (
        'connector' = 'filesystem',
        'format' = 'json',
        'path' = '{}',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""".format("data/sample.json")
table_env.execute_sql(source_table_ddl)
```
:::

::: {.cell .markdown}
Using the from_path function you can bring a table created with SQL as a
Table object in Pyflink, we can then run the `execute` and `print`
functions to verify that the data was loaded successfully. In the case
of the Classic Models data, convert the result to pandas and execute the
`head` function, for better readability.
:::

::: {.cell .code}
``` python
stock_table = table_env.from_path("stock_data")
stock_table.execute().print()
```
:::

::: {.cell .code}
``` python
input_table = table_env.from_path("source_data")
input_table_df = input_table.to_pandas()
input_table_df.head()
```
:::

::: {.cell .markdown}
`<a id='2-4'>`{=html}`</a>`{=html}

### 2.4 - SQL Queries {#24---sql-queries}

Now that you have the source data in Table format and in the catalog,
you can do run SQL queries on top of the table in the catalog. As the
source data is bounded, you can run queries and get results rather
quickly. If the source data was in a stream then you would have to find
more carefully how your queries are meant to be run, so they don\'t run
indefinitely.

Let\'s start with a simple query. You want to know the average price for
the Amazon stock \'AMZN\'.
:::

::: {.cell .code}
``` python
query_result = table_env.execute_sql("""SELECT AVG(price) as avg_price
                                     FROM stock_data
                                     WHERE ticker = 'AMZN'
                                     """)
query_result.print()
```
:::

::: {.cell .markdown}
As Flink goes through the table, some operations, such as group
aggregation, produce update events, and in this case, the aggregation do
get updated. Now, create a query to get the total amount that each
customer has spend in the Classic Models sample table.
:::

::: {.cell .code}
``` python
query_result = table_env.execute_sql("""SELECT customer_number, 
                                     SUM(total_price) as customer_total_amount
                                     FROM source_data GROUP BY customer_number
                                     """)
query_result.print()
```
:::

::: {.cell .markdown}
`<a id='2-5'>`{=html}`</a>`{=html}

### 2.5 - Window Queries {#25---window-queries}

Before going into more detail about window queries, let\'s define a
user-defined function (UDF) to convert timestamp fields into string
fields. This will help us when writing results into output tables.
:::

::: {.cell .code}
``` python
@udf(input_types=[DataTypes.TIMESTAMP(3)], result_type=DataTypes.STRING())
def to_string(i):
    return str(i)

table_env.create_temporary_system_function("to_string", to_string)
```
:::

::: {.cell .markdown}
Windows is the main tool to process unbounded streaming data. It allows
us to divide the stream into manageable finite section and appy
computations for each section. In Flink, the syntax will be:

``` python
stream.window(...)                 <-  required: "assigner"
      .reduce/aggregate/apply()    <-  required: "function"
```
:::

::: {.cell .markdown}
A **tumbling windows assigner** assigns each element to a window of a
specified window size, the window size is fixed and the windows don\'t
overlap.

![tumbling-windows](images/tumbling-windows.png)
:::

::: {.cell .markdown}
Here is an example of a Tumble window query, you will look at the
minimum price of a stock for 10 second windows.
:::

::: {.cell .code}
``` python
example_tumbling_window_table = (
    stock_table.window(
        Tumble.over(lit(10).seconds).on(col("event_time")).alias("ten_second_window")
    )
    .group_by(col('ticker'), col('price'), col('ten_second_window'))
    .select(col('ticker'), col('price').min.alias('price'), (to_string(col('ten_second_window').end)).alias('event_time'))
)

example_tumbling_window_df = example_tumbling_window_table.to_pandas()
example_tumbling_window_df.head()
```
:::

::: {.cell .markdown}
`<a id='ex01'>`{=html}`</a>`{=html}

### Exercise 1

Create a query to get the number of orders in 1-minute windows of time
(60 second windows).
:::

::: {.cell .code}
``` python
tumbling_window_table = (
    input_table.window(
        ### START CODE HERE ### (~ 1 line of code)
        Tumble.over(lit(60).seconds).on(col("order_timestamp")).alias("one_minute_window") # @REPLACE None.None(None(60).seconds).on(col("order_timestamp")).alias("one_minute_window")
        ### END CODE HERE ###
    )
    .group_by(col('one_minute_window'))
    .select((to_string(col('one_minute_window').end)).alias('event_time'),col('order_id').count.distinct.alias('num_orders'))
)

tumbling_window_df = tumbling_window_table.to_pandas()
tumbling_window_df.head()
```
:::

::: {.cell .markdown}
##### **Expected Output**

  **event_time**        **num_orders**
  --------------------- ----------------
  2024-06-01 00:01:00   1
  2024-06-01 00:02:00   2
  2024-06-01 00:03:00   2
  2024-06-01 00:04:00   2
  2024-06-01 00:05:00   2
:::

::: {.cell .markdown}
*Note*: You can also perform window queries with Flink SQL, this is an
example of one:
:::

::: {.cell .code}
``` python
tumble_query = """SELECT window_start, window_end, COUNT(DISTINCT order_id) AS total_orders
  FROM TABLE(
    TUMBLE(TABLE source_data, DESCRIPTOR(order_timestamp), INTERVAL '60' SECONDS))
  GROUP BY window_start, window_end; """
query_result = table_env.execute_sql(tumble_query)
query_result.print()
```
:::

::: {.cell .markdown}
The **sliding window assigner** assigns elements to windows of fixed
length, it requires two arguments: window size and window slide. The
window slide determines how frequently the windows are created, if this
value is larger than the window size, then the windows will be
overlapping.

![sliding-windows](images/sliding-windows.png)
:::

::: {.cell .markdown}
This is an example on how to use a Sliding window query using PyFlink
functions. Here first you will create the sliding window for 10 seconds
every 5 seconds on the Stock table, then get the minimum price for each
ticker in that window of time.
:::

::: {.cell .code}
``` python
example_sliding_window_table = (
    stock_table
    .window(
        Slide.over(lit(10).seconds)
        .every(lit(5).seconds)        
        .on(col("event_time"))
        .alias("ten_second_window")
    )
    .group_by(col("ticker"), col("ten_second_window"))
    .select(col("ticker"), col("price").min.alias("price"),
            to_string(col("ten_second_window").end).alias("event_time"))
)

example_sliding_window_df = example_sliding_window_table.to_pandas()
example_sliding_window_df.head()
```
:::

::: {.cell .markdown}
`<a id='ex02'>`{=html}`</a>`{=html}

### Exercise 2

Create a sliding window query to get the total amount of sales for a
sliding window of 6 minutes, every 3 minutes.
:::

::: {.cell .code}
``` python
sliding_window_table = (
        input_table.window(
            ### START CODE HERE ### (~ 2 lines of code)
            Slide.over(lit(6).minute) # @REPLACE None.None(None(6).minute)
            .every(lit(3).minutes) # @REPLACE .None(None(3).minutes)
            ### END CODE HERE ###
            
            .on(col("order_timestamp"))
            .alias("six_minute_window")
        )
        .group_by(col("six_minute_window"))
        .select((col("six_minute_window").end).alias("event_time"), col("total_price").sum.alias("total_sales"))
    )

sliding_window_df = sliding_window_table.to_pandas()
sliding_window_df.head()
```
:::

::: {.cell .markdown}
##### **Expected Output** {#expected-output}

  **event_time**        **total_sales**
  --------------------- -----------------
  2024-06-01 00:03:00   2966
  2024-06-01 00:06:00   6045
  2024-06-01 00:09:00   5826
  2024-06-01 00:12:00   5511
  2024-06-01 00:15:00   5389
:::

::: {.cell .markdown}
`<a id='2-6'>`{=html}`</a>`{=html}

### 2.6 - Output Tables {#26---output-tables}

In Apache Flink, you need to define output tables, known as **sinks**,
to store the results of the queries. The process of defining sink tables
is similar to defining source tables, with the primary distinction being
the specific sink configurations that can be applied. Additionally,
Flink provides **print tables**, which are particularly useful for
development and debugging purposes. Print tables allow us to output the
results of a query directly to the console, enabling us to quickly
verify and test our data processing logic. Here are some examples:
:::

::: {.cell .code}
``` python
query_result = table_env.sql_query("SELECT customer_number, SUM(total_price) as session_price FROM source_data GROUP BY customer_number")
```
:::

::: {.cell .code}
``` python
table_env.create_temporary_view("query_result_view", query_result)
```
:::

::: {.cell .code}
``` python
print_sink_ddl = """
    create table sink_table (
        customer_number INT,
        total_price DOUBLE
    ) with (
        'connector' = 'print'
    )
"""

table_env.execute_sql(print_sink_ddl)
```
:::

::: {.cell .code}
``` python
table_result = table_env.execute_sql("INSERT INTO {0} SELECT * FROM {1}"
                                         .format("sink_table", "query_result_view"))
```
:::

::: {.cell .markdown}
You can revisit one of the previous example queries. Define a Table with
the filesystem connector and save the results of the query into a JSON
file.
:::

::: {.cell .code}
``` python
example_sliding_window_table = (
    stock_table
    .window(
        Slide.over(lit(10).seconds)
        .every(lit(5).seconds)
        .on(col("event_time"))
        .alias("ten_second_window")
    )
    .group_by(col("ticker"), col("ten_second_window"))
    .select(col("ticker"), col("price").min.alias("price"),
            to_string(col("ten_second_window").end).alias("event_time")))
table_env.create_temporary_view("example_sliding_window_view", example_sliding_window_table)
```
:::

::: {.cell .code}
``` python
output_ddl =  """ CREATE TABLE json_sink (
                ticker VARCHAR(6),
                price DOUBLE,
                event_time VARCHAR(64)
              )
              PARTITIONED BY (ticker)
              WITH (
                'connector' = 'filesystem',
                'path' = '{0}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format('data/output_sample')
table_env.execute_sql(output_ddl)
```
:::

::: {.cell .code}
``` python
table_result = table_env.execute_sql("INSERT INTO {0} SELECT * FROM {1}"
                                         .format("json_sink", "example_sliding_window_view"))
```
:::

::: {.cell .markdown}
Check the folder in the `data/output_sample` path to see the output of
the query in JSON files.
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - PyFlink with Kinesis {#3---pyflink-with-kinesis}

For Flink (and PyFlink) to work with Kinesis, you would need to
configure Flink SQL Connectors using JARs. JAR stands for **J**ava
**AR**chive, it contains a collection of files that compose a package.
Instead of configuring the connectors, to better leverage the power of
Kinesis, you are going to use the Amazon Managed Service for Apache
Flink service. The Managed service allows to develop Flink applications
or to run Flink in notebooks in a Studio environment. You will use it to
run queries on the Kinesis Data stream.
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
3.1. To enter the studio which was provided for this lab, go to the AWS
console and search for `Managed Apache Flink`. Select the service, and
you should see a dashboard like this one:

![kinesis dashboard](images/kinesis_dashboard.png)
:::

::: {.cell .markdown}
3.2. Select the **Studio notebooks** link in the sidebar of the
dashboard, you should see this new Dashboard with a single Studio
notebook:

![kinesis studio dashboard](images/studio_dashboard.png)
:::

::: {.cell .markdown}
3.3. Click on the notebook name and click on **Run** button. It takes
around 3-5 minutes to get the Studio environment ready. Once it\'s done,
the status should change to `Running`

![kinesis studio running](images/studio_running.png)
:::

::: {.cell .markdown}
3.4. Select the studio notebook name and then click on **Open in Apache
Zeppelin** button. You now should see the Apache Zeppelin Environment
UI:

![zeppelin dashboard](images/zeppelin_import.png)
:::

::: {.cell .markdown}
3.5. In explorer you should see the
`C3_W3_Lab_2_Streaming_Queries_Flink_Studio.zpln` notebook. Download it
locally, then go to the Zeppelin UI, click on **Import note**. A new
menu should appear. Click on `Select JSON File/IPYNB File` and import
the `C3_W3_Lab_2_Streaming_Queries_Flink_Studio.zpln` notebook. The
notebook should now appear in the UI:

![zeppelin imported](images/importer_notebook.png)
:::

::: {.cell .markdown}
3.6. Open the notebook and follow the instructions there.
:::

::: {.cell .code}
``` python
```
:::
