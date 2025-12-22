::: {#4d6705e4-db90-4979-be94-bc2dcd871c44 .cell .markdown}
# Week 1 Lab: Data Normalization

In this lab, you will learn how to transform a denormalized table or
\"One Big Table\" (OBT) into First Normal Form (1NF), Second Normal Form
(2NF), and Third Normal Form (3NF). This process is fundamental to
database normalization, which helps reduce data redundancy and improve
data integrity.

To open the solution notebook, follow these steps:

-   Go to the main menu and select `File -> Preferences -> Settings`.
-   Click on `Text Editor` on the left, then scroll down to the
    `Files: Exclude` section.
-   Remove the line `**/C4_W1_Lab_1_Data_Normalization_Solution.ipynb`.
    The file will now appear in the explorer.
-   You can close the `Settings` tab.
:::

::: {#761589e4-4b64-488f-9a21-4e6b0c560d97 .cell .markdown}
# Table of Contents

-   [1 - Introduction](#1)
    -   [1.1 - Data Normalization](#1-1)
    -   [1.2 - Dataset](#1-2)
-   [2 - First Normal Form (1NF)](#2)
-   [3 - Second Normal Form (2NF)](#3)
-   [4 - Third Normal Form (3NF)](#4)
:::

::: {#887c8947-0e07-4968-8c36-ff4e37df0025 .cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction {#1---introduction}

As a Data Engineer, you may not frequently perform data normalization
from scratch, but understanding this process is crucial. Typically, you
will encounter source databases that are already normalized, and your
task will often involve denormalizing this data to make it useful for
extracting analytical insights or solving business questions. This lab
focuses on the opposite process: taking a dataset stored as a One Big
Table and normalizing it up to the third normal form, which is common in
transactional systems.
:::

::: {#2e459a9c-33da-4641-aa16-d2e56e2b4e38 .cell .markdown}
`<a id='1-1'>`{=html}`</a>`{=html}

### 1.1 - Data Normalization {#11---data-normalization}

Normalization is a database design technique that involves dividing
large tables into smaller, less redundant tables and defining
relationships between them. The goal is to isolate data so that
additions, deletions, and modifications can be made in a single table
and then propagated through the rest of the database using defined
relationships.

Here are some of the normalization benefits:

-   Reduce Data Redundancy: Eliminating duplicate data saves storage
    space and ensures consistency across the database.
-   Improve Data Integrity: Ensuring that each piece of data is stored
    in only one place reduces the likelihood of data anomalies and
    maintains the accuracy of the data.
-   Enhance Update/Delete Query Performance
:::

::: {#4b36eb02-51a5-4cad-856b-77d41df06938 .cell .markdown}
`<a id='1-2'>`{=html}`</a>`{=html}

### 1.2 - Dataset {#12---dataset}

The dataset you\'ll use contains similar information to the normalized
classicmodels you used in previous labs. However, in this lab, the data
is stored as One Big Table, originally extracted from a multiline JSON
file, where each JSON object has the following structure:
:::

::: {#2cc156d6-d29e-4412-880f-cbdd03cc11d1 .cell .markdown}
``` json
{
  "orderNumber": 10100,
  "orderDate": "2003-01-06",
  "requiredDate": "2003-01-13",
  "shippedDate": "2003-01-10",
  "status": "Shipped",
  "comments": null,
  "orderDetails": [
    {
      "productCode": "S18_1749",
      "quantityOrdered": 30,
      "priceEach": 136.00
    },
    {
      "productCode": "S18_2248",
      "quantityOrdered": 50,
      "priceEach": 55.09
    },
    {
      "productCode": "S18_4409",
      "quantityOrdered": 22,
      "priceEach": 75.46
    },
    {
      "productCode": "S24_3969",
      "quantityOrdered": 49,
      "priceEach": 35.29
    }
  ],
  "customer": {
    "customerName": "Online Diecast Creations Co.",
    "contactLastName": "Young",
    "contactFirstName": "Dorothy",
    "phone": "6035558647",
    "addressLine1": "2304 Long Airport Avenue",
    "addressLine2": null,
    "city": "Nashua",
    "state": "NH",
    "postalCode": "62005",
    "country": "USA",
    "salesRepEmployeeNumber": 1216.00,
    "creditLimit": 114200.00
  }
}
```
:::

::: {#c6b1c208-f47c-41b7-b16b-522ae0a81fb6 .cell .markdown}
This dataset has already been uploaded into the database as a table
named `orders` under the schema `classicmodels_obt`. Here\'s the
table\'s schema:

`<img src="./images/ERD_OBT.png" width="300">`{=html}

Note that the `orderDetails` and `customer` fields are saved as JSON
objects directly in the database: each object is structured as a
dictionary with key-value pairs holding information about each order and
the customer that placed that order.
:::

::: {#9fb8a4fb-7a52-4f26-b6c5-1b143185161c .cell .markdown}
To explore the data, let\'s import all the necessary packages and SQL
extensions for running the `%sql` magic commands used in this notebook:
:::

::: {#106d6493-afc8-4e46-84f0-ac177edf8919 .cell .code execution_count="1"}
``` python
import os 
import json

import pandas as pd
import psycopg2

from IPython.display import HTML
from dotenv import load_dotenv
from sqlalchemy import create_engine

pd.set_option('display.max_columns', 30)
```
:::

::: {#d256d614-365c-40de-ab6e-615a4af08ca5 .cell .code execution_count="2"}
``` python
%load_ext sql
```
:::

::: {#6dd88514 .cell .markdown}
Run the following code to get the link to the AWS console.

*Note*: For security reasons, the URL to access the AWS console will
expire every 15 minutes, but any AWS resources you created will remain
available for the 2 hour period. If you need to access the console after
15 minutes, please rerun this code cell to obtain a new active link.
:::

::: {#fa98bb61 .cell .code}
``` python
with open('../.aws/aws_console_url', 'r') as file:
    aws_url = file.read().strip()

HTML(f'<a href="{aws_url}" target="_blank">GO TO AWS CONSOLE</a>')
```
:::

::: {#b2973bf2 .cell .markdown}
*Note:* If you see the window like in the following printscreen, click
on **logout** link, close the window and click on console link again.

![AWSLogout](images/AWSLogout.png)
:::

::: {#f6d10ebf .cell .markdown}
In the AWS console, go to **CloudFormation**.

-   Click on the alphanumeric stack name and find the **Outputs** tab.
    You will see the key `PostgresEndpoint` and its corresponding
    **Value** column. Copy the value.
-   Edit the `./src/env` file, replacing the placeholder
    `<RDS-ENDPOINT>` with the endpoint value.
-   Save changes to the file.

Execute the following cell to load the environment variables and connect
to the database:
:::

::: {#8d42d088 .cell .code execution_count="3"}
``` python
load_dotenv('./src/env', override=True)

DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')
DBNAME = os.getenv('DBNAME')
DBUSER = os.getenv('DBUSER')
DBPASSWORD = os.getenv('DBPASSWORD')

connection_url = f"postgresql+psycopg2://{DBUSER}:{DBPASSWORD}@{DBHOST}:{DBPORT}/{DBNAME}"

%sql {connection_url}
     
```
:::

::: {#d41c113d-a833-452b-835f-7f3d372140b2 .cell .markdown}
Explore the loaded data:
:::

::: {#0da6f294-9293-49c6-9f66-6107011d299f .cell .code}
``` python
%%sql
select count(*) from classicmodels_obt.orders;
```
:::

::: {#16712591-1b8e-4a7c-a8a5-669c47c17777 .cell .code}
``` python
%%sql
select * from classicmodels_obt.orders limit 3;
```
:::

::: {#93704eee-21dd-4221-aae0-1a1e32372bea .cell .markdown}
The `orderdetails` and `customer` fields are not of basic data types,
they are dictionaries with the following structure:

-   `orderdetails` is a list of dictionaries/JSON objects about all the
    products placed within an individual order. Each entry in the list
    refers to a product, with keys on its product code, the quantity
    ordered and the unitary price.

-   `customer` field contains a dictionary/JSON object where each key
    corresponds to a feature of the customer that placed the order, such
    as personal details, contact, and location.

Now you will convert this denormalized table to a first normal form, by
unnesting these two columns and ensuring that the table has a unique
primary key.
:::

::: {#4c416b73-70f9-4369-82d1-70b0c978fa93 .cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - First Normal Form (1NF) {#2---first-normal-form-1nf}

Your first task is to create the First Normal Form (1NF) and insert it
into the database. For that, you will read the data using an SQL query
and then transform the data using `pandas` package.

The features of the First Normal Form is that each column has a single
value, meaning there should be no nested data in any columns, and that
the table has a unique primary key:

-   For that, you will unnest each list in the `orderdetails` column,
    meaning you will create a row for each element in the list. In
    addition, since each element is a JSON object or dictionary, you
    will unnest each element by creating a new field or column for each
    key inside this dictionary. You will repeat this last step for the
    `customer` field.

-   After you unnest these two columns, each row will represent an item
    placed within an order. So to uniquely identify each row, you will
    create a column `orderlinenumber`that denotes the item\'s number
    within its corresponding order. The two columns `ordernumber` and
    `orderlinenumber` will represent the composite primary key of the
    table.

The final schema after the 1NF will look like the following image:

`<img src="./images/ERD_1NF_Orders.png" width="400">`{=html}
:::

::: {#a3346da7-f3f0-47e3-a5c8-2eb252282291 .cell .markdown}
2.1. Create the schema in which the relations in 1NF will be stored and
then read the OBT dataset and save it as a `pandas` DataFrame.
:::

::: {#07375627-45aa-4cd3-b885-428e59e27129 .cell .code}
``` python
%%sql
CREATE SCHEMA IF NOT EXISTS classicmodels_1nf;
```
:::

::: {#7807de29-a08b-4fcb-ad51-2b2ca12c7910 .cell .markdown}
Have a look again at the original data:
:::

::: {#89cd13aa-7c0b-4816-97eb-9729a6b94988 .cell .code}
``` python
result = %sql select * from classicmodels_obt.orders

df = result.DataFrame()

df.head()
```
:::

::: {#69e167d2-8a3e-4bb5-aac4-301f18909e9c .cell .markdown}
2.2. Create a new flat table by unnesting the content of the `customer`
field of the `df` DataFrame. For that, you can use the `pandas`
[`json_normalize()`
method](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html)
over the `customer` field. This function will return a DataFrame with
the keys of the key-value pairs as columns in the new table.

*Note*: In the cells where you see the comments
`### START CODE HERE ###` and `### END CODE HERE ###` you need to
complete the code replacing all `None`. The rest of the cells are
already complete, you just need to review and run the code.
:::

::: {#1de17bc4-7fb0-4cb3-a07c-01ffb56b0722 .cell .code}
``` python
### START CODE HERE ### (1 line of code)
customers_df = pd.json_normalize(df['customer']) # @REPLACE EQUALS pd.None(None['None'])
### END CODE HERE ###

customers_df.head()
```
:::

::: {#5be37ab8-94bb-4c9c-83fc-d2fbd375adec .cell .markdown}
##### **Expected Output**

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **city**    **phone**    **state**   **country**   **postalCode**   **creditLimit**   **addressLine1**   **addressLine2**   **customerName**   **contactLastName**   **contactFirstName**   **salesRepEmployeeNumber**
  ----------- ------------ ----------- ------------- ---------------- ----------------- ------------------ ------------------ ------------------ --------------------- ---------------------- ----------------------------
  Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0
                                                                                        Avenue                                Creations Co.                                                   

  Frankfurt   +49 69 66 90 None        Germany       60528            59700.0           Lyonerstr. 34      None               Blauer See Auto,   Keitel                Roland                 1504.0
              2555                                                                                                            Co.                                                             

  NYC         2125551500   NY          USA           10022            76400.0           2678 Kingston Rd.  Suite 101          Vitachrome Inc.    Frick                 Michael                1286.0

  Stavern     07-98 9555   None        Norway        4110             81700.0           Erling Skakkes     None               Baane Mini Imports Bergulfsen            Jonas                  1504.0
                                                                                        gate 78                                                                                               

  Madrid      \(91\) 555   None        Spain         28034            227600.0          C/ Moralzarzal, 86 None               Euro+ Shopping     Freyre                Diego                  1370.0
              94 44                                                                                                           Channel                                                         
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#e3e95479-b262-4883-9854-5bae3a204aa4 .cell .markdown}
`pd.json_normalize` creates a DataFrame where each dictionary in the
`customer` column is flattened into a row. The index of the original
DataFrame is preserved, which is crucial for maintaining the correct
relationship between the original rows and the new flattened rows. It is
time to concatenate the two datasets.
:::

::: {#9d688b28-ace7-40f1-b5a9-9cc254b99b9c .cell .markdown}
2.3. You need to drop the `customer` column from the original dataframe,
using the `drop()` method and specifying the column. You should keep
`inplace` argument equal to `True`.

Then concatenate the dataframe `df` with `customers_df`. For that, you
will use `pd.concat` method with the `axis` parameter set as 1. Using
`pd.concat` with `axis=1` joins the DataFrames column-wise, aligning
rows by their index. Since the index is preserved, each row in the
flattened `customers_df` dataframe aligns correctly with its
corresponding row in the original dataframe.
:::

::: {#3eae1056-7955-4287-85bf-1b2200f35fe5 .cell .code}
``` python
### START CODE HERE ### (2 lines of code)
df.drop(columns='customer', inplace=True) # @REPLACE df.None(columns='None', inplace=True)
df = pd.concat([df, customers_df], axis=1) # @REPLACE EQUALS pd.None([df, None], axis=1)
### END CODE HERE ###

df.head()
```
:::

::: {#e611ab1a-5cda-49b6-b670-dbd8e89fb6e5 .cell .markdown}
##### **Expected Output** {#expected-output}

*Note*: Some text is omitted.

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **ordernumber**   **orderdate**   **requireddate**   **shippeddate**   **status**   **comments**    **orderdetails**    **city**    **phone**    **state**   **country**   **postalCode**   **creditLimit**   **addressLine1**   **addressLine2**   **customerName**   **contactLastName**   **contactFirstName**   **salesRepEmployeeNumber**
  ----------------- --------------- ------------------ ----------------- ------------ --------------- ------------------- ----------- ------------ ----------- ------------- ---------------- ----------------- ------------------ ------------------ ------------------ --------------------- ---------------------- ----------------------------
  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            \[{\'priceEach\':   Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0
                                                                                                      136.0,                                                                                                    Avenue                                Creations Co.                                                   
                                                                                                      \'productCode\':                                                                                                                                                                                                
                                                                                                      \'S18_1749\...                                                                                                                                                                                                  

  10101             2003-01-09      2003-01-18         2003-01-11        Shipped      Check on        \[{\'priceEach\':   Frankfurt   +49 69 66 90 None        Germany       60528            59700.0           Lyonerstr. 34      None               Blauer See Auto,   Keitel                Roland                 1504.0
                                                                                      availability.   108.06,                         2555                                                                                                            Co.                                                             
                                                                                                      \'productCode\':                                                                                                                                                                                                
                                                                                                      \'S18_232\...                                                                                                                                                                                                   

  10102             2003-01-10      2003-01-18         2003-01-14        Shipped      None            \[{\'priceEach\':   NYC         2125551500   NY          USA           10022            76400.0           2678 Kingston Rd.  Suite 101          Vitachrome Inc.    Frick                 Michael                1286.0
                                                                                                      95.55,                                                                                                                                                                                                          
                                                                                                      \'productCode\':                                                                                                                                                                                                
                                                                                                      \'S18_1342\...                                                                                                                                                                                                  

  10103             2003-01-29      2003-02-07         2003-02-02        Shipped      None            \[{\'priceEach\':   Stavern     07-98 9555   None        Norway        4110             81700.0           Erling Skakkes     None               Baane Mini Imports Bergulfsen            Jonas                  1504.0
                                                                                                      214.3,                                                                                                    gate 78                                                                                               
                                                                                                      \'productCode\':                                                                                                                                                                                                
                                                                                                      \'S10_1949\...                                                                                                                                                                                                  

  10104             2003-01-31      2003-02-09         2003-02-01        Shipped      None            \[{\'priceEach\':   Madrid      \(91\) 555   None        Spain         28034            227600.0          C/ Moralzarzal, 86 None               Euro+ Shopping     Freyre                Diego                  1370.0
                                                                                                      131.44,                         94 44                                                                                                           Channel                                                         
                                                                                                      \'productCode\':                                                                                                                                                                                                
                                                                                                      \'S12_314\...                                                                                                                                                                                                   
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#32ed6185-d086-46c3-9254-ade02914d35e .cell .markdown}
2.4. The field `customerName` uniquely identifies each customer. To make
it easy to identify each customer, run the following cell to add a
numerical ID column with each entry associated with the customer\'s
name. (The code below is complete; no change is required).
:::

::: {#5b02c4b0-5224-43bf-be69-d6f6bffc5efe .cell .code}
``` python
df['customerNumber'] = pd.factorize(df['customerName'])[0] + 1
df.head()
```
:::

::: {#563dc3af-cc9c-44d8-b3cd-c9d11699def4 .cell .markdown}
Now that the `customer` field has been transformed into atomic-valued
fields, you need to do the same for the `orderdetails` column.
:::

::: {#4f6be02d-fc5e-47ae-93e8-797c36be26a9 .cell .markdown}
2.5. Create a new row for each product in the same order using the
`pandas` [`explode()`
method](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.explode.html)
on `orderdetails` column of the original `df` DataFrame. Make sure to
set the `ignore_index` parameter to `True`.
:::

::: {#65b3d3da-cdf1-45c5-bf25-9637fb2298d0 .cell .code}
``` python
### START CODE HERE ### (1 line of code)
df_exploded = df.explode('orderdetails', ignore_index=True) # @REPLACE EQUALS df.None('None', ignore_index=None)
### END CODE HERE ###

df_exploded.head()
```
:::

::: {#3c085c73-f21f-4b38-b6ed-e9b77d3fbe2f .cell .markdown}
##### **Expected Output** {#expected-output}

*Note*: Some text is omitted.

  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **ordernumber**   **orderdate**   **requireddate**   **shippeddate**   **status**   **comments**    **orderdetails**   **city**    **phone**    **state**   **country**   **postalCode**   **creditLimit**   **addressLine1**   **addressLine2**   **customerName**   **contactLastName**   **contactFirstName**   **salesRepEmployeeNumber**   **customerNumber**
  ----------------- --------------- ------------------ ----------------- ------------ --------------- ------------------ ----------- ------------ ----------- ------------- ---------------- ----------------- ------------------ ------------------ ------------------ --------------------- ---------------------- ---------------------------- --------------------
  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            {\'priceEach\':    Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1
                                                                                                      136.0,                                                                                                   Avenue                                Creations Co.                                                                                
                                                                                                      \'productCode\':                                                                                                                                                                                                                            
                                                                                                      \'S18_1749\'\...                                                                                                                                                                                                                            

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            {\'priceEach\':    Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1
                                                                                                      55.09,                                                                                                   Avenue                                Creations Co.                                                                                
                                                                                                      \'productCode\':                                                                                                                                                                                                                            
                                                                                                      \'S18_2248\'\...                                                                                                                                                                                                                            

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            {\'priceEach\':    Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1
                                                                                                      75.46,                                                                                                   Avenue                                Creations Co.                                                                                
                                                                                                      \'productCode\':                                                                                                                                                                                                                            
                                                                                                      \'S18_4409\'\...                                                                                                                                                                                                                            

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            {\'priceEach\':    Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1
                                                                                                      35.29,                                                                                                   Avenue                                Creations Co.                                                                                
                                                                                                      \'productCode\':                                                                                                                                                                                                                            
                                                                                                      \'S24_3969\'\...                                                                                                                                                                                                                            

  10101             2003-01-09      2003-01-18         2003-01-11        Shipped      Check on        {\'priceEach\':    Frankfurt   +49 69 66 90 None        Germany       60528            59700.0           Lyonerstr. 34      None               Blauer See Auto,   Keitel                Roland                 1504.0                       2
                                                                                      availability.   108.06,                        2555                                                                                                            Co.                                                                                          
                                                                                                      \'productCode\':                                                                                                                                                                                                                            
                                                                                                      \'S18_2325\'\...                                                                                                                                                                                                                            
  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#12feafa1-bc37-4e84-98f3-79cf836cfc58 .cell .markdown}
The `explode` function is used to transform each element of a list-like
column into a separate row. Setting `ignore_index=True` resets the index
of the resulting dataframe, creating a new integer index that starts
from 0.
:::

::: {#b8ee37d6-be2c-45ec-ba9c-50e9830d017f .cell .markdown}
2.6. Use again the `json_normalize` over the `orderdetails` column of
the dataframe `df_exploded`.
:::

::: {#7cdf75a3-813f-4705-82f2-4a2a5588c30c .cell .code}
``` python
### START CODE HERE ### (1 line of code)
orderdetails_normalized = pd.json_normalize(df_exploded['orderdetails']) # @REPLACE EQUALS pd.None(None['None'])
### END CODE HERE ###

orderdetails_normalized.head()
```
:::

::: {#8317ecd6-2d50-4534-9ffb-85c52c257809 .cell .markdown}
##### **Expected Output** {#expected-output}

  **priceEach**   **productCode**   **quantityOrdered**
  --------------- ----------------- ---------------------
  136.00          S18_1749          30
  55.09           S18_2248          50
  75.46           S18_4409          22
  35.29           S24_3969          49
  108.06          S18_2325          25
:::

::: {#a3b1d3ad .cell .markdown}
2.7. Finally, `drop()` the `orderdetails` column from the original
`df_exploded` DataFrame keeping `inplace` argument equal to `True`. Then
`concat()` `df_exploded` dataframe with the `orderdetails_normalized`
DataFrame.
:::

::: {#316445a5-99e5-4906-9e0e-016b43534219 .cell .code}
``` python
### START CODE HERE ### (2 lines of code)
df_exploded.drop(columns='orderdetails', inplace=True) # @REPLACE df_exploded.None(columns='None', inplace=True)
df_normalized = pd.concat([df_exploded, orderdetails_normalized], axis=1) # @REPLACE EQUALS pd.None([df_exploded, None], axis=1)
### END CODE HERE ###

# First Normal Form
df_normalized.head()
```
:::

::: {#6e49b67f-1412-412b-b015-621482ce9467 .cell .markdown}
##### **Expected Output** {#expected-output}

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **ordernumber**   **orderdate**   **requireddate**   **shippeddate**   **status**   **comments**    **city**    **phone**    **state**   **country**   **postalCode**   **creditLimit**   **addressLine1**   **addressLine2**   **customerName**   **contactLastName**   **contactFirstName**   **salesRepEmployeeNumber**   **customerNumber**   **priceEach**   **productCode**   **quantityOrdered**
  ----------------- --------------- ------------------ ----------------- ------------ --------------- ----------- ------------ ----------- ------------- ---------------- ----------------- ------------------ ------------------ ------------------ --------------------- ---------------------- ---------------------------- -------------------- --------------- ----------------- ---------------------
  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1                    136.00          S18_1749          30
                                                                                                                                                                                            Avenue                                Creations Co.                                                                                                                                       

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1                    55.09           S18_2248          50
                                                                                                                                                                                            Avenue                                Creations Co.                                                                                                                                       

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1                    75.46           S18_4409          22
                                                                                                                                                                                            Avenue                                Creations Co.                                                                                                                                       

  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            Nashua      6035558647   NH          USA           62005            114200.0          2304 Long Airport  None               Online Diecast     Young                 Dorothy                1216.0                       1                    35.29           S24_3969          49
                                                                                                                                                                                            Avenue                                Creations Co.                                                                                                                                       

  10101             2003-01-09      2003-01-18         2003-01-11        Shipped      Check on        Frankfurt   +49 69 66 90 None        Germany       60528            59700.0           Lyonerstr. 34      None               Blauer See Auto,   Keitel                Roland                 1504.0                       2                    108.06          S18_2325          25
                                                                                      availability.               2555                                                                                                            Co.                                                                                                                                                 
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#add7a718-d52b-43bc-8144-7808b4c67b1a .cell .markdown}
The resulting DataFrame `df_normalized` should be in the Frist Normal
Form now.
:::

::: {#f654d61e-309d-4bd2-9217-0d1d893ac8cd .cell .markdown}
2.8. Now that you have atomic values in your dataset, each row
represents a product placed within an order. So to uniquely identify
each row, you need to have a composite primary key which consists of two
columns: the first column represents the order id which is already
provided as `ordernumber`. The second column should identify the product
within an order. So let\'s create an additional column to identify each
product in each order. You will call it Order Line Number
(`orderlinenumber`).
:::

::: {#7f856e7a-c7c0-415c-bb20-f082d406e4ab .cell .code}
``` python
df_normalized['orderlinenumber'] = df_normalized.groupby('ordernumber').cumcount() + 1
df_normalized.head()
```
:::

::: {#210bc34a-1e33-4401-9ad9-73e8746224b5 .cell .markdown}
2.9. With these transformations, you have finished the normalization
process up to 1NF. Let\'s insert this dataset into your database. Run
the following cell to drop the table if it has been loaded before to
avoid an error.
:::

::: {#7c4ccd40-4119-4098-939a-638561c74748 .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_1nf.orders
```
:::

::: {#1bd1d523-020f-4401-a800-7062aec7bc5d .cell .markdown}
Populate the table `orders` under the schema `classicmodels_1nf` with
the data from the DataFrame `df_normalized`.
:::

::: {#3fa4e993-6d50-4736-a00b-6235a4b69fec .cell .code}
``` python
engine = create_engine(connection_url)

df_normalized.to_sql('orders', engine, schema='classicmodels_1nf', index=False)
```
:::

::: {#a302c29d .cell .markdown}
Inspect the data that you just loaded.
:::

::: {#517cc283-ab1e-4a04-8bed-34893682d99a .cell .code}
``` python
%%sql 
SELECT COUNT(*) FROM classicmodels_1nf.orders;
```
:::

::: {#9b345337-b069-4a0e-af02-48a3a8242eb3 .cell .code}
``` python
%%sql 
SELECT * FROM classicmodels_1nf.orders LIMIT 10;
```
:::

::: {#d9d7e094-071a-458a-ba55-4fc8363496fb .cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Second Normal Form (2NF) {#3---second-normal-form-2nf}

For the second normal form (2NF), the requirements of first normal form
must be met and any partial dependencies should be removed. A partial
dependency occurs when there is a subset of non-key columns that depend
on some columns in the composite key.

In the table of the first normal form, the columns that contain
information related to the order such as `orderDate`, `shippedDate`,
`status` and `comments` only depend on the `ordernumber` column of the
composite primary key. Moreover all the columns that contain information
about the customers (`city`, `phone`, `state`, `country`, `postalCode`,
`creditLimit`, `addressLine1`, `addressLine2`, `customerName`,
`customerLastName`, `customerFirstName`, `salesRepEmployeeNumber`,
`customerNumber`) also depend only on the `ordernumber` column. The
remaining columns`priceEach`, `productCode`, `quantityOrdered` require
the two columns of the composite primary key. So, to move from 1NF to
2NF, you will split the table of the first normal form into two tables:

-   `orderdetails` that contains the details of each item placed within
    each order;
-   `orders` that contains information related to the orders, meaning
    that could be uniquely identified using the ordernumber column.

The final schema after the 2NF transformations will look like this:

`<img src="./images/ERD_2NF.png" width="400">`{=html}
:::

::: {#ea61f476-edb9-4be3-9703-158b11a8163d .cell .markdown}
3.1. Create the `classicmodels_2nf` schema and then read the 1NF dataset
and transform it to create the 2NF version.
:::

::: {#d290f76a-4928-4e28-a523-4b0fe4531c0b .cell .code}
``` python
%%sql
CREATE SCHEMA IF NOT EXISTS classicmodels_2nf;
```
:::

::: {#045b751f-4c04-44f1-9d94-60fdbac9cc30 .cell .markdown}
Have a look again at the data in the 1NF:
:::

::: {#605c5ab5-28c4-46b9-92f1-b30d533c44f8 .cell .code}
``` python
result = %sql select * from classicmodels_1nf.orders
df_orders = result.DataFrame()

df_orders.head()
```
:::

::: {#ffcb8c3f .cell .markdown}
3.2. Extract all the information related to `orderdetails` and create a
table with the values for the order details.

-   Take only the orderdetails related columns from your `df_orders`
    dataframe (see the list in the cell below) and make a copy of it
    with the `copy()` method naming it `df_orderdetails`.
:::

::: {#1c3e0fd8 .cell .code}
``` python
orderdetails_columns = ['ordernumber',
                        'orderlinenumber',
                        'priceEach',
                        'productCode',
                        'quantityOrdered'
                       ]

### START CODE HERE ### (1 line of code)
df_orderdetails = df_orders[orderdetails_columns].copy() # @REPLACE EQUALS None[None].None()
### END CODE HERE ###

df_orderdetails.head()
```
:::

::: {#fc191c25-cd01-440e-8d02-6f3f1b503fbb .cell .markdown}
##### **Expected Output** {#expected-output}

  -----------------------------------------------------------------------------------------------
  **ordernumber**   **orderlinenumber**   **priceEach**   **productCode**   **quantityOrdered**
  ----------------- --------------------- --------------- ----------------- ---------------------
  10100             1                     136.00          S18_1749          30

  10100             2                     55.09           S18_2248          50

  10100             3                     75.46           S18_4409          22

  10100             4                     35.29           S24_3969          49

  10101             1                     108.06          S18_2325          25
  -----------------------------------------------------------------------------------------------
:::

::: {#b36798d8-79a2-4982-a5e6-c469ef526075 .cell .markdown}
3.3. Now that you have the orderdetails stored in a separate table, that
information can be dropped from the original DataFrame. The only
necessary column to keep is the `ordernumber` as it helps relate the
orders with orderdetails\' information. Run the following cell to create
a list of the columns you need to drop from the `df_orders` DataFrame:
:::

::: {#b1b53a0f-2ee9-4333-9748-cb5107e12839 .cell .code}
``` python
orderdetails_columns.pop(0)
orderdetails_columns
```
:::

::: {#291cbcfb-739c-4d67-90b0-55585b865844 .cell .markdown}
3.4. Drop the `orderdetails_columns` from the DataFrame `df_orders`. The
`inplace` argument should be equal to `True`. After that, drop the
duplicate rows from `df_orders` using the method `drop_duplicates`. Make
sure that you put argument `inplace` equal to `True`.
:::

::: {#424313b8-603d-473b-a131-87444a3b652f .cell .code}
``` python
### START CODE HERE ### (2 lines of code)
df_orders.drop(columns=orderdetails_columns, inplace=True) # @REPLACE None.None(columns=None, inplace=None)
df_orders.drop_duplicates(inplace=True) # @REPLACE None.None(inplace=None)
### END CODE HERE ###

df_orders.head()
```
:::

::: {#ef92a745-ae01-4acd-ae4d-533fbb1aa9b5 .cell .markdown}
##### **Expected Output** {#expected-output}

  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    **ordernumber** **orderdate**   **requireddate**   **shippeddate**   **status**   **comments**    **city**    **phone**    **state**   **country**     **postalCode**   **creditLimit** **addressLine1**   **addressLine2**   **customerName**   **contactLastName**   **contactFirstName**     **salesRepEmployeeNumber**   **customerNumber**
  ----------------- --------------- ------------------ ----------------- ------------ --------------- ----------- ------------ ----------- ------------- ---------------- ----------------- ------------------ ------------------ ------------------ --------------------- ---------------------- ---------------------------- --------------------
              10100 2003-01-06      2003-01-13         2003-01-10        Shipped      None            Nashua      6035558647   NH          USA                      62005           114,200 2304 Long Airport  None               Online Diecast     Young                 Dorothy                                        1216                    1
                                                                                                                                                                                            Avenue                                Creations Co.                                                                                

              10101 2003-01-09      2003-01-18         2003-01-11        Shipped      Check on        Frankfurt   +49 69 66 90 None        Germany                  60528            59,700 Lyonerstr. 34      None               Blauer See Auto,   Keitel                Roland                                         1504                    2
                                                                                      availability.               2555                                                                                                            Co.                                                                                          

              10102 2003-01-10      2003-01-18         2003-01-14        Shipped      None            NYC         2125551500   NY          USA                      10022            76,400 2678 Kingston Rd.  Suite 101          Vitachrome Inc.    Frick                 Michael                                        1286                    3

              10103 2003-01-29      2003-02-07         2003-02-02        Shipped      None            Stavern     07-98 9555   None        Norway                    4110            81,700 Erling Skakkes     None               Baane Mini Imports Bergulfsen            Jonas                                          1504                    4
                                                                                                                                                                                            gate 78                                                                                                                            

              10104 2003-01-31      2003-02-09         2003-02-01        Shipped      None            Madrid      \(91\) 555   None        Spain                    28034           227,600 C/ Moralzarzal, 86 None               Euro+ Shopping     Freyre                Diego                                          1370                    5
                                                                                                                  94 44                                                                                                           Channel                                                                                      
  -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#555c4012-2c04-4e72-aaac-aa4b5f22be1c .cell .markdown}
3.5. The two tables you just created can be related through the
`ordernumber` field. Insert the two tables into the `classicmodels_2nf`
schema. You will need to drop the tables before that in case they have
been added before.
:::

::: {#29b3dff6-31da-47ed-943d-91963c18181c .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_2nf.orders
```
:::

::: {#1b9fe00f-f5fa-4d04-b8d7-7bcb320c6fa0 .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_2nf.orderdetails
```
:::

::: {#1f527026-22f8-411a-bb9e-74f1ba7a74e4 .cell .code}
``` python
df_orders.to_sql('orders', engine, schema='classicmodels_2nf', index=False)
```
:::

::: {#deaffa6a-38f3-4f6f-a701-f9f7e18e4493 .cell .code}
``` python
df_orderdetails.to_sql('orderdetails', engine, schema='classicmodels_2nf', index=False)
```
:::

::: {#d3c12a44-aae7-4387-9a55-c1f92399065e .cell .markdown}
Explore the tables.
:::

::: {#be780032 .cell .code}
``` python
%%sql
SELECT COUNT(*) FROM classicmodels_2nf.orders;
```
:::

::: {#7d8c6142 .cell .code}
``` python
%%sql
SELECT COUNT(*) FROM classicmodels_2nf.orderdetails;
```
:::

::: {#dbf3d383 .cell .code}
``` python
%%sql
SELECT * FROM classicmodels_2nf.orderdetails limit 10;
```
:::

::: {#f0056649 .cell .code}
``` python
%%sql
SELECT * FROM classicmodels_2nf.orders limit 10;
```
:::

::: {#dfddeb4b-e0d9-4294-917e-b240ef6a5112 .cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - Third Normal Form (3NF) {#4---third-normal-form-3nf}

The features of 3NF are the following:

-   Already in 2NF: The table must already be in Second Normal Form
    (2NF).
-   No Transitive Dependencies: There should be no transitive
    dependencies. A transitive dependency occurs when a non-key column
    depends on another non-key column.
:::

::: {#93bd2acf-dad9-4f7b-852a-b1187d854623 .cell .markdown}
In the `orders` table, you have the following columns: `phone`, `state`,
`country`, `postalCode`, `creditLimit`, `addressLine1`, `addressLine2`,
`customerName`, `customerLastName`, `customerFirstName`, and
`salesRepEmployeeNumber`. All of these columns are non-key columns that
depends on the non-key column `customerNumber`.

So to transform the 2NF form into 3NF form, you need to remove these
columns from the `orders` table and create a new table that contains the
details of each customer.
:::

::: {#ae51126c-ddc7-4ed5-9d2a-c34da032c06a .cell .markdown}
The final schema of the 3NF normalization is the following:

`<img src="./images/ERD_3NF.png" width="600">`{=html}
:::

::: {#af65b87f-8849-41ab-86be-19904f4161ec .cell .markdown}
4.1. Create the `classicmodels_3nf` schema to store your transformed
tables there.
:::

::: {#6592e27e-fb86-415e-9676-d1587524efed .cell .code}
``` python
%%sql
CREATE SCHEMA IF NOT EXISTS classicmodels_3nf;
```
:::

::: {#581b27df .cell .markdown}
4.2. You will need to read the `orders` and `orderdetails` tables from
the `classicmodels_2nf` schema. Although you will not make any further
changes to the `orderdetails` table in this step, you will upload it
into the 3NF schema to keep all your datasets in the same place.

Read the `orders` table into the `df_orders` pandas DataFrame:
:::

::: {#1a78587b-646e-4b81-bdef-d55c752be7dc .cell .code}
``` python
result = %sql select * from classicmodels_2nf.orders
df_orders = result.DataFrame()

df_orders.head()
```
:::

::: {#f30710ac .cell .markdown}
Read the `orderdetails` table into `df_orderdetails`:
:::

::: {#f84f1ffc-d9b4-4756-8c1a-01e67dda3ffc .cell .code}
``` python
result = %sql select * from classicmodels_2nf.orderdetails
df_orderdetails = result.DataFrame()

df_orderdetails.head()
```
:::

::: {#3641dbbf .cell .markdown}
4.3. To create the customers table, extract only the customer-related
columns from your `df_orders` DataFrame (see the list in the cell below)
and make a copy of it with the `copy()` method naming it `df_customers`.
After that drop the duplicate rows from `df_customers` using the method
`drop_duplicates`. Make sure that you put argument `inplace` equal to
`True`.
:::

::: {#9bb6ff9e-2bfe-4e03-b31a-b4a145992235 .cell .code}
``` python
customer_columns = ['customerNumber', 
                    'customerName', 
                    'contactLastName', 
                    'contactFirstName', 
                    'phone', 
                    'addressLine1', 
                    'addressLine2',
                    'postalCode',                     
                    'city', 
                    'state', 
                    'country', 
                    'creditLimit',
                    'salesRepEmployeeNumber'
                   ] 

### START CODE HERE ### (2 lines of code)
df_customers = df_orders[customer_columns].copy() # @REPLACE EQUALS None[None].None()
df_customers.drop_duplicates(inplace=True) # @REPLACE None.None(inplace=None)
### END CODE HERE ###

df_customers.head()
```
:::

::: {#a0839bb5-b0e0-4205-87a4-c65e9ed20ed5 .cell .markdown}
##### **Expected Output** {#expected-output}

  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  **customerNumber**   **customerName**   **contactLastName**   **contactFirstName**   **phone**    **addressLine1**   **addressLine2**   **postalCode**   **city**    **state**   **country**   **creditLimit**   **salesRepEmployeeNumber**
  -------------------- ------------------ --------------------- ---------------------- ------------ ------------------ ------------------ ---------------- ----------- ----------- ------------- ----------------- ----------------------------
  1                    Online Diecast     Young                 Dorothy                6035558647   2304 Long Airport  None               62005            Nashua      NH          USA           114200.0          1216.0
                       Creations Co.                                                                Avenue                                                                                                         

  2                    Blauer See Auto,   Keitel                Roland                 +49 69 66 90 Lyonerstr. 34      None               60528            Frankfurt   None        Germany       59700.0           1504.0
                       Co.                                                             2555                                                                                                                        

  3                    Vitachrome Inc.    Frick                 Michael                2125551500   2678 Kingston Rd.  Suite 101          10022            NYC         NY          USA           76400.0           1286.0

  4                    Baane Mini Imports Bergulfsen            Jonas                  07-98 9555   Erling Skakkes     None               4110             Stavern     None        Norway        81700.0           1504.0
                                                                                                    gate 78                                                                                                        

  5                    Euro+ Shopping     Freyre                Diego                  \(91\) 555   C/ Moralzarzal, 86 None               28034            Madrid      None        Spain         227600.0          1370.0
                       Channel                                                         94 44                                                                                                                       
  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
:::

::: {#64a8b8ca .cell .markdown}
4.4. Create a list of the columns which you need to drop from the
`df_orders` DataFrame - the ones associated with the `customers` table.
You will keep the `customerNumber` field as it is the way to relate the
two tables:
:::

::: {#bea53417-f809-489f-8329-e658f14d90cc .cell .code}
``` python
customer_columns.pop(0)
customer_columns
```
:::

::: {#46932004-3357-498a-9ee0-7435f6a046d7 .cell .markdown}
4.5. Drop the columns specified in the list `customer_columns` from the
`df_orders` DataFrame. You should keep `inplace` argument equal to
`True`.
:::

::: {#e073340d-9ac2-4b74-a42e-56971a059d69 .cell .code}
``` python
### START CODE HERE ### (1 line of code)
df_orders.drop(columns=customer_columns, inplace=True) # @REPLACE None.None(columns=None, inplace=None)
### END CODE HERE ###

df_orders.head()
```
:::

::: {#3e0a71c1-2525-4acd-ba26-4224d2dc8477 .cell .markdown}
##### **Expected Output** {#expected-output}

  ------------------------------------------------------------------------------------------------------------------------
  **ordernumber**   **orderdate**   **requireddate**   **shippeddate**   **status**   **comments**    **customerNumber**
  ----------------- --------------- ------------------ ----------------- ------------ --------------- --------------------
  10100             2003-01-06      2003-01-13         2003-01-10        Shipped      None            1

  10101             2003-01-09      2003-01-18         2003-01-11        Shipped      Check on        2
                                                                                      availability.   

  10102             2003-01-10      2003-01-18         2003-01-14        Shipped      None            3

  10103             2003-01-29      2003-02-07         2003-02-02        Shipped      None            4

  10104             2003-01-31      2003-02-09         2003-02-01        Shipped      None            5
  ------------------------------------------------------------------------------------------------------------------------
:::

::: {#911962f6-d87d-489a-a405-ef36d3d37faa .cell .markdown}
Great! With those transformations you have achieved a 3NF from your
initial OBT.
:::

::: {#5b028ec2-adb3-4fb5-942b-78b73c3fefb2 .cell .markdown}
4.6. Let\'s upload the data into the `classicmodels_3nf` schema.
Remember that you have three tables: `customers`, `orders` and
`orderdetails`.
:::

::: {#ebc242fc-a042-48aa-8641-8e9394c95f73 .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_3nf.customers
```
:::

::: {#58cfc225-6e90-4c01-b2a4-87c2779f508b .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_3nf.orders
```
:::

::: {#c6812b24-ca2c-457d-bd7b-34b31ee3fe76 .cell .code}
``` python
%%sql
DROP TABLE IF EXISTS classicmodels_3nf.orderdetails
```
:::

::: {#97aefd68-aca1-4a74-a58b-f66f338f260d .cell .code}
``` python
df_customers.to_sql('customers', engine, schema='classicmodels_3nf', index=False)
```
:::

::: {#943847f4-5fe4-40ac-952f-bb6b3277ed8f .cell .code}
``` python
df_orders.to_sql('orders', engine, schema='classicmodels_3nf', index=False)
```
:::

::: {#a86a3b8f-7a85-47ae-9690-f77328963b4b .cell .code}
``` python
df_orderdetails.to_sql('orderdetails', engine, schema='classicmodels_3nf', index=False)
```
:::

::: {#70616129 .cell .markdown}
Finally, you can take a look to each of the tables that have been stored
in your database.
:::

::: {#e08b6823-d5c7-49c8-ac4d-1f8036ee1fe7 .cell .code}
``` python
%%sql
select count(*) from classicmodels_3nf.customers;
```
:::

::: {#caddf56c-94f1-4a00-8827-df9905cde210 .cell .code}
``` python
%%sql
select count(*) from classicmodels_3nf.orders;
```
:::

::: {#c63163ee-c5f9-4717-befd-dd814497434a .cell .code}
``` python
%%sql
select count(*) from classicmodels_3nf.orderdetails;
```
:::

::: {#c21aa10a-f5cb-4c49-86dd-1e2f10a5797c .cell .markdown}
In this lab, you have successfully transformed a One Big Table (OBT)
into First Normal Form (1NF), Second Normal Form (2NF), and Third Normal
Form (3NF). This process of normalization is crucial in designing
efficient and reliable databases.

Throughout the lab, you have learned the importance of each normal form
and the specific steps required to achieve them:

-   1NF: Ensuring that each table cell contains only atomic
    (indivisible) values and each record is unique.
-   2NF: Building on 1NF by removing partial dependencies, ensuring that
    non-key attributes are fully dependent on the composite primary key.
-   3NF: Further refining the table structure by removing transitive
    dependencies, ensuring that non-key attributes are dependent only on
    the primary key.

This lab has provided you with hands-on experience in transforming a
dataset into a normalized form, highlighting the practical steps and
considerations involved in database normalization. As you progress in
your work as a Data Engineer, these skills will be invaluable in
ensuring the quality and efficiency of the databases you design and
maintain.
:::

::: {#35cb1f8c-c1d3-4efe-8a69-85eefa283646 .cell .code}
``` python
```
:::
