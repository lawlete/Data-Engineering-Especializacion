::: {.cell .markdown}
# Modeling and Transforming Text Data for ML

In this assignment, you will work with the Amazon Reviews dataset to
generate a training dataset for an ML use case. The purpose of this lab
is to apply feature engineering to process the numerical and categorical
features of the raw JSON files, and to transform the text reviews and
product information into text embeddings. You will finally store the
generated features in a provided Postgres database, which will serve as
a vector database.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Introduction](#1)
-   [2 - Source Data Exploration](#2)
-   [3 - Creating Functions to Process Data](#3)
    -   [3.1 - Process the Reviews Dataset](#3-1)
    -   [3.2 - Process the Metatada Dataset](#3-2)
    -   [3.3 - Process the Textual Features](#3-3)
    -   [3.4 - Process the Numerical Features](#3-4)
    -   [3.5 - Process the Categorical Features](#3-5)
-   [4 - Split Data and Create Text Embeddings](#4)
    -   [4.1 - Split Data](#4-1)
    -   [4.2 - Create Text Embeddings](#4-2)
:::

::: {.cell .markdown}
Load the required libraries:
:::

::: {.cell .code execution_count="1"}
``` python
import re
import datetime as dt
import gzip
import json
import math
import requests
import time
import os
import subprocess
from typing import Dict, List, Union


import boto3
import numpy as np
import pandas as pd
import psycopg2 
import smart_open

from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector
from IPython.display import HTML
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, StandardScaler, KBinsDiscretizer

pd.set_option('display.max_columns', 30)
```
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Introduction {#1---introduction}

Imagine you are employed as a Data Engineer at a prominent e-commerce
retailer. The Machine Learning (ML) team has initiated a new research
project and obtained a dataset comprising Amazon Reviews for different
products. They have requested you to build a pipeline to refine the raw
JSON data into structured datasets suitable for training ML models. To
start the development, they have provided you with two sample files from
the original dataset to validate the logic and develop an initial
pipeline prototype within this notebook. Additionally, the Data
Analytics team has requested that you generate embeddings from the
reviews and product texts and then store the vectors in a vector
database for future analysis; for this purpose, the ML team has enabled
an API that runs a text embedder ML model for you to consume and
generate the vectors.

The main requirements regarding the datasets are the following:

1.  Process the textual, categorical and numerical features.
2.  Generate text embeddings based on the review text and product
    information (provided from the product description or the product
    title)
3.  Divide the original data into three tables:
    -   Reviews embeddings dataset: it must contain the reviewer ID,
        product ASIN, the review text and the corresponding embedding
        vector.
    -   Product embeddings dataset: it must contain the product ASIN,
        the product information and the corresponding embedding vector.
    -   Review metadata dataset: it must contain the remaining features
        related to the reviews and products for each review from the
        original data.
4.  Store the new features in the provisioned RDS Postgres instance.
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - Source Data Exploration {#2---source-data-exploration}

The dataset is comprised of two compressed JSON files, one with the
reviews and one with the metadata of the reviewed products. You\'ve
already worked with this dataset in C3W2 lab. Here is an example of a
review:

``` json
{
  "reviewerID": "A2SUAM1J3GNN3B",
  "asin": "0000013714",
  "reviewerName": "J. McDonald",
  "helpful": [2, 3],
  "reviewText": "I bought this for my husband who plays the piano.  He is having a wonderful time playing these old hymns.  The music  is at times hard to read because we think the book was published for singing from more than playing from.  Great purchase though!",
  "overall": 5.0,
  "summary": "Heavenly Highway Hymns",
  "unixReviewTime": 1252800000,
  "reviewTime": "09 13, 2009"
}
```

Here is the description of the fields:

-   `reviewerID` - ID of the reviewer, e.g. A2SUAM1J3GNN3B
-   `asin` - ID of the product, e.g. 0000013714
-   `reviewerName` - name of the reviewer
-   `helpful` - helpfulness rating of the review, e.g. 2/3
-   `reviewText` - text of the review
-   `overall` - rating of the product
-   `summary` - summary of the review
-   `unixReviewTime` - time of the review (unix time)
-   `reviewTime` - time of the review (raw)

And this is an example of the review metadata:

``` json
{
  "asin": "0641843224",
  "description": "Set your phasers to stun and prepare for a warp speed ride through the most memorable vocabulary from the sci-fi/fantasy genre.",
  "title": "McNeill Designs YBS Sci-fi/Fantasy Add-on Deck", 
  "price": 5.19,  
  "imUrl": "http://ecx.images-amazon.com/images/I/418t9AN9hiL._SY300_.jpg", 
  "related": 
  {
    "also_bought": ["B000EVLZ9U", "0641843208", "0641843216", "0641843267", "1450751210", "0641843232", "B00ALQFYGI", "B004G7B3NQ", "B002PDM288", "B009ZNJZV8", "B009YG928W", "B0063NC3N0"], 
    "also_viewed": ["B000EVLZ9U", "1450751210", "0641843208", "0641843267", "0641843232", "0641843216", "B003EIK136", "B004G7B3NQ", "B003N2Q5JC"], 
    "bought_together": ["B000EVLZ9U"]
  },
  "salesRank": {"Toys & Games": 154868}, 
  "brand": "McNeill Designs", 
  "categories": [["Toys & Games", "Games", "Card Games"]]
}
```

With the following fields:

-   `asin` - ID of the product, e.g. 0000031852
-   `description` - Description of the product
-   `title` - name of the product
-   `price` - price in US dollars (at time of crawl)
-   `imUrl` - url of the product image
-   `related` - related products (`also_bought`, `also_viewed`,
    `bought_together`)
-   `salesRank` - sales rank information
-   `brand` - brand name
-   `categories` - list of categories the product belongs to
:::

::: {.cell .markdown}
2.1. Run the following code to get the link to the AWS console.

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
<a href="https://signin.aws.amazon.com/federation?Action=login&SigninToken=DjwS1LZHJL178P9UEedDvkohg-P9Xz7IJ6etc7hovKz1TbHb03oLpuH0qrg703DZ7SMjb8qf1kQIEyW6ondMtYC_R78geCCSL-er7czVD8bWbixcqJCsg9JyS63HPLcYKBWUv7lXLNv8iEu8biRVdcSeJgLVr0q9G0cEZX40U1zfiZraE0HQp6GWe7PkYVLWWML1dbj4sKynubTpp4Q2LHb9dhYxZVfgg0mJcT0Qe3ypocPQjuEIs-78QMxTcJZYMkR20oxSDGxm3Bbd6U2NT9pPKlAStCD7wRX4DV6FP7T8FIZ89IqPAO2GduSi3XZvtp0ME2S4E--9e7ymyLffGQQODd1cCALlzkZkh8wn38u8dRGqM6Cjs7S_XeHS8Zmd2Xr0ZMsIKRphKrUIqsnHhgkfgN7huyXuhfqnCIrS0Wt5zaEFL9fF1CSlfQIV4Cdb07hsW2YQTCqiefiO4u1DwOz395TdN9kCfErKJZk_I3W4Rt0GhMbW0T483qYx9lin2fF_3tYpsZ3w4lVsdzsBB0TVrFHBAND6k1HqoMOnGdxbJDp_VergsZ6FPUZJJf25VekBbrmZ8RpRSXZzApH0lmFLHMgYXjVFBCSG4SsLed4ULcvG1U_q1rY1DpDbwK7soARMFAxjmKH2S-HbC8iyHF6gKG3zneXQfYqPEApMaHZ7dWKe30BzLk8QJujw_sUiUjwlzMqw1g7fqcgiyJuyok_ttAbC2KILKgnjdzU5woT0Y5lBvXYKe6lOsuu1ZlsjCWFBQ0bdEkewNrEZZcjwhJ3ygT1_Qy4dvhDs8wIsThw9as53ER6izFEskuQTV0BrQCDNGOS8qo9NaoijoVQP2998Rroj5vBpP4-RnC3Y23CAk8B-HMCQe_E0LSUbI6UC3-ZhCLVEMLRJj9o3QaeVPauHWn97xmS6FhrCOK_PFK3SSCtJ6ORxCEKMHerfitx4ODPGE5S6-_JcX3jHvxIuURNb22dQWtWbF9OziW4G_cmiIAWDSaxUWPPXJ9X9vr42G1aZAOWdbWBm31XmKjx1kWw3LCThjf7-7tDQNHfjYKSzD_RuCTBQLBD8AvSSiEagvFmTseGURfw86sXUXxD9KDJI8bE2_lNv4Bv23qT-jUbsINlih7cDOtf6TSaXw2CV_Keu3wQ0Miq0m6AnueDA9ynEiRVRhT1ikEbm4fmfRWQ7Wj05YeZzA7EwQKF2mpwG2ozDwa16p9cneip3ne6tUnKaF_cVLYiCuUj0o5wdtBRzWoLyn5SUdCzMy9Q8Cp-dDF7dxLBosTbB23AERY12khojzI--d5_jCBJ6sHOhuLtKQ2czGylwzETgob-bQsD2xXuq8H8UqYGSImjCwhCME5wVsvJBbMF1LkE8E3NQFQ6mUDzN3TLEix1-P0dQitDETVLdG1JdRLi--B4f-EI44TBhql4uCzzGytS2smM&Issuer=https%3A%2F%2Fapi.vocareum.com&Destination=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome%3Fregion%3Dus-east-1" target="_blank">GO TO AWS CONSOLE</a>
:::
::::

::: {.cell .markdown}
*Note:* If you see the window like in the following printscreen, click
on **logout** link, close the window and click on console link again.

![AWSLogout](images/AWSLogout.png)
:::

::: {.cell .markdown}
The ML team has provided the dataset JSON files in an S3 bucket. In the
code below, set the variable `BUCKET_NAME`.

Go to **CloudFormation** in the AWS console. Click on the alphanumeric
stack name and search for the **Outputs** tab. You will see the key
`MlModelDNS`, copy the corresponding **Value** and replace with it the
placeholder `<ML_MODEL_ENDPOINT_URL>` in the cell below. Please, be
careful not to remove the `http://` part and only replace the
placeholder with the value you get from CloudFormation.
:::

::: {.cell .code execution_count="34"}
``` python
AWS_ACCOUNT_ID = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'], capture_output=True, text=True).stdout.strip()
BUCKET_NAME = f'de-c4w2a1-{AWS_ACCOUNT_ID}-us-east-1-data-lake'
ENDPOINT_URL = 'http://ec2-100-48-140-231.compute-1.amazonaws.com/'
```
:::

::: {.cell .markdown}
2.2. Open the `./src/env` file and replace the placeholder
`<RDS-ENDPOINT>` with the `PostgresEndpoint` value from the
CloudFormation outputs. Save changes. Run the following cell to load the
connection credentials that will be used later.
:::

::: {.cell .code execution_count="38"}
``` python
load_dotenv('./src/env', override=True)

DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')
DBNAME = os.getenv('DBNAME')
DBUSER = os.getenv('DBUSER')
DBPASSWORD = os.getenv('DBPASSWORD')
```
:::

::: {.cell .markdown}
2.3. Explore the samples of the dataset. You are provided with a
function to load the data into memory and explore it. Take a look at the
function defined below:
:::

::: {.cell .code execution_count="5"}
``` python
def read_data_sample(bucket_name: str, s3_file_key: str) -> pd.DataFrame:
    """Reads review sample dataset

    Args:
        bucket_name (str): Bucket name
        s3_file_key (str): Dataset s3 key location

    Returns:
        pd.DataFrame: Read dataframe
    """
    s3_client = boto3.client('s3')
    source_uri = f's3://{bucket_name}/{s3_file_key}'
    json_list = []
    for json_line in smart_open.open(source_uri, transport_params={'client': s3_client}):
        json_list.append(json.loads(json_line))
    df = pd.DataFrame(json_list)
    return df
```
:::

::: {.cell .markdown}
2.3.4. Start reading the sample datasets with the `read_data_sample()`
method. The key for the main reviews file is
`'staging/reviews_Toys_and_Games_sample.json.gz'` and the one for
metadata is `'staging/meta_Toys_and_Games_sample.json.gz'`. Also, assign
the `bucket_name` values to `BUCKET_NAME`.
:::

:::: {.cell .code execution_count="6" exercise="[\"ex01\"]" tags="[\"graded\"]"}
``` python
### START CODE HERE ### (2 lines of code)
review_sample_df = read_data_sample(bucket_name=BUCKET_NAME, s3_file_key='staging/reviews_Toys_and_Games_sample.json.gz')
metadata_sample_df = read_data_sample(bucket_name=BUCKET_NAME, s3_file_key='staging/meta_Toys_and_Games_sample.json.gz')
### END CODE HERE ###

review_sample_df.head()
```

::: {.output .execute_result execution_count="6"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerID</th>
      <th>asin</th>
      <th>reviewerName</th>
      <th>helpful</th>
      <th>reviewText</th>
      <th>overall</th>
      <th>summary</th>
      <th>unixReviewTime</th>
      <th>reviewTime</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>Nicole Soeder</td>
      <td>[0, 0]</td>
      <td>Great product, thank you! Our son loved the pu...</td>
      <td>5.0</td>
      <td>Puzzles</td>
      <td>1388016000</td>
      <td>12 26, 2013</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A3C9CSW3TJITGT</td>
      <td>0005069491</td>
      <td>Renee</td>
      <td>[0, 0]</td>
      <td>I love these felt nursery rhyme characters and...</td>
      <td>4.0</td>
      <td>Charming characters but busy work required</td>
      <td>1377561600</td>
      <td>08 27, 2013</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A31POTIYCKSZ9G</td>
      <td>0076561046</td>
      <td>So CA Teacher</td>
      <td>[0, 0]</td>
      <td>I see no directions for its use. Therefore I h...</td>
      <td>3.0</td>
      <td>No directions for use...</td>
      <td>1404864000</td>
      <td>07 9, 2014</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>Dalilah G.</td>
      <td>[0, 0]</td>
      <td>This is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>Great CD-ROM</td>
      <td>1382400000</td>
      <td>10 22, 2013</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>Dayna English</td>
      <td>[0, 0]</td>
      <td>Although not as streamlined as the Algebra I m...</td>
      <td>5.0</td>
      <td>Algebra II -- presentation materials</td>
      <td>1374278400</td>
      <td>07 20, 2013</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

:::: {.cell .code execution_count="7" exercise="[\"ex02\"]" tags="[\"graded\"]"}
``` python
metadata_sample_df.head()
```

::: {.output .execute_result execution_count="7"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>asin</th>
      <th>description</th>
      <th>title</th>
      <th>price</th>
      <th>salesRank</th>
      <th>imUrl</th>
      <th>brand</th>
      <th>categories</th>
      <th>related</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0000191639</td>
      <td>Three Dr. Suess' Puzzles: Green Eggs and Ham, ...</td>
      <td>Dr. Suess 19163 Dr. Seuss Puzzle 3 Pack Bundle</td>
      <td>37.12</td>
      <td>{'Toys &amp; Games': 612379}</td>
      <td>http://ecx.images-amazon.com/images/I/414PLROX...</td>
      <td>Dr. Seuss</td>
      <td>[[Toys &amp; Games, Puzzles, Jigsaw Puzzles]]</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0005069491</td>
      <td>NaN</td>
      <td>Nursery Rhymes Felt Book</td>
      <td>NaN</td>
      <td>{'Toys &amp; Games': 576683}</td>
      <td>http://ecx.images-amazon.com/images/I/51z4JDBC...</td>
      <td>NaN</td>
      <td>[[Toys &amp; Games]]</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0076561046</td>
      <td>Learn Fractions Decimals Percents using flash ...</td>
      <td>Fraction Decimal Percent Card Deck</td>
      <td>NaN</td>
      <td>{'Toys &amp; Games': 564211}</td>
      <td>http://ecx.images-amazon.com/images/I/51ObabPu...</td>
      <td>NaN</td>
      <td>[[Toys &amp; Games, Learning &amp; Education, Flash Ca...</td>
      <td>{'also_viewed': ['0075728680']}</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0131358936</td>
      <td>New, Sealed. Fast Shipping with tracking, buy ...</td>
      <td>NaN</td>
      <td>36.22</td>
      <td>{'Software': 8080}</td>
      <td>http://ecx.images-amazon.com/images/I/51%2B7Ej...</td>
      <td>NaN</td>
      <td>[[Toys &amp; Games, Learning &amp; Education, Mathemat...</td>
      <td>{'also_bought': ['0321845536', '0078787572'], ...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0133642984</td>
      <td>NaN</td>
      <td>Algebra 2 California Teacher Center</td>
      <td>731.93</td>
      <td>{'Toys &amp; Games': 1150291}</td>
      <td>http://ecx.images-amazon.com/images/I/51VK%2BL...</td>
      <td>Prentice Hall</td>
      <td>[[Toys &amp; Games, Learning &amp; Education, Mathemat...</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
In course 3, you created some functions to perform some processing over
the reviews and metadata datasets. Let\'s recreate those functions.
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Creating Functions to Process Data {#3---creating-functions-to-process-data}
:::

::: {.cell .markdown}
`<a id='3-1'>`{=html}`</a>`{=html}

### 3.1 - Process the Reviews Dataset {#31---process-the-reviews-dataset}

Complete the `process_review()` function to perform some
transformations. Please follow the instructions in the code.
:::

:::: {.cell .code execution_count="8" exercise="[\"ex03\"]" tags="[\"graded\"]"}
``` python
def process_review(raw_df: pd.DataFrame) -> pd.DataFrame:    
    """Transformations steps for Reviews dataset

    Args:
        raw_df (DataFrame): Raw data loaded in dataframe

    Returns:
        DataFrame: Returned transformed dataframe
    """

    ### START CODE HERE ### (5 lines of code)
    
    # Convert the `unixReviewTime` column of the dataframe `raw_df` to date with the `to_datetime()` function
    # The timestamp is defined in seconds (use `s` for the `unit` parameter)
    raw_df['reviewTime'] = pd.to_datetime(raw_df['unixReviewTime'], unit='s')

    # Extract the year and month from the `reviewTime`, and save those values in new columns named `year` and `month`
    # You can apply `.dt.year` and `.dt.month` methods to `raw_df['reviewTime']` to do that
    raw_df['year'] = raw_df['reviewTime'].dt.year
    raw_df['month'] = raw_df['reviewTime'].dt.month

    # Create a new dataframe based on converting the `helpful` column from the `raw_df` into a list with the `to_list()` method
    # Set the column names as `helpful_votes` and `total_votes`
    df_helpful = pd.DataFrame(raw_df['helpful'].to_list(), columns=['helpful_votes', 'total_votes'])

    # With the `pd.concat()` function, concatenate the `raw_df` dataframe with `df_helpful`
    # Make sure that you drop the `helpful` column from `raw_df` with the `drop()` method and  set `axis` equal to 1
    target_df = pd.concat([raw_df.drop(columns=['helpful']), df_helpful], axis=1)
    
    ### END CODE HERE ###
    
    target_df['not_helpful_votes'] = target_df['total_votes'] - target_df['helpful_votes']
    
    return target_df

transformed_review_sample_df = process_review(raw_df=review_sample_df)
transformed_review_sample_df.head(2)
```

::: {.output .execute_result execution_count="8"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerID</th>
      <th>asin</th>
      <th>reviewerName</th>
      <th>reviewText</th>
      <th>overall</th>
      <th>summary</th>
      <th>unixReviewTime</th>
      <th>reviewTime</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>Nicole Soeder</td>
      <td>Great product, thank you! Our son loved the pu...</td>
      <td>5.0</td>
      <td>Puzzles</td>
      <td>1388016000</td>
      <td>2013-12-26</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A3C9CSW3TJITGT</td>
      <td>0005069491</td>
      <td>Renee</td>
      <td>I love these felt nursery rhyme characters and...</td>
      <td>4.0</td>
      <td>Charming characters but busy work required</td>
      <td>1377561600</td>
      <td>2013-08-27</td>
      <td>2013</td>
      <td>8</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
`<a id='3-2'>`{=html}`</a>`{=html}

### 3.2 - Process the Metatada Dataset {#32---process-the-metatada-dataset}

3.2.1. Let\'s perform some small transformations over the metadata
dataset. Follow the instructions in the code to complete the
`process_metadata()` function.
:::

:::: {.cell .code execution_count="9" exercise="[\"ex04\"]" tags="[\"graded\"]"}
``` python
def process_metadata(raw_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Function in charge of the transformation of the raw data of the
    Reviews Metadata.

    Args:
        raw_df (DataFrame): Raw data loaded in dataframe
        cols (list): List of columns to select
        cols_to_clean (list): List of columns 

    Returns:
        DataFrame: Returned transformed dataframe
    """

    ### START CODE HERE ### (6 lines of code)
    
    # Remove any records that have null values for the `salesRank` column. You should apply `dropna()` method to the dataframe `raw_df`
    # The value of the parameter `how` should be equal to `"any"`
    tmp_df = raw_df.dropna(subset=["salesRank"], how="any")

    # Extract the sales rank and category from the `salesRank` column into two new columns: `sales_category` as key and `sales_rank` as value
    df_rank = pd.DataFrame([{"sales_category": key, "sales_rank": value} for d in tmp_df["salesRank"].tolist() for key, value in d.items()])

    # Concatenate dataframes `tmp_df` and `df_rank`
    target_df = pd.concat([tmp_df, df_rank], axis=1)

    # Select the columns that don't contain arrays and the new columns (this line is complete)
    target_df = target_df[cols] 
    

    # Remove any record that has null values for the `asin`, `price` and `sales_rank` column
    # You should use `dropna()` method and the value of the parameter `how` should be equal to `"any"`
    target_df = target_df.dropna(subset=["asin", "price", "sales_rank"], how="any")

    # Fill the null values of the rest of the Dataframe with an empty string `""`. Use `fillna()` method to do that
    target_df = target_df.fillna("")
    
    ### END CODE HERE ###
    
    return target_df

processed_metadata_df = process_metadata(raw_df=metadata_sample_df, 
                                         cols=['asin', 'description', 'title', 'price', 'brand','sales_category','sales_rank']
                                         )
processed_metadata_df.head()
```

::: {.output .execute_result execution_count="9"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>asin</th>
      <th>description</th>
      <th>title</th>
      <th>price</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>sales_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0000191639</td>
      <td>Three Dr. Suess' Puzzles: Green Eggs and Ham, ...</td>
      <td>Dr. Suess 19163 Dr. Seuss Puzzle 3 Pack Bundle</td>
      <td>37.12</td>
      <td>Dr. Seuss</td>
      <td>Toys &amp; Games</td>
      <td>612379.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0131358936</td>
      <td>New, Sealed. Fast Shipping with tracking, buy ...</td>
      <td></td>
      <td>36.22</td>
      <td></td>
      <td>Software</td>
      <td>8080.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0133642984</td>
      <td></td>
      <td>Algebra 2 California Teacher Center</td>
      <td>731.93</td>
      <td>Prentice Hall</td>
      <td>Toys &amp; Games</td>
      <td>1150291.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>0375829695</td>
      <td>A collection of six 48-piece (that is,slightly...</td>
      <td>Dr. Seuss Jigsaw Puzzle Book: With Six 48-Piec...</td>
      <td>24.82</td>
      <td>Dr. Seuss</td>
      <td>Home &amp;amp; Kitchen</td>
      <td>590975.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>0439400066</td>
      <td>Send your mind into overdrive with this mind-b...</td>
      <td>3D Puzzle Buster</td>
      <td>99.15</td>
      <td></td>
      <td>Toys &amp; Games</td>
      <td>1616332.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.2.2. Once you have your data with some initial preprocessing, you have
to join the data according to the ID of each product (`asin`). Do an
inner join in this case as you are interested only in reviews for which
you have product information.
:::

:::: {.cell .code execution_count="10"}
``` python
reviews_product_metadata_df = transformed_review_sample_df.merge(processed_metadata_df, left_on='asin', right_on='asin', how='inner')
reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="10"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerID</th>
      <th>asin</th>
      <th>reviewerName</th>
      <th>reviewText</th>
      <th>overall</th>
      <th>summary</th>
      <th>unixReviewTime</th>
      <th>reviewTime</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>description</th>
      <th>title</th>
      <th>price</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>sales_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>Nicole Soeder</td>
      <td>Great product, thank you! Our son loved the pu...</td>
      <td>5.0</td>
      <td>Puzzles</td>
      <td>1388016000</td>
      <td>2013-12-26</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Three Dr. Suess' Puzzles: Green Eggs and Ham, ...</td>
      <td>Dr. Suess 19163 Dr. Seuss Puzzle 3 Pack Bundle</td>
      <td>37.12</td>
      <td>Dr. Seuss</td>
      <td>Toys &amp; Games</td>
      <td>612379.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>Dalilah G.</td>
      <td>This is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>Great CD-ROM</td>
      <td>1382400000</td>
      <td>2013-10-22</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>New, Sealed. Fast Shipping with tracking, buy ...</td>
      <td></td>
      <td>36.22</td>
      <td></td>
      <td>Software</td>
      <td>8080.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>Dayna English</td>
      <td>Although not as streamlined as the Algebra I m...</td>
      <td>5.0</td>
      <td>Algebra II -- presentation materials</td>
      <td>1374278400</td>
      <td>2013-07-20</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td></td>
      <td>Algebra 2 California Teacher Center</td>
      <td>731.93</td>
      <td>Prentice Hall</td>
      <td>Toys &amp; Games</td>
      <td>1150291.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>annie</td>
      <td>What a great theme for a puzzle book. My daugh...</td>
      <td>5.0</td>
      <td>So cute!!</td>
      <td>1291939200</td>
      <td>2010-12-10</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>A collection of six 48-piece (that is,slightly...</td>
      <td>Dr. Seuss Jigsaw Puzzle Book: With Six 48-Piec...</td>
      <td>24.82</td>
      <td>Dr. Seuss</td>
      <td>Home &amp;amp; Kitchen</td>
      <td>590975.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>Beth Sharo "bookmom"</td>
      <td>My son got this book for his birthday.  He lov...</td>
      <td>1.0</td>
      <td>Disappointing Puzzle Book</td>
      <td>1297209600</td>
      <td>2011-02-09</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>A collection of six 48-piece (that is,slightly...</td>
      <td>Dr. Seuss Jigsaw Puzzle Book: With Six 48-Piec...</td>
      <td>24.82</td>
      <td>Dr. Seuss</td>
      <td>Home &amp;amp; Kitchen</td>
      <td>590975.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.2.3. Before starting the next processing steps, you can convert the
column names to lowercase. Furthermore, some columns that are not
necessary for the model can be deleted.
:::

:::: {.cell .code execution_count="11"}
``` python
reviews_product_metadata_df.columns = [col.lower() for col in reviews_product_metadata_df.columns]
reviews_product_metadata_df.drop(columns=['reviewername', 'summary', 'unixreviewtime', 'reviewtime'], inplace=True)
reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="11"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>description</th>
      <th>title</th>
      <th>price</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>sales_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>Great product, thank you! Our son loved the pu...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Three Dr. Suess' Puzzles: Green Eggs and Ham, ...</td>
      <td>Dr. Suess 19163 Dr. Seuss Puzzle 3 Pack Bundle</td>
      <td>37.12</td>
      <td>Dr. Seuss</td>
      <td>Toys &amp; Games</td>
      <td>612379.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>This is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>New, Sealed. Fast Shipping with tracking, buy ...</td>
      <td></td>
      <td>36.22</td>
      <td></td>
      <td>Software</td>
      <td>8080.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>Although not as streamlined as the Algebra I m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td></td>
      <td>Algebra 2 California Teacher Center</td>
      <td>731.93</td>
      <td>Prentice Hall</td>
      <td>Toys &amp; Games</td>
      <td>1150291.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>What a great theme for a puzzle book. My daugh...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>A collection of six 48-piece (that is,slightly...</td>
      <td>Dr. Seuss Jigsaw Puzzle Book: With Six 48-Piec...</td>
      <td>24.82</td>
      <td>Dr. Seuss</td>
      <td>Home &amp;amp; Kitchen</td>
      <td>590975.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>My son got this book for his birthday.  He lov...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>A collection of six 48-piece (that is,slightly...</td>
      <td>Dr. Seuss Jigsaw Puzzle Book: With Six 48-Piec...</td>
      <td>24.82</td>
      <td>Dr. Seuss</td>
      <td>Home &amp;amp; Kitchen</td>
      <td>590975.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.2.4. In the dataset, you can see that there are two possible
categorical variables: `brand` and `sales_category`. Let\'s explore them
to understand their structure and decide the type of encoding that you
will implement.
:::

:::: {.cell .code execution_count="12"}
``` python
reviews_product_metadata_df['brand'].value_counts()
```

::: {.output .execute_result execution_count="12"}
    brand
    Wizards of the Coast                                954
    Mudpuppy                                            636
    Days of Wonder                                      619
                                                        526
    Creations by You                                    424
                                                       ... 
    Star Fleet Battles ADB                                1
    Relationship Enrichment Systems                       1
    Flames of War - WWII - Core Rules &amp; Assorted      1
    Yottoy                                                1
    Prentice Hall                                         1
    Name: count, Length: 86, dtype: int64
:::
::::

:::: {.cell .code execution_count="13"}
``` python
reviews_product_metadata_df['sales_category'].value_counts()
```

::: {.output .execute_result execution_count="13"}
    sales_category
    Toys & Games               3715
    Software                    667
    Home &amp; Kitchen           58
    Arts, Crafts & Sewing        18
    Electronics                   2
    Industrial & Scientific       2
    Sports &amp; Outdoors         2
    Name: count, dtype: int64
:::
::::

::: {.cell .markdown}
The DataFrame `reviews_product_metadata` now contains information about
the reviews and the reviewed products. In the next section, you will
apply the following feature engineering steps:

-   clean the texts in the columns `reviewtext`, `descriptions`,
    `title`, `brand` and `sales_category`;
-   process the numerical features `price` and `rank_category` by
    standardizing their values, and the numerical features
    `helpful_votes` and `not_helpful_votes` by dividing each by the
    `total_votes`;
-   process the categorical features `brand` and `sales_category` by
    encoding each into numerical features.
:::

::: {.cell .markdown}
`<a id='3-3'>`{=html}`</a>`{=html}

### 3.3 - Process the Textual Features {#33---process-the-textual-features}

For text data, you have the following columns `reviewtext`,
`description` and `title`. Although the `brand` and `sales_category` do
not contain actual text and are actually categorical string variables,
you can process those strings in a similar way as the text fields.

For that, you will use the `re` python library. The processing steps to
be performed over these text and string data will be more focused on
cleaning:

-   Lowercasing: Convert all text to lowercase to maintain consistency.
-   Remove Punctuation: Strip out unnecessary punctuation.
-   Remove Special Characters: Clean any special characters that are not
    relevant to the task.
-   Remove leading, trailing or multiple spaces.

Further processing such as tokenization can be performed by some ML
models.
:::

::: {.cell .markdown}
3.3.1. Complete the `clean_text` function using the instructions in the
code.
:::

:::: {.cell .code execution_count="14" exercise="[\"ex05\"]" tags="[\"graded\"]"}
``` python
def clean_text(text: str) -> str:

    """Function in charge of cleaning text data by converting to lowercase 
    and removing punctuation and special characters.

    Args:
        text (str): Raw text string to be cleaned

    Returns:
        str: Cleaned text string
    """
    
    ### START CODE HERE ### (2 lines of code)
    # Take the `text` string and convert it to lowercase with the `lower` method that Python strings have
    text = text.lower()

    # Pass the `text` to the `re.sub()` method as the third parameter. This removes punctuation and special characters
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    ### END CODE HERE ###
    
    return text

columns_to_clean = ['reviewtext', 'description', 'title', 'brand', 'sales_category']

for column in columns_to_clean:
    # Applying cleaning function
    reviews_product_metadata_df[column] = reviews_product_metadata_df[column].apply(clean_text) 

    # Deleting unnecessary spaces
    reviews_product_metadata_df[column] = reviews_product_metadata_df[column].str.strip().str.replace(r'\s+', ' ', regex=True)

reviews_product_metadata_df[columns_to_clean].head()
```

::: {.output .execute_result execution_count="14"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewtext</th>
      <th>description</th>
      <th>title</th>
      <th>brand</th>
      <th>sales_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>great product thank you our son loved the puzz...</td>
      <td>three dr suess puzzles green eggs and ham favo...</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
      <td>dr seuss</td>
      <td>toys games</td>
    </tr>
    <tr>
      <th>1</th>
      <td>this is a great tool for any teacher using the...</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
      <td></td>
      <td></td>
      <td>software</td>
    </tr>
    <tr>
      <th>2</th>
      <td>although not as streamlined as the algebra i m...</td>
      <td></td>
      <td>algebra california teacher center</td>
      <td>prentice hall</td>
      <td>toys games</td>
    </tr>
    <tr>
      <th>3</th>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>a collection of six piece that isslightlychall...</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
    </tr>
    <tr>
      <th>4</th>
      <td>my son got this book for his birthday he loves...</td>
      <td>a collection of six piece that isslightlychall...</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.3.2. As a last step to process your text data, you are asked to create
a column that summarizes the product information. This information can
be found in the `title` or `description` fields. You are asked to use
the `title` as the primary product information field and for rows where
the product\'s title is not available, you can use the `description`.
:::

:::: {.cell .code execution_count="15" exercise="[\"ex06\"]" tags="[\"graded\"]"}
``` python
### START CODE HERE ### (3 lines of code)

# Take `title` column of the dataframe `reviews_product_metadata_df` and use `replace()` method to replace empty strings with NaN
reviews_product_metadata_df['title'] = reviews_product_metadata_df['title'].replace('', np.nan)

# Fill NaN values in title with the description
# Use the `fillna()` dataframe method to fill the missing values from the `title` with those from `description` column of the dataframe `reviews_product_metadata_df`
reviews_product_metadata_df['product_information'] = reviews_product_metadata_df['title'].fillna(reviews_product_metadata_df['description'])

# Drop `title` and `description` columns of the dataframe `reviews_product_metadata_df`
reviews_product_metadata_df.drop(columns=['title', 'description'], inplace=True)

### END CODE HERE ###

reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="15"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>price</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>sales_rank</th>
      <th>product_information</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>37.12</td>
      <td>dr seuss</td>
      <td>toys games</td>
      <td>612379.0</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>36.22</td>
      <td></td>
      <td>software</td>
      <td>8080.0</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>731.93</td>
      <td>prentice hall</td>
      <td>toys games</td>
      <td>1150291.0</td>
      <td>algebra california teacher center</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>24.82</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>590975.0</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>24.82</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>590975.0</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
Other types of processing steps for text data may include deleting stop
words, lemmatization or tokenization, but the utility of those steps may
depend on the type of ML model to use or can directly be done by the
model itself. For this lab, you are not required to implement further
processing steps for the text data.
:::

::: {.cell .markdown}
`<a id='3-4'>`{=html}`</a>`{=html}

### 3.4 - Process the Numerical Features {#34---process-the-numerical-features}

3.4.1. You can see that the joined dataset has, apart from the label
`overall` (overall ratings), two numerical variables: `price` and
`sales_rank`. You have already processed this type of data in the
previous lab, so you will implement a similar approach by performing a
standardization over these two variables using `scikit-learn`.
:::

:::: {.cell .code execution_count="16" exercise="[\"ex07\"]" tags="[\"graded\"]"}
``` python
reviews_num_columns = ["price", "sales_rank"]

### START CODE HERE ### (3 lines of code)

# Create a `StandardScaler` instance
reviews_num_std_scaler = StandardScaler()

# Compute the mean and standard deviation statistics over the `reviews_num_columns` of the dataframe `reviews_product_metadata_df`
# Use `fit()` method applied to the `reviews_num_std_scaler`
reviews_num_std_scaler.fit(reviews_product_metadata_df[reviews_num_columns])

# Perform the transformation over the `reviews_num_columns` of the datafram `reviews_product_metadata_df` 
# with the `transform()` method applied to the `reviews_num_std_scaler`
scaled_price_sales_rank = reviews_num_std_scaler.transform(reviews_product_metadata_df[reviews_num_columns])

### END CODE HERE ###

# Convert to pandas DF
scaled_price_sales_rank_df = pd.DataFrame(scaled_price_sales_rank, columns=reviews_num_columns, index=reviews_product_metadata_df.index)

scaled_price_sales_rank_df.head()
```

::: {.output .execute_result execution_count="16"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>price</th>
      <th>sales_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.103559</td>
      <td>0.730212</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.089188</td>
      <td>-0.716717</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11.197485</td>
      <td>2.018184</td>
    </tr>
    <tr>
      <th>3</th>
      <td>-0.092834</td>
      <td>0.678962</td>
    </tr>
    <tr>
      <th>4</th>
      <td>-0.092834</td>
      <td>0.678962</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.4.2. Add the `scaled_price_sales_rank_df` to the
`reviews_product_metadata_df` DataFrame.
:::

:::: {.cell .code execution_count="17"}
``` python
# Drop the original column values
reviews_product_metadata_df.drop(columns=reviews_num_columns, inplace=True)

# Add the scaled values
reviews_product_metadata_df = pd.concat([reviews_product_metadata_df, scaled_price_sales_rank_df], axis=1)

reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="17"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>product_information</th>
      <th>price</th>
      <th>sales_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>toys games</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
      <td>0.103559</td>
      <td>0.730212</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td></td>
      <td>software</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
      <td>0.089188</td>
      <td>-0.716717</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>prentice hall</td>
      <td>toys games</td>
      <td>algebra california teacher center</td>
      <td>11.197485</td>
      <td>2.018184</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.4.3. As a last step to process your numerical variables, you are going
to create a pair of new features based on the `helpful_votes`,
`total_votes` and `not_helpful_votes`. The meaning of those three fields
is the following:

-   `helpful_votes` represents the number of users who found the review
    helpful.
-   `total_votes` represents the total number of users who voted on the
    review\'s helpfulness.
-   `not_helpful_votes` represents the number of users who didn\'t find
    the review helpful.

Create two new features based on the ratios between these columns, named
`helpful_ratio` and `not_helpful_ratio`:
:::

:::: {.cell .code execution_count="18"}
``` python
# Helpful ratio
reviews_product_metadata_df['helpful_ratio'] = reviews_product_metadata_df[['helpful_votes', 'total_votes']].apply(lambda x: x['helpful_votes']/x['total_votes'] if x['total_votes'] != 0 else 0, axis=1)

# Not helpful ratio
reviews_product_metadata_df['not_helpful_ratio'] = reviews_product_metadata_df[['not_helpful_votes', 'total_votes']].apply(lambda x: x['not_helpful_votes']/x['total_votes'] if x['total_votes'] != 0 else 0, axis=1)

reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="18"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>brand</th>
      <th>sales_category</th>
      <th>product_information</th>
      <th>price</th>
      <th>sales_rank</th>
      <th>helpful_ratio</th>
      <th>not_helpful_ratio</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>toys games</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
      <td>0.103559</td>
      <td>0.730212</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td></td>
      <td>software</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
      <td>0.089188</td>
      <td>-0.716717</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>prentice hall</td>
      <td>toys games</td>
      <td>algebra california teacher center</td>
      <td>11.197485</td>
      <td>2.018184</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
`<a id='3-5'>`{=html}`</a>`{=html}

### 3.5 - Process the Categorical Features {#35---process-the-categorical-features}

You can find two categorical variables in the dataset: `brand` and
`sales_category`. When you explored the sample data, you saw that the
`sales_category` column has 7 nominal categories, while the `brand`
column has 86 categories. The column `sales_category` can be encoded
using one hot encoding. However, using one hot encoding for the column
`brand`, which has a large number of unique categories, can pose
challenges for some ML models in terms of memory and computational
complexity. In the lecture, you\'ve learned about label encoding which
could be used here, but there are also other types of encodings (you can
check this
[article](https://medium.com/anolytics/all-you-need-to-know-about-encoding-techniques-b3a0af68338b)
for a list of these methods), where each type of encoding can have a
different effect on the performance of the ML model. In this lab, you
will use an approach named frequency encoding or count encoding. This
technique encodes the categorical features based on the frequency of
each category.
:::

::: {.cell .markdown}
3.5.1. In the following cell, you will compute the frequency of each
category of the `brand` column with the `value_counts` method, then,
those categories are mapped back to the DataFrame so instead of having
the string categories, you will have the frequency of each category.
:::

:::: {.cell .code execution_count="19"}
``` python
# Frequency encoding
frequency_encoding_brand = reviews_product_metadata_df['brand'].value_counts().to_dict()
reviews_product_metadata_df['encoded_brand'] = reviews_product_metadata_df['brand'].map(frequency_encoding_brand)

# Dropping the brand column
reviews_product_metadata_df.drop(columns=["brand"], inplace=True)

reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="19"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>sales_category</th>
      <th>product_information</th>
      <th>price</th>
      <th>sales_rank</th>
      <th>helpful_ratio</th>
      <th>not_helpful_ratio</th>
      <th>encoded_brand</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>toys games</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
      <td>0.103559</td>
      <td>0.730212</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>software</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
      <td>0.089188</td>
      <td>-0.716717</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>526</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>toys games</td>
      <td>algebra california teacher center</td>
      <td>11.197485</td>
      <td>2.018184</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>home amp kitchen</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.5.2. For the `sales_category` column, you can use the `OneHotEncoder`
class given that the number of categories is not that large.
:::

:::: {.cell .code execution_count="20" exercise="[\"ex08\"]" tags="[\"graded\"]"}
``` python
### START CODE HERE ### (5 lines of code)

# Create an instance of the `OneHotEncoder` class. Use the `"ignore"` value for the `handle_unknown` parameter
sales_category_ohe = OneHotEncoder(handle_unknown="ignore")

# Copy the column `sales_category` of the dataframe `reviews_product_metadata_df` with the method `copy()`
# You will need to use double square brackets to output it as a dataframe, not a series
sales_category_df = reviews_product_metadata_df[["sales_category"]].copy()

# Convert string categories into lowercase (the code line is complete)
sales_category_df["sales_category"] = sales_category_df["sales_category"].map(lambda x: x.strip().lower())  

# Fit your encoder `sales_category_ohe` to the `sales_category_df` dataframe with the `fit()` method
sales_category_ohe.fit(sales_category_df)

# Apply the transformation using the same encoder over the same column. You will need to use the `transform()` method
# Chain `todense()` method to create a dense matrix for the encoded data
encoded_sales_category = sales_category_ohe.transform(sales_category_df).todense()

### END CODE HERE ###

# Convert the result to DataFrame
encoded_sales_category_df = pd.DataFrame(
    encoded_sales_category, 
    columns=sales_category_ohe.get_feature_names_out(["sales_category"]),
    index=reviews_product_metadata_df.index
)

encoded_sales_category_df.head()
```

::: {.output .execute_result execution_count="20"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>sales_category_arts crafts sewing</th>
      <th>sales_category_electronics</th>
      <th>sales_category_home amp kitchen</th>
      <th>sales_category_industrial scientific</th>
      <th>sales_category_software</th>
      <th>sales_category_sports amp outdoors</th>
      <th>sales_category_toys games</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
3.5.3. Add the new dataset with the transformed values to the original
dataset.
:::

:::: {.cell .code execution_count="21"}
``` python
# Drop the original column values
reviews_product_metadata_df.drop(columns=["sales_category"], inplace=True)

# Add the scaled values
reviews_product_metadata_df = pd.concat([reviews_product_metadata_df, encoded_sales_category_df], axis=1)

reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="21"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>product_information</th>
      <th>price</th>
      <th>sales_rank</th>
      <th>helpful_ratio</th>
      <th>not_helpful_ratio</th>
      <th>encoded_brand</th>
      <th>sales_category_arts crafts sewing</th>
      <th>sales_category_electronics</th>
      <th>sales_category_home amp kitchen</th>
      <th>sales_category_industrial scientific</th>
      <th>sales_category_software</th>
      <th>sales_category_sports amp outdoors</th>
      <th>sales_category_toys games</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
      <td>0.103559</td>
      <td>0.730212</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
      <td>0.089188</td>
      <td>-0.716717</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>526</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>algebra california teacher center</td>
      <td>11.197485</td>
      <td>2.018184</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

::: {.cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - Split Data and Create Text Embeddings {#4---split-data-and-create-text-embeddings}
:::

::: {.cell .markdown}
`<a id='4-1'>`{=html}`</a>`{=html}

### 4.1 - Split Data {#41---split-data}
:::

::: {.cell .markdown}
Now, that you have your dataset processed, you are going to split it
into three subdatasets. The first one will contain all the information
about the user\'s review and product features, except the `reviewtext`
and `product_information` columns; those two columns will be stored in
two different datasets and you will refer to them through the
`reviewerid` and `asin` identifiers to join with the main table. You
will make this split to process reviews and product information only
once for each unique review or product value.
:::

::: {.cell .code execution_count="22"}
``` python
# Creating reviews dataset
reviews_text_df = reviews_product_metadata_df[["reviewerid", "asin", "reviewtext"]].copy()
reviews_text_df.drop_duplicates(inplace=True)

# Creating products dataset
product_information_df = reviews_product_metadata_df[["asin", "product_information"]].copy()
product_information_df.drop_duplicates(inplace=True)

# Dropping unnecessary columns from the original DataFrame
reviews_product_metadata_df.drop(columns=["reviewtext", "product_information"], inplace=True)
```
:::

:::: {.cell .code execution_count="23"}
``` python
reviews_text_df.head()
```

::: {.output .execute_result execution_count="23"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>reviewtext</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>great product thank you our son loved the puzz...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>this is a great tool for any teacher using the...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>although not as streamlined as the algebra i m...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>what a great theme for a puzzle book my daught...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>my son got this book for his birthday he loves...</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

:::: {.cell .code execution_count="24"}
``` python
product_information_df.head()
```

::: {.output .execute_result execution_count="24"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>asin</th>
      <th>product_information</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0000191639</td>
      <td>dr suess dr seuss puzzle pack bundle</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0131358936</td>
      <td>new sealed fast shipping with tracking buy wit...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0133642984</td>
      <td>algebra california teacher center</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0375829695</td>
      <td>dr seuss jigsaw puzzle book with six piece puz...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>0439400066</td>
      <td>d puzzle buster</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

:::: {.cell .code execution_count="25"}
``` python
reviews_product_metadata_df.head()
```

::: {.output .execute_result execution_count="25"}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>reviewerid</th>
      <th>asin</th>
      <th>overall</th>
      <th>year</th>
      <th>month</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>not_helpful_votes</th>
      <th>price</th>
      <th>sales_rank</th>
      <th>helpful_ratio</th>
      <th>not_helpful_ratio</th>
      <th>encoded_brand</th>
      <th>sales_category_arts crafts sewing</th>
      <th>sales_category_electronics</th>
      <th>sales_category_home amp kitchen</th>
      <th>sales_category_industrial scientific</th>
      <th>sales_category_software</th>
      <th>sales_category_sports amp outdoors</th>
      <th>sales_category_toys games</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AMEVO2LY6VEJA</td>
      <td>0000191639</td>
      <td>5.0</td>
      <td>2013</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.103559</td>
      <td>0.730212</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A2GGHHME9B6W4O</td>
      <td>0131358936</td>
      <td>5.0</td>
      <td>2013</td>
      <td>10</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0.089188</td>
      <td>-0.716717</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>526</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A1FSLDH43ORWZP</td>
      <td>0133642984</td>
      <td>5.0</td>
      <td>2013</td>
      <td>7</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>11.197485</td>
      <td>2.018184</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AYVR1MQCTNU5D</td>
      <td>0375829695</td>
      <td>5.0</td>
      <td>2010</td>
      <td>12</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A3CJHKFHHQJP2K</td>
      <td>0375829695</td>
      <td>1.0</td>
      <td>2011</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>-0.092834</td>
      <td>0.678962</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>
:::
::::

:::: {.cell .code execution_count="26"}
``` python
reviews_product_metadata_df.shape
```

::: {.output .execute_result execution_count="26"}
    (4464, 20)
:::
::::

::: {.cell .markdown}
`<a id='4-2'>`{=html}`</a>`{=html}

### 4.2 - Create Text Embeddings {#42---create-text-embeddings}

Now that the data has been separated into three different datasets,
let\'s create embeddings for the text data. You will create embeddings
for the `product_information` field in the `product_information_df`
DataFrame and the `reviewtext` field in the `reviews_text_df` DataFrame.

Just as a refresher, an **embedding** is a numerical representation of
text, documents, images, or audio. This representation captures the
semantic meaning of the embedded content while representing the data in
a more compact form. The ML team has provided you with an API with the
Sentence Transformer library, running the
[`all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2)
model, which is a multipurpose and lightweight sentence transformer
model that can map sentences and paragraphs to a 384-dimensional vector.
You\'ve seen in the lecture how you can interact with such a model using
the `sentence-transformers` [module](https://sbert.net/index.html). For
this lab, the model has been provisioned on an EC2 instance, allowing
you to interact with it through API calls. This approach aligns with the
current industry standard for accessing LLM services.
:::

::: {.cell .markdown}
4.2.1. The `ENDPOINT_URL` variable that you defined at the beginning of
the lab is the endpoint that you will use to interact with the API.
Furthermore, you are provided with a function to make API requests for
the model to return the embeddings of a provided text. You should
already be familiar with requests to REST APIs, so you can see that this
particular call is a POST call in which you also send data to the
endpoint through the `payload` dictionary.
:::

::: {.cell .code execution_count="27"}
``` python
def get_text_embeddings(endpoint_url: str , text: Union[str, List[str]]): 
    
    payload = {"text": text}

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(endpoint_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
  
        return response.json()
    
    except Exception as err:
        print(f"Error: {err}")
```
:::

::: {.cell .markdown}
4.2.2. As an example of the result of the API call, you can execute the
following cell:
:::

:::: {.cell .code execution_count="35"}
``` python
# Text to send to the API call
text = product_information_df.iloc[0]['product_information']

# Performing API call and getting the result
embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text)
embeddings = embedding_response['body']

print(f"Text sent to the API: {text}")
print(f"Length of the embedding from the response: {len(embeddings)}")
print(f"A subset of the returned embeddings with the first 10 elements: {embeddings[:10]}")
```

::: {.output .stream .stdout}
    Text sent to the API: dr suess dr seuss puzzle pack bundle
    Length of the embedding from the response: 384
    A subset of the returned embeddings with the first 10 elements: [-0.03424278646707535, 0.022777101024985313, -0.0032790950499475002, -0.030152201652526855, 0.003919770009815693, 0.03298501297831535, 0.05947704613208771, 0.06927525252103806, -0.0616498626768589, 0.02031721919775009]
:::
::::

::: {.cell .markdown}
*Note:* If you get an error with Connection aborted and
RemoteDisconnected messages, go to the AWS console and head to the
**EC2** section. There should be an EC2 instance with the name
`de-c4w2a1-ml-model-instance`, go to the **Instance state** dropdown and
click on **Reboot instance**.

![EC2Reboot](images/reboot_ec2_instance.png)
:::

::: {.cell .markdown}
You can see that the size of the returned embedding is of 384 elements.
This size will be the same for all vector embeddings that will be
returned.
:::

::: {.cell .markdown}
4.2.3. Let\'s process the text for the `product_information_df`
DataFrame and get the corresponding embeddings. You are going to divide
the DataFrame into more manageable chunks (smaller DataFrames); for
that, use the `split_dataframe` function provided below.
:::

::: {.cell .code execution_count="36"}
``` python
def split_dataframe(df, chunk_size=20):
    chunks = list()
    num_chunks = (len(df) + chunk_size - 1) // chunk_size 
    
    for i in range(num_chunks):
        chunk = df[i*chunk_size:(i+1)*chunk_size]
        if not chunk.empty:
            chunks.append(chunk)
    return chunks

# Split the `product_information_df` with the default chunk size of 20 rows per chunk
list_df = split_dataframe(df=product_information_df)
```
:::

::: {.cell .markdown}
4.2.4. Given that you need to perform several API calls and the
dimension of your embeddings is 384 elements, to avoid exceeding the
usage of RAM of your development environment, you will perform API calls
to get the embeddings of a chunk of data and then insert the
corresponding data directly into the database. Let\'s create the
connection to the database through the `psycopg2` package. Then, in a
similar way as you did in the Course 1 Week 4 Assignment, you need to
enable the `vector` extension in your PostgreSQL database.
:::

::: {.cell .code execution_count="39"}
``` python
conn = psycopg2.connect( 
    database=DBNAME, user=DBUSER, 
    password=DBPASSWORD, host=DBHOST, port=DBPORT
) 

# Set autocommit to true
conn.autocommit = True

# Create cursor
cursor = conn.cursor()
cursor.execute('CREATE EXTENSION IF NOT EXISTS vector')
```
:::

::: {.cell .markdown}
4.2.5. The
[`psycopg2`package](https://github.com/pgvector/pgvector-python?tab=readme-ov-file#psycopg-2)
provides `pgvector` support for Python, in other words it enables vector
similarity search for Postgres. Once you have enabled the `vector`
extension, you need to register the vector type with your connection.
:::

::: {.cell .code execution_count="40"}
``` python
register_vector(conn)
```
:::

::: {.cell .markdown}
4.2.6. Then, you can create the `product_embeddings` table. In that
table, you will insert the `asin` (product ID), the product information
(either the product title or description) and the embedding from the
product information.
:::

::: {.cell .code execution_count="41"}
``` python
cursor.execute('DROP TABLE IF EXISTS product_embeddings')
cursor.execute('CREATE TABLE product_embeddings (asin VARCHAR(15) PRIMARY KEY, product_information TEXT, product_embedding vector(384))')
```
:::

::: {.cell .markdown}
4.2.7. Complete the code in the next cell to call the API with a chunk
of text and then insert it into the vector database. The insertion
process code has already been provided to you. It is created as a string
with the `INSERT` statement and the values to be inserted are appended
through the `value_array` list.
:::

:::: {.cell .code execution_count="42" exercise="[\"ex09\"]" tags="[\"graded\"]"}
``` python
for id, df_chunk in enumerate(list_df):
    
    ### START CODE HERE ### (3 lines of code)
    
    # Convert the `asin` column from the `df_chunk` dataframe into a list with the `to_list()` method
    asin_list = df_chunk['asin'].to_list()

    # Convert the `product_information` column from the `df_chunk` dataframe into a list with the `to_list()` method
    text_list = df_chunk['product_information'].to_list()

    # Perform an API call through the `get_text_embeddings` function
    # Pass the `ENDPOINT_URL` variable and the chunk of texts stored in the list `text_list` as parameters to that function
    embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text_list)

    ### END CODE HERE ###
    
    # Inserting the data    
    insert_statement = f'INSERT INTO product_embeddings (asin, product_information, product_embedding) VALUES'
    
    value_array = [] 
    for asin, text, embedding in zip(asin_list, text_list, embedding_response['body']):
        value_array.append(f"('{asin}', '{text}', '{embedding}')")
                
    value_str = ",".join(value_array)
    insert_statement = f"{insert_statement} {value_str};"
    
    cursor.execute(insert_statement)

    if id % 5 == 0:
        print(f"Data inserted for batch with id {id}")
        time.sleep(5)
```

::: {.output .stream .stdout}
    Data inserted for batch with id 0
    Data inserted for batch with id 5
    Data inserted for batch with id 10
    Data inserted for batch with id 15
:::
::::

::: {.cell .markdown}
4.2.8. Check that data has been inserted:
:::

:::: {.cell .code execution_count="43" scrolled="true"}
``` python
cursor.execute('SELECT COUNT(*) FROM product_embeddings')
cursor.fetchall()
```

::: {.output .execute_result execution_count="43"}
    [(400,)]
:::
::::

::: {.cell .markdown}
4.2.9. Use the following cell to get the embeddings from the product
information of a particular product.
:::

:::: {.cell .code execution_count="44"}
``` python
text = product_information_df.iloc[11]['product_information']

# Performing API call and getting the result
embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text)
embeddings = embedding_response['body']

print(f"Text: {text}")
```

::: {.output .stream .stdout}
    Text: watercolor painting for dummies
:::
::::

::: {.cell .markdown}
4.2.10. In a vector database, you can perform searches for the most
similar elements. In [`pgvector`](https://github.com/pgvector/pgvector)
you can use the `<->` operator to get the nearest neighbors by L2
distance. Extract the 5 nearest products according to the L2 distance.
:::

:::: {.cell .code execution_count="45" scrolled="true"}
``` python
select_statement = f"SELECT asin, product_information FROM product_embeddings ORDER BY product_embedding <-> '{embeddings}' LIMIT 5"
cursor.execute(select_statement)
cursor.fetchall()
```

::: {.output .execute_result execution_count="45"}
    [('0470182318', 'watercolor painting for dummies'),
     ('048645195X', 'dover publicationsdecorative tile designs coloring book'),
     ('0486430502', 'i love america stained glass coloring book'),
     ('0735335788', 'mudpuppy mermaids colorin crowns'),
     ('073532753X', 'mudpuppy wooden magnetic painters palette letters')]
:::
::::

::: {.cell .markdown}
4.2.11. You can play with the text of the product information to find
some similar products. For example, create a custom text to find similar
products. Given that you have used the `classicmodels` dataset
previously, let\'s search for similar products to this or similar
descriptions: \"scale car toy\".
:::

:::: {.cell .code execution_count="46"}
``` python
text = "scale car toy"

# Performing API call and getting the result
embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text)
embeddings = embedding_response['body']

select_statement = f"SELECT asin, product_information FROM product_embeddings ORDER BY product_embedding <-> '{embeddings}' LIMIT 5"
cursor.execute(select_statement)
cursor.fetchall()
```

::: {.output .execute_result execution_count="46"}
    [('0786953306',
      'star wars masters of the force a star wars miniatures booster expansion star wars miniatures product'),
     ('0786949864',
      'demonweb a dampd miniatures booster expansion dampd miniatures product'),
     ('0786938919', 'star wars cmg miniatures game universe huge booster pack'),
     ('0786941006', 'starship battles huge booster star wars miniatures'),
     ('0439843073', 'magalina dog quot soft toy')]
:::
::::

::: {.cell .markdown}
Although no information about cars can be found in the sample dataset,
the embeddings were able to relate the word `scale` with `miniature`.
:::

::: {.cell .markdown}
4.2.12. Once the embeddings from the product\'s information have been
stored, compute and store the embeddings for the reviews. Follow the
same procedure as the one done for the product information in the next
three cells. Create the `review_embeddings` table.
:::

::: {.cell .code execution_count="47"}
``` python
cursor.execute('DROP TABLE IF EXISTS review_embeddings')
cursor.execute('CREATE TABLE review_embeddings (reviewerid VARCHAR(30), asin VARCHAR(15), reviewtext TEXT, review_embedding vector(384), PRIMARY KEY(reviewerid, asin))')
```
:::

::: {.cell .markdown}
4.2.13. Use `split_dataframe()` function to split the `reviews_text_df`
DataFrame with the default chunk size of 20 rows per chunk.
:::

::: {.cell .code execution_count="48"}
``` python
### START CODE HERE ### (1 line of code)
list_df = split_dataframe(df=reviews_text_df, chunk_size=20)
### END CODE HERE ###
```
:::

::: {.cell .markdown}
4.2.14. Complete the code in the next cell to call the API with a chunk
of text and then insert it into the vector database. Execute the
insertion cell; this **should take less than 10 minutes**.
:::

:::: {.cell .code execution_count="49" exercise="[\"ex10\"]" tags="[\"graded\"]"}
``` python
# Call the API and insert the data 
start_time = time.time()

for id, df_chunk in enumerate(list_df):
    
    ### START CODE HERE ### (4 lines of code)

    # Convert the `reviewerid`, `asin` and `reviewtext` columns from the `df_chunk` dataframe into a list with the `to_list()` method
    reviewer_list = df_chunk['reviewerid'].to_list()
    asin_list = df_chunk['asin'].to_list()
    text_list = df_chunk['reviewtext'].to_list()

    # Perform an API call through the `get_text_embeddings` function
    # Pass the `ENDPOINT_URL` variable and the chunk of texts stored in the list `text_list` as parameters to that function
    embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text_list)

    ### END CODE HERE ###
    
    # Insert the data
    insert_statement = f'INSERT INTO review_embeddings (reviewerid, asin, reviewtext, review_embedding) VALUES'
    value_array = [] 
    
    for reviewer, asin, text, embedding in zip(reviewer_list, asin_list, text_list, embedding_response['body']):
        value_array.append(f"('{reviewer}', '{asin}', '{text}', '{embedding}')")
        
    value_str = ",".join(value_array)
    insert_statement = f"{insert_statement} {value_str};"
    
    cursor.execute(insert_statement)    

    if id % 50 == 0:
        print(f"Data inserted for batch with id {id}")
        time.sleep(10)

end_time = time.time()
print(f"Total time spent {end_time - start_time} seconds")
```

::: {.output .stream .stdout}
    Data inserted for batch with id 0
    Data inserted for batch with id 50
    Data inserted for batch with id 100
    Data inserted for batch with id 150
    Data inserted for batch with id 200
    Total time spent 490.1256642341614 seconds
:::
::::

::: {.cell .markdown}
4.2.15. Check that the data has been properly inserted.
:::

:::: {.cell .code execution_count="50"}
``` python
cursor.execute('SELECT count(*) FROM review_embeddings')
cursor.fetchall()
```

::: {.output .execute_result execution_count="50"}
    [(4464,)]
:::
::::

::: {.cell .markdown}
4.2.16. Finally, you can take a text and search for the more similar
reviews using the L2 distance. Change the text for a custom message to
experiment with the results! As an example, explore the results from
this message: \"I didn\'t like this toy, it was broken!\"
:::

:::: {.cell .code execution_count="51"}
``` python
text=reviews_text_df.iloc[0]['reviewtext']
embedding_response = get_text_embeddings(endpoint_url=ENDPOINT_URL, text=text)
embeddings = embedding_response['body']
print(f"Text: {text}")
```

::: {.output .stream .stdout}
    Text: great product thank you our son loved the puzzles they have large pieces yet they are still challenging for a year old
:::
::::

:::: {.cell .code execution_count="52"}
``` python
select_statement = f"SELECT reviewerid, asin, reviewtext FROM review_embeddings ORDER BY review_embedding <-> '{embeddings}' LIMIT 5"
cursor.execute(select_statement)
cursor.fetchall()
```

::: {.output .execute_result execution_count="52"}
    [('AMEVO2LY6VEJA',
      '0000191639',
      'great product thank you our son loved the puzzles they have large pieces yet they are still challenging for a year old'),
     ('ABZLHQGQS7U9V',
      '0735331146',
      'such great quality easy to put together because of the thick well made pieces my year old loves them and can do these piece puzzles by herself'),
     ('A5N978MBDMG8T',
      '0735331111',
      'love these puzzlesi have them all now my year old loves the big pieces and the map to show her the picture big hit'),
     ('A2R4O4128P54Y5',
      '0735329605',
      'i love these puzzles in a tin no cardboard boxes that bend and fall apart losing piecesi buy them for my five year old granddaughter she loves the challenge of a piece puzzle it is a nice activity for us to do togetheralthough i have to admit that she is faster than i am at finding all the right pieces'),
     ('A3SU2PP1NXVYEE',
      '0867343125',
      'my year old loves this puzzle the large thick pieces are easy to manipulate great for teaching numbers up to')]
:::
::::

::: {.cell .markdown}
4.2.17. The last step consists of inserting the data from the
`reviews_product_metadata_df` DataFrame, which contains the other
transformed variables that can be used to train another ML model.
:::

::: {.cell .code execution_count="53"}
``` python
ddl_statement = """CREATE TABLE review_product_transformed (
    reviewerid VARCHAR(25) NOT NULL,
    asin VARCHAR(15) NOT NULL,
    overall FLOAT,
    year INTEGER,
    month INTEGER,
    helpful_votes INTEGER,
    total_votes INTEGER,
    not_helpful_votes INTEGER,
    price FLOAT,
    sales_rank FLOAT,
    helpful_ratio FLOAT,
    not_helpful_ratio FLOAT,
    encoded_brand INTEGER,
    sales_category_arts_crafts_sewing FLOAT,
    sales_category_electronics FLOAT,
    sales_category_home_amp_kitchen FLOAT,
    sales_category_industrial_scientific FLOAT,
    sales_category_software FLOAT,
    sales_category_sports_amp_outdoors FLOAT,
    sales_category_toys_games FLOAT,
    PRIMARY KEY (reviewerid, asin)
);
"""

cursor.execute('DROP TABLE IF EXISTS review_product_transformed')
cursor.execute(ddl_statement)
```
:::

::: {.cell .code execution_count="54"}
``` python
insert_query = """
    INSERT INTO review_product_transformed (
        reviewerid, asin, overall, year, month, helpful_votes, total_votes, 
        not_helpful_votes, price, sales_rank, helpful_ratio, not_helpful_ratio, 
        encoded_brand, sales_category_arts_crafts_sewing, sales_category_electronics, 
        sales_category_home_amp_kitchen, sales_category_industrial_scientific, 
        sales_category_software, sales_category_sports_amp_outdoors, sales_category_toys_games
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterate over the DataFrame rows and insert each row into the database
for i, row in reviews_product_metadata_df.iterrows():
    cursor.execute(insert_query, tuple(row))
```
:::

:::: {.cell .code execution_count="55"}
``` python
cursor.execute('SELECT COUNT(*) FROM review_product_transformed')
cursor.fetchall()
```

::: {.output .execute_result execution_count="55"}
    [(4464,)]
:::
::::

::: {.cell .markdown}
4.2.18. Finally, you are required to close the connection to the vector
database.
:::

::: {.cell .code}
``` python
cursor.close()
conn.close()
```
:::

::: {.cell .markdown}
In this lab, you successfully transformed a dataset provided in JSON
format into a structured format suitable for machine learning (ML)
applications. This process involved three critical steps of feature
engineering: data extraction, feature creation, and data storage. In
addition, you processed text data and interacted with an NLP model to
generate text embeddings. You integrated your process with a vector
database in PostgreSQL which facilitates the efficient storage and
retrieval of high-dimensional vector data. With the embeddings stored in
your vector database, you are also able to find similar items or reviews
through the usage of an L2 Euclidean distance to measure similarity
between vectors. This approach allowed you to retrieve items or reviews
with embeddings that were closest to the query embedding.
:::

::: {.cell .code}
``` python
```
:::
