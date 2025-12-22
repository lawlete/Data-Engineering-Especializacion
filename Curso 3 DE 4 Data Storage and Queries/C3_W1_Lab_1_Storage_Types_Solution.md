::: {.cell .markdown}
# Week 1 Lab: Comparing Cloud Data Storage Options

In this lab, you will explore the advantages and drawbacks of various
storage options in the Cloud: object, file, memory, and block storage.
These considerations are vital for Data Engineers to make informed
decisions tailored to their project needs, optimizing performance,
scalability, and cost-effectiveness.

*Note*: Object storage (Amazon S3) was covered in Course 2 Week 1 Lab 3
\"Introduction to Object Storage - Amazon S3\". You may want to review
its content, however, that\'s not compulsory to understand the material
of this lab.

To open the solution notebook, follow these steps:

-   Go to the main menu and select `File -> Preferences -> Settings`.
-   Click on `text editor` on the left, then scroll down to the
    `Files: Exclude` section.
-   Remove the line `**/C3_W1_Lab_1_Storage_Types_Solution.ipynb`. The
    file will now appear in the explorer.
-   You can close the `Settings` tab.
:::

::: {.cell .markdown}
# Table of Contents

-   [1 - Object Storage](#1)
    -   [1.1 - Configure S3 Bucket and Upload Data](#1-1)
        -   [Exercise 1](#ex01)
        -   [Exercise 2](#ex02)
        -   [Exercise 3](#ex03)
    -   [1.2 - Modify and Reupload Data](#1-2)
        -   [Exercise 4](#ex04)
        -   [Exercise 5](#ex05)
        -   [Exercise 6](#ex06)
        -   [Exercise 7](#ex07)
        -   [Exercise 8](#ex08)
-   [2 - File Storage](#2)
    -   [2.1 - Key Components of File Storage System](#2-1)
    -   [2.2 - Features of File Storage System](#2-2)
-   [3 - Memory Storage](#3)
    -   [3.1 - Caching and Volatility](#3-1)
    -   [3.2 - Memory Storage Capacity](#3-2)
-   [4 - Block Storage](#4)
:::

::: {.cell .markdown}
First, let\'s import the required packages.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
import os
import gc
import boto3
import botocore
import timeit
import subprocess
import pandas as pd

from cache_pandas import timed_lru_cache
from botocore.errorfactory import ClientError
from src.block_storage_client import BlockStorageClient
from io import StringIO
from IPython.display import HTML
from typing import Any, Dict
```
:::

::: {.cell .markdown}
`<a id='1'>`{=html}`</a>`{=html}

## 1 - Object Storage {#1---object-storage}

**Object storage systems** are a type of data storage architecture that
stores data as objects rather than in a traditional file hierarchy.

In object storage systems, data is stored as discrete units called
**objects**. Each object consists of the actual data (payload or
content) along with metadata attributes that describe the object.
Metadata typically includes information such as object name, unique
identifier (key), size, content type, creation/modification timestamps,
and custom user-defined metadata.

Unlike file storage systems, object storage systems do not use a
hierarchical directory structure. Instead, objects are stored in a flat
namespace, identified by unique keys or identifiers. This flat
architecture allows for easier scalability and distribution of data
across multiple storage nodes. Object storage contains immutable objects
of various sizes and types. Unlike files on a local disk, objects cannot
be modified in place.

Between other features of object storage systems you can find:

-   Durability and Redundancy: Object storage systems typically employ
    data redundancy and replication techniques to ensure data durability
    and availability. Objects are often replicated across multiple
    storage nodes or data centers to protect against hardware failures,
    data corruption, and other types of data loss.

-   Access Methods: Objects are accessed using standardized application
    programming interfaces (APIs) such as Amazon S3 (Simple Storage
    Service) API. These APIs provide a set of operations for storing,
    retrieving, updating, and deleting objects, as well as managing
    object metadata and access control.

-   Data Access Patterns: Object storage systems are well-suited for
    storing unstructured data and large volumes of data, such as
    multimedia files, backups, archives, log files, and data lakes. They
    are commonly used in cloud storage services, content delivery
    networks (CDNs), and big data analytics platforms.
:::

::: {.cell .markdown}
`<a id='1-1'>`{=html}`</a>`{=html}

### 1.1 - Configure S3 Bucket and Upload Data {#11---configure-s3-bucket-and-upload-data}

For the Object Storage system part, you will use the file that is
locally stored at `./data/employees.csv`. You will upload the file to an
S3 Bucket, which is the Object Storage Service available in AWS and you
will work with the versioning of the file.
:::

::: {.cell .markdown}
You need to set some Python constants for the data bucket. Run the
following cell to do that:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
AWS_ACCOUNT_ID = subprocess.run(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'], capture_output=True, text=True).stdout.strip()
BUCKET_NAME = f'de-c3w1lab1-{AWS_ACCOUNT_ID}-us-east-1-data-bucket'
AWS_DEFAULT_REGION = 'us-east-1'
```
:::

::: {.cell .markdown}
Enable versioning in your S3 bucket by running the
`configure_bucket_versioning()` function:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def configure_bucket_versioning(bucket_name: str, versioning_config: Dict[str, str]) -> Dict[Any, Any]:
    s3_client = boto3.client('s3')

    response = s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration=versioning_config
    )

    return response

versioning_config = {'Status': 'Enabled'}

response = configure_bucket_versioning(bucket_name=BUCKET_NAME, 
                                       versioning_config=versioning_config)
print(response)
```
:::

::: {.cell .markdown}
Now use the following AWS CLI command to check that your bucket has the
versioning feature enabled. You can run it in the following cell or in
the terminal.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
!aws s3api get-bucket-versioning --bucket $BUCKET_NAME
```
:::

::: {.cell .markdown}
Before uploading any files, remember that you can use the AWS CLI tool
to check the content of your bucket (it is empty at the moment).
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
!aws s3 ls $BUCKET_NAME
```
:::

::: {.cell .markdown}
`<a id='ex01'>`{=html}`</a>`{=html}

### Exercise 1

In the following cell, complete the function `upload_file_to_s3` to
upload a file from your local file system storage to the S3 object
storage.

1.  Create a `boto3` client for S3 (recall from previous labs that you
    can do this by calling the `boto3.client()` method with `'s3'` as
    the parameter).
2.  Use the `upload_file()` method of your S3 client and pass the
    Filename, Bucket and Key parameters according to the function
    blueprint. You can always check the
    [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html)
    for more information about those parameters.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def upload_file_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> None:
    """Uploads a local file to S3 using boto3

    Args:
        local_file_path (str): Local file path
        BUCKET_NAME (str): Bucket name
        s3_key (str): Key (Path) in S3 to save the file
    """

    ### START CODE HERE ### (~ 3 lines of code)
    # Create an S3 client
    s3_client = boto3.client('s3') # @REPLACE EQUALS None.None('None')
    
    try: # @KEEP
        
        # Upload the file to S3
        s3_client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=s3_key) # @REPLACE s3_client.None(Filename=None, Bucket=None, Key=None)
        
        ### END CODE HERE ###
        
        print(f"File at {local_file_path} has been uploaded to s3://{bucket_name}/{s3_key} successfully.")
    except Exception as err:
        print(f"Error uploading file to S3: {err}")
```
:::

::: {.cell .markdown}
`<a id='ex02'>`{=html}`</a>`{=html}

### Exercise 2

Upload the local file at `./data/employees.csv` into the bucket you
created earlier with the following S3 key: `data/csv/employees.csv`.
Pass the corresponding parameters to the function `upload_file_to_s3()`.
Once uploaded, you should get a message telling you that the file was
uploaded successfully.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
### START CODE HERE ### (~ 3 lines of code)

# Define the local file path, and S3 key
local_file_path = './data/employees.csv' # @REPLACE EQUALS 'None'
s3_key = 'data/csv/employees.csv' # @REPLACE EQUALS 'None'

# Upload the file to S3
upload_file_to_s3(local_file_path=local_file_path, bucket_name=BUCKET_NAME, s3_key=s3_key) # @REPLACE None(local_file_path=None, bucket_name=None, s3_key=None)
### END CODE HERE ###
```
:::

::: {.cell .markdown}
Now that the file has been uploaded, you can check if a particular key
exists. Remember that in an Object storage system, every object has an
associated unique key that acts as the unique identifier for that
object. This key may resemble the directory and subdirectory hierarchy
that you are used to seeing, but those folders and subfolders do not
actually exist. To check if a particular key exists, you\'ll create the
`key_exists_in_s3()` function that makes use of the [`head_object`
method](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html)
from the boto3 client; this method allows you to retrieve the object
metadata based on the object\'s key without returning the complete
object. If the key exists (i.e., there is an object related to that key)
you will be able to see the associated metadata; if the key cannot be
found, that means that there is no object associated with that key and
an error will be returned.
:::

::: {.cell .markdown}
`<a id='ex03'>`{=html}`</a>`{=html}

### Exercise 3

1.  Create again a `boto3` client for S3.
2.  Check the existence of the file with the `head_object()` method
    passing it the Bucket name and Key parameters.
3.  Return the `response` variable.

You should receive a message telling you that the object with the key
you inputted exists.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def key_exists_in_s3(bucket_name: str, s3_key: str) -> Dict[Any, Any]:
    """Checks if an specific key exists in an S3 bucket

    Args:        
        BUCKET_NAME (str): Bucket name
        s3_key (str): Key (Path) in S3 to save the file
    """
    
    ### START CODE HERE ### (~ 5 lines of code)
    
    # Create an S3 client
    s3_client = boto3.client('s3') # @REPLACE EQUALS None.None('None')
     
    try: # @KEEP
        
        # Use `head_object()` method passing Bucket name and Key parameters.
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key) # @REPLACE EQUALS s3_client.None(Bucket=None, Key=None)
        print(f"File at s3://{bucket_name}/{s3_key} exists!") # @KEEP
        
        return response # @REPLACE return None
        
        ### END CODE HERE ###
        
    except botocore.exceptions.ClientError as err:
        print(f"Error message: {err}")
        
        if err.response['Error']['Message'] == 'Not Found':
            print(f"The key {s3_key} does not exist.")                        
            return err.response

s3_key_to_check = 'data/csv/employees.csv'

response = key_exists_in_s3(bucket_name=BUCKET_NAME, s3_key=s3_key_to_check)
```
:::

::: {.cell .markdown}
Check the response and you will see a key named `'VersionId'` with an
identifier of the current version of the Object you just queried,
alongside other information about the size of the object and the date of
modification.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
response
```
:::

::: {.cell .markdown}
You were searching for the complete key of the file and were able to
retrieve the metadata of an existing object. Let\'s try to see if the
object `data/csv` exists. You may think that this is a file path with
subfolders (as you are used to using when working with file systems, as
we will check later), but in Object storage, all objects are stored at
the same level and the Key is only a unique identifier for each object.
Run the following cells, you are expected to find errors:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
s3_key_to_check_incorrect = 'data/csv'

response_failed = key_exists_in_s3(bucket_name=BUCKET_NAME, s3_key=s3_key_to_check_incorrect)
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
response_failed
```
:::

::: {.cell .markdown}
`<a id='1-2'>`{=html}`</a>`{=html}

### 1.2 - Modify and Reupload Data {#12---modify-and-reupload-data}

Now, let\'s read the file you just uploaded, perform some modifications
to it, and re-upload the modified file.
:::

::: {.cell .markdown}
`<a id='ex04'>`{=html}`</a>`{=html}

### Exercise 4

Complete the `read_csv_from_s3` function to read a CSV file directly
from your S3 bucket.

1.  Create the `boto3` client for S3.
2.  Call the `get_object()` method of the client and pass the bucket
    name and object key.
3.  Use the `read()` method of the `response['Body']` object and then
    chain the `decode('utf-8')` method. The `read()` method reads the
    raw bytes of the file stored in the S3 object\'s body. When you
    retrieve an object from S3 using `get_object()`, it returns a
    response object that contains the file\'s contents in its body. The
    `decode('utf-8')` method converts the raw bytes into a UTF-8 encoded
    string. This step is necessary because the `read()` method returns
    the file\'s content as bytes, and to work with text data (such as a
    CSV file), you need to decode these bytes into a string using an
    appropriate character encoding, which is UTF-8 in this case.
4.  Use `pd.read_csv()` to read the object at `csv_content`. This
    `csv_content` object should be enclosed first by the `StringIO`
    class. The `StringIO` class from the `io` module allows you to treat
    a string as a file-like object. `StringIO(csv_content)` creates a
    file-like object from the CSV content string decoded in the previous
    step. The `pd.read_csv()` function from pandas expects a file-like
    object (such as a file handle or a `StringIO` object) as input, so
    using `StringIO` allows you to pass the CSV content directly to
    `read_csv()`.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def read_csv_from_s3(bucket_name: str, s3_key: str) -> pd.DataFrame:
    """Reads csv file stored in an S3 bucket

    Args:        
        BUCKET_NAME (str): Bucket name
        s3_key (str): Key (Path) in S3 of the file
    """

    ### START CODE HERE ### (~ 5 lines of code)
    
    # Create an S3 client
    s3_client = boto3.client('s3') # @REPLACE EQUALS None.None('None')

    try: # @KEEP

        
        # Get object from the bucket using `get_object()` method
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key) # @REPLACE EQUALS s3_client.None(Bucket=None, Key=None)

        # Read raw bytes from the S3 object, convert it to string, and save it into the `csv_content` variable
        csv_content = response['Body'].read().decode('utf-8') # @REPLACE EQUALS None['None'].None().None('None')

        # Use pandas `read_csv()` method to read the data into the dataframe
        df = pd.read_csv(StringIO(csv_content)) # @REPLACE EQUALS None.None(None(None))
        
        ### END CODE HERE ###
        return df
        
    except Exception as err:
        print(f"Error message: {err}") 
```
:::

::: {.cell .markdown}
`<a id='ex05'>`{=html}`</a>`{=html}

### Exercise 5

Use the `read_csv_from_s3()` function to read the object located in the
bucket you previously created, with the S3 key
`'data/csv/employees.csv'`.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
### START CODE HERE ### (~ 2 lines of code)
csv_s3_key = 'data/csv/employees.csv' # @REPLACE EQUALS 'None'

df = read_csv_from_s3(bucket_name=BUCKET_NAME, s3_key=csv_s3_key) # @REPLACE EQUALS None(bucket_name=None, s3_key=None)
### END CODE HERE ###

df.head()
```
:::

::: {.cell .markdown}
Explore the dataset with the `shape` attribute.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df.shape
```
:::

::: {.cell .markdown}
Check the content of the data for the `'Brooks Ltd'` company and select
the employees with a Salary higher than 120,000. You will expect not
more that a hundred rows that match that condition:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df[(df['Company']=='Brooks Ltd') & (df['Salary']>120000)]
```
:::

::: {.cell .markdown}
Let\'s suppose that for `'Brooks Ltd'` company the maximum salary that
an employee can earn is 120,000. Then, modify the content of the
dataframe according to that condition:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df.loc[(df['Company']=='Brooks Ltd') & (df['Salary']>120000), 'Salary'] = 120000
```
:::

::: {.cell .markdown}
Check the modification of the data:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df[(df['Company']=='Brooks Ltd') & (df['Salary']==120000)]
```
:::

::: {.cell .markdown}
Now, let\'s upload again the file pointing to the same S3 key. For that,
you will use the `upload_pandas_to_s3` to convert the DataFrame object
to CSV and then upload it again to the bucket.

You are already provided with a code snippet:

-   `csv_buffer = StringIO()`: This line creates an in-memory buffer
    using the `StringIO` class from the `io` module. In this case, it
    creates an empty buffer where we\'ll write the CSV data.
-   `df.to_csv(csv_buffer, index=False)`: This line writes the contents
    of the DataFrame `df` to the `csv_buffer` in CSV format. The
    `to_csv()` method of pandas DataFrame converts the DataFrame into a
    CSV-formatted string and writes it to the provided file-like object
    (csv_buffer in this case).
:::

::: {.cell .markdown}
`<a id='ex06'>`{=html}`</a>`{=html}

### Exercise 6

1.  Create the `boto3` client to S3.
2.  Call the [`put_object()`
    method](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html)
    from your S3 client and pass the parameters Bucket, Key and Body.
    For the body, make sure to pass `csv_buffer.getvalue()`. In this
    case, `Body` is set to the contents of the `csv_buffer` obtained
    using the `getvalue()` method. The `getvalue()` method retrieves the
    contents of the `csv_buffer` as a string, which is necessary because
    the `Body` parameter expects the object content as a binary string
    or bytes-like object.
3.  Return the response to the client\'s call.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def upload_pandas_to_s3(df: pd.DataFrame, bucket_name: str, s3_key: str) -> Dict[Any, Any]:
    """ Uploads a pandas dataframe as a csv file to an S3 bucket

    Args:        
        df: (pd.DataFrame): Pandas dataframe to store
        BUCKET_NAME (str): Bucket name
        s3_key (str): Key (Path) in S3 of the file
    """
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    ### START CODE HERE ### (~ 4 lines of code)
    
    # Create an S3 client
    s3_client = boto3.client('s3') # @REPLACE EQUALS None.None('None')

    try: # @KEEP

        # Put object into the S3 bucket
        response = s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue()) # @REPLACE EQUALS s3_client.None(Bucket=None, Key=None, Body=None.None())

        # Return the response
        return response # @REPLACE return None
        
        ### END CODE HERE ###
    
    except Exception as err:
        print(f"Error message: {err}")
        return err.response
```
:::

::: {.cell .markdown}
`<a id='ex07'>`{=html}`</a>`{=html}

### Exercise 7

Use function `upload_pandas_to_s3()` to upload the pandas dataframe `df`
to the same Key used before. Check the response from your function.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
### START CODE HERE ### (~ 2 lines of code)
s3_key = 'data/csv/employees.csv' # @REPLACE EQUALS 'None'

response_upload_pandas = upload_pandas_to_s3(df=df, bucket_name=BUCKET_NAME, s3_key=s3_key) # @REPLACE EQUALS None(df=None, bucket_name=None, s3_key=None)
### END CODE HERE ###

response_upload_pandas
```
:::

::: {.cell .markdown}
Given that you are uploading a different object to the same Key in S3,
that means that you would be overwriting directly that object; but given
that you have enabled the versioning in our object storage you are
actually creating a new version of that object. Let\'s list the versions
of the Objects which have a key that starts with
`data/csv/employees.csv`:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def list_object_versions(bucket_name: str, s3_key: str) -> None:
    """ List versions of S3 objects that start with a particular key

    Args:                
        BUCKET_NAME (str): Bucket name
        s3_key (str): Prefix of the Key (Path) in S3 of the object
    
    """
    
    # Create an S3 client
    s3_client = boto3.client('s3')

    # List object versions
    response = s3_client.list_object_versions(Bucket=bucket_name, Prefix=s3_key)

    # Process the response to get object versions
    for version in response.get('Versions', []):
        print("Object Key:", version['Key'])
        print("Object Version Id:", version['VersionId'])
        print("Is Latest:", version['IsLatest'])
        print("Last Modified:", version['LastModified'])
        print()

list_object_versions(bucket_name=BUCKET_NAME, s3_key=s3_key)
```
:::

::: {.cell .markdown}
Now, let\'s append more data to the dataframe you have been working
with. You can check that the data has been added to the `df` dataframe
by inspecting its shape or by inspecting the last rows of it. Execute
the following cell to do it.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
data = {
    'ID': [1000001, 1000002, 1000003, 1000004, 1000005],
    'Name': ['Kelly Murphy', 'Monica Fernandez', 'Andrew Lang', 'John Mccoy', 'Rachel Brown'],
    'Company': ['Brooks Ltd', 'Brooks Ltd', 'Brooks Ltd', 'Nichols-Webb', 'Nichols-Webb'],
    'Address': ['0344 Berger Camp\nAnthonybury, SC 57484', '12609 Curtis Place Apt. 977\nSimmonsmouth, TN', 'USCGC Johnson\nFPO AE 37537', 'PSC 7850, Box 1744\nAPO AA 32566', '09861 Jessica Drive\nSheamouth, KS 42050'],
    'City': ['South Kara', 'Johnsonmouth', 'Beckermouth', 'Millerton', 'West Kristenside'],
    'Email': ['kmurphy@example.com', 'mon.fer@example.net', 'andrew.lang@example.org', 'j.mccoy@example.com', 'ra.brown@example.com'],
    'Salary': [70000, 120000, 89000, 65000, 105000]
}

df_new = pd.DataFrame(data)

df_updated = pd.concat([df, df_new], ignore_index=True)

df_updated.shape
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df_updated.tail(10)
```
:::

::: {.cell .markdown}
Upload the new `df_updated` with more rows to the S3 bucket, using the
same key and check again the available versions:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
s3_key = 'data/csv/employees.csv'

response = upload_pandas_to_s3(df=df_updated, bucket_name=BUCKET_NAME, s3_key=s3_key)

list_object_versions(bucket_name=BUCKET_NAME, s3_key=s3_key)
```
:::

::: {.cell .markdown}
You can see that there are now 3 different versions, each one identified
by its ID and a Last-Modified date. With that example, you can see that
if you modify or append data to an object stored in an Object Storage,
you will have different versions of the same object, associated with the
same object key. In the case that you want to retrieve an older version,
you can do it by specifying the version ID you want to retrieve in the
`get_object()` method of the `boto3` client that you used in the
`read_csv_from_s3()` function.
:::

::: {.cell .markdown}
`<a id='ex08'>`{=html}`</a>`{=html}

### Exercise 8

Complete the `read_csv_version_from_s3()` function to read a particular
version of the object. Use the [`get_object()`
method](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html)
of the `boto3` client with the Bucket, Key, and VersionId parameters.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def read_csv_version_from_s3(bucket_name: str, s3_key: str, version_id: str) -> pd.DataFrame:
    """Reads a particular version of a csv file stored in an S3 bucket

    Args:        
        BUCKET_NAME (str): Bucket name
        s3_key (str): Key (Path) in S3 of the file
        version_id (str): Object's Version ID
    """
    # Create an S3 client    
    s3_client = boto3.client('s3')

    try:
        
        ### START CODE HERE ### (~ 1 line of code)
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key, VersionId=version_id) # @REPLACE EQUALS s3_client.None(Bucket=None, Key=None, VersionId=None)
        ### END CODE HERE ###
        
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
                
        return df
        
    except Exception as err:
        print(f"Error message: {err}") 
```
:::

::: {.cell .markdown}
Use the `read_csv_version_from_s3` function to read the first version of
your object. Pass the S3 key and the Version ID, replacing the
placeholder `<VERSION-ID>` with the object version ID for the first
version of the object you obtained earlier. Inspect the dataframe and
the number of rows of the read object to check that it is actually the
first version of the object.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
s3_key = 'data/csv/employees.csv'
version_id = '<VERSION-ID>'

df_version_1 = read_csv_version_from_s3(bucket_name=BUCKET_NAME, s3_key=s3_key, version_id=version_id)
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df_version_1.shape
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df_version_1.describe()
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
df_version_1[(df_version_1['Company']=='Brooks Ltd') & (df_version_1['Salary']>120000)]
```
:::

::: {.cell .markdown}
With that, you have seen how Object Storage systems work, by using a Key
and metadata to identify a particular object and by uploading a
completely new version of the object if you perform any kind of
modification or appending data to it.
:::

::: {.cell .markdown}
`<a id='2'>`{=html}`</a>`{=html}

## 2 - File Storage {#2---file-storage}

A **File Storage System**, also known as a file system, is a method used
by operating systems to manage and store data on storage devices such as
hard disk drives (HDDs), solid-state drives (SSDs), and network-attached
storage (NAS) devices. It provides a hierarchical structure for
organizing files and directories, and it includes mechanisms for
accessing, reading, writing, and modifying files.
:::

::: {.cell .markdown}
`<a id='2-1'>`{=html}`</a>`{=html}

### 2.1 - Key Components of File Storage System {#21---key-components-of-file-storage-system}

Here are the key components of File Storage Systems

-   File: A named collection of data stored on a storage device. Files
    can contain various types of data, such as text, documents, images,
    videos, and programs.

-   Directory (or Folder): A container used to organize files into a
    hierarchical structure. Directories can contain files and other
    directories, allowing for the creation of a tree-like directory
    structure.

-   File System Metadata: Information about files and directories stored
    by the file system, including attributes such as file name, size,
    type, permissions, creation/modification timestamps, and file
    location.

-   File System Operations: Actions performed on files and directories,
    such as creating, reading, writing, deleting, renaming, moving,
    copying, and accessing files. These operations are typically
    performed using file system APIs or command-line utilities.

-   File System Drivers: Software components responsible for interacting
    with storage devices and translating file system operations into
    low-level disk operations. These drivers enable the operating system
    to access and manipulate files stored on different types of storage
    media
:::

::: {.cell .markdown}
In a File Storage System, the folder hierarchy organizes files and
directories into a hierarchical structure, like a tree structure:

![image](./images/file_system.png)
:::

::: {.cell .markdown}
In the previous image you can see the tree-like structure for a Linux
file system, where you can find these components:

-   Root Directory: The root directory is at the top of the hierarchy,
    denoted by a forward slash (\"/\") in Unix-based systems or a drive
    letter (e.g., \"C:\") in Windows-based systems. The root directory
    contains all other directories and files.

-   Directories (Folders): Directories, also known as folders, are
    containers used to organize files and other directories. They can
    contain files and subdirectories. Each directory has a unique name
    within its parent directory.

-   Subdirectories: Subdirectories are directories contained within
    other directories. They can further organize files and directories
    into nested structures. A subdirectory\'s path consists of its name
    preceded by the paths of its parent directories, separated by
    slashes.

-   Files: Files are objects that contain data, such as text, images, or
    programs. They are stored within directories and can have unique
    names within their parent directories.

-   Path: A path is a string that specifies the location of a file or
    directory within the hierarchy. It consists of a sequence of
    directory names separated by slashes `/`. The path to a file or
    directory uniquely identifies its location within the folder
    hierarchy.
:::

::: {.cell .markdown}
You could be more accustomed to using File Storage Systems as it is the
default storage system in an Operating System.
:::

::: {.cell .markdown}
`<a id='2-2'>`{=html}`</a>`{=html}

### 2.2 - Features of File Storage System {#22---features-of-file-storage-system}

Let\'s see some of the features of File Storage System.

2.2.1. In the terminal run the following command:
:::

::: {.cell .markdown}
``` bash
ls -alh
```
:::

::: {.cell .markdown}
This will show you several lines, but let\'s focus only on the one
associated with the `data` directory, which should look similar to this:
:::

::: {.cell .markdown}
``` bash
...
drwxrwxrwx  2 nobody nogroup 6.0K   Apr  3 23:11 data
...
```
:::

::: {.cell .markdown}
There, you will see some metadata about this particular folder, such as
the date of creation, the owner of the folder and the permissions. If
you are not familiar with Unix-like systems, here is a brief explanation
of this output:

-   File Permissions (`-alh`): The first column shows the file
    permissions. It consists of ten characters and is divided into four
    parts:

    -   The first character indicates the type of file (e.g., `-` for a
        regular file, `d` for a directory, `l` for a symbolic link).
    -   The next three characters represent the owner\'s permissions:
        read (r), write (w), and execute (x), respectively. If a hyphen
        (-) appears, it means that that particular permission is not
        set.
    -   The next three characters represent the group\'s permissions:
        read (r), write (w), and execute (x), respectively. If a hyphen
        (-) appears, it means that that particular permission is not
        set.
    -   The final three characters represent others\' permissions: read
        (r), write (w), and execute (x), respectively. If a hyphen (-)
        appears, it means that that particular permission is not set.

-   Number of Links: The second column shows the number of hard links to
    the file or directory.

-   Owner: The third column shows the owner of the file or directory.

-   Group: The fourth column shows the group associated with the file or
    directory.

-   File Size: The fifth column shows the size of the file or directory.
    The `-h` option of the command you just typed in the terminal makes
    the file sizes human-readable by using units like KB, MB, GB, etc.,
    instead of just bytes.

-   Modification Time: The sixth column shows the date and time when the
    file or directory was last modified.

-   File/Directory Name: The final column shows the name of the file or
    directory.
:::

::: {.cell .markdown}
Given that, in a File Storage System you can easily identify the
hierarchy of directories/subdirectories vs regular files, which do not
exist in Object Storage, as you saw in the previous section where you do
not have directories/subdirectories but only a Key to identify each
object.
:::

::: {.cell .markdown}
2.2.2. Check the content of the `data` folder:
:::

::: {.cell .markdown}
``` bash
cd data
ls -alh
```
:::

::: {.cell .markdown}
You will see an output similar to this one:
:::

::: {.cell .markdown}
``` bash
total 16K
drwxrwxrwx 2 nobody nogroup 6.0K Oct 30 11:49 .
drwxrwxrwx 7 nobody nogroup 6.0K Oct 30 11:49 ..
lrwxrwxrwx 1 nobody nogroup   41 Oct 30 11:49 employees.csv -> /home/coder/project-ro/data/employees.csv
-rwxrwxrwx 1 coder  coder    636 Oct 30 11:47 employees_sample.csv
```
:::

::: {.cell .markdown}
You can see that the `employees_sample.csv` is a regular file, indicated
by the hyphen at the beginning of its permissions (`-rwxrwxrwx`). The
`employees.csv` is a symbolic link, as indicated by the \"l\" at the
start of its permissions (`lrwxrwxrwx`). This means `employees.csv`
points to another file located at
`/home/coder/project-ro/data/employees.csv`, rather than storing the
data itself (this is due to the file size). The permissions show that
both files have read, write, and execute permissions for all users, and
the ownerships are also specified. There is no metadata indicating any
version history for these files.
:::

::: {.cell .markdown}
2.2.3. Now, let\'s freely modify the file `employees_sample.csv`. Open
the file `./data/employees_sample.csv` file. Change any row value you
like, and save the file (`Cmd + S` or `Ctrl + S`)..
:::

::: {.cell .markdown}
2.2.4. In the terminal, write again the command:
:::

::: {.cell .markdown}
``` bash
ls -alh
```
:::

::: {.cell .markdown}
The output should be quite similar to the previous one that you
obtained, only with a new value for the Modification Date column (and
possible change of the file size). Again, there is no information about
any version as the file is directly modified on the disk.
:::

::: {.cell .markdown}
2.2.5. Open the file `./data/employees_sample.csv` and append the
following new line at the end of the file, then save the file (`Cmd + S`
or `Ctrl + S`):
:::

::: {.cell .markdown}
``` bash
6,Rachel Brown,Nichols-Webb,"09861 Jessica Drive\nSheamouth, KS 42050",West Kristenside,ra.brown@example.com,105000
```
:::

::: {.cell .markdown}
The `ls -alh` command will show a change in the Modification Date and
file size only.
:::

::: {.cell .markdown}
2.2.6. Finally, in a File Storage System you can easily modify the
permissions of any file. For example, if you want to avoid any user
modifying the file, including the owner of the `employees_sample.csv`
file you can use the following command:
:::

::: {.cell .markdown}
``` bash
chmod -w employees_sample.csv
```
:::

::: {.cell .markdown}
2.2.7. Run the `ls` command again:
:::

::: {.cell .markdown}
``` bash
ls -alh
```
:::

::: {.cell .markdown}
The output will be similar to this one:
:::

::: {.cell .markdown}
``` bash
...
-r-xrwxrwx 1 coder  coder    636 Oct 30 11:47 employees_sample.csv
```
:::

::: {.cell .markdown}
Note the difference in the permissions. If you open the file and try to
modify it, such as deleting the last row that you inserted, you will get
an error when you try to save the file.
:::

::: {.cell .markdown}
2.2.8. To make the file modifiable again, you can use the command:
:::

::: {.cell .markdown}
``` bash
chmod +w employees_sample.csv
```
:::

::: {.cell .markdown}
Check that you can edit the file now.
:::

::: {.cell .markdown}
`<a id='3'>`{=html}`</a>`{=html}

## 3 - Memory Storage {#3---memory-storage}

**Memory-based Storage Systems**, also known as in-memory databases or
caching systems, store data primarily in RAM (Random Access Memory)
instead of on disk. These systems are designed to provide fast and
efficient access to data by keeping it in memory, which allows for much
quicker read and write operations compared to traditional disk-based
storage systems.

-   Speed: Since data is stored in memory, which has much faster access
    times compared to disk storage, operations such as reads, writes,
    and queries can be performed very quickly. This makes memory-based
    systems ideal for applications that require low-latency access to
    data, such as real-time analytics, caching, and high-performance
    transaction processing.

-   Caching: One common use case for memory-based storage systems is
    caching frequently accessed data to improve the performance of
    applications. By storing frequently accessed data in memory,
    applications can avoid repeatedly fetching data from slower
    disk-based storage systems, resulting in faster response times and
    reduced load on backend databases.

-   Data Structures: Memory-based storage systems often use specialized
    data structures optimized for fast access and manipulation of data
    in memory. Examples include hash tables, trees, and linked lists.
    These data structures are designed to minimize the overhead of
    memory management and provide efficient access to stored data.

-   Volatility: Unlike disk-based storage, which retains data even when
    the power is turned off, memory-based storage systems are volatile,
    meaning that data is lost when the system is powered down or
    restarted. To mitigate this, some memory-based systems offer
    mechanisms for persistence, such as periodic snapshots or
    replication to disk-based storage for durability.

-   Scalability: Memory-based storage systems can often scale
    horizontally by adding more nodes to distribute the data across
    multiple servers. This allows them to handle large volumes of data
    and high request rates, making them suitable for use in distributed
    environments and cloud computing platforms.

-   Use Cases: Memory-based storage systems are commonly used in various
    applications, including web servers, content delivery networks
    (CDNs), session stores, real-time analytics platforms, and
    high-frequency trading systems. Any application that requires fast
    access to data or benefits from caching frequently accessed data can
    benefit from using memory-based storage.

Overall, memory-based storage systems offer significant performance
advantages over disk-based storage systems, making them well-suited for
applications that demand low-latency access to data and high throughput.
However, they may also have limitations in terms of data durability and
storage capacity compared to disk-based alternatives, so their use
should be carefully considered based on the specific requirements of the
application.

Although in this lab you are not going to work directly with an
in-memory database, the idea is to give you some insight into certain
features of those systems.
:::

::: {.cell .markdown}
`<a id='3-1'>`{=html}`</a>`{=html}

### 3.1 - Caching and Volatility {#31---caching-and-volatility}

As an example of the caching feature of memory storage, you will explore
the `timed_lru_cache` decorator from the `cache-pandas` package. It
allows you to store in memory the resulting dataframe from a function.
You will compare the time it takes to read the `data/employees.csv` file
the first time with the time it takes to read the same file after it is
stored in memory. Here is a function `read_csv_to_memory` that uses the
`timed_lru_cache` decorator. Run the following cell to see how long it
takes to read the file the first time:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
@timed_lru_cache(seconds=100, maxsize=None)
def read_csv_to_memory(path: str) -> pd.DataFrame:
    """Read CSV function with a cache decorator."""
    return pd.read_csv(path)

start = timeit.default_timer()
df = read_csv_to_memory('data/employees.csv')
end = timeit.default_timer()
print(f'Elapsed time: {end - start} seconds')
```
:::

::: {.cell .markdown}
Read the same file using the `read_csv_to_memory` function and store it
into the `df_cache` dataframe. You\'ll notice that it\'s much quicker to
read the file when it\'s stored in memory.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
start = timeit.default_timer()
df_cache = read_csv_to_memory('data/employees.csv')
end = timeit.default_timer()
print(f'Elapsed time: {end - start} seconds')
```
:::

::: {.cell .markdown}
Wait two minutes, the cache should have cleared, then run the function
again with the cell after the line. You\'ll notice that it takes longer
to read the file again once memory from the cache has been cleared:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
start = timeit.default_timer()
df_after = read_csv_to_memory('data/employees.csv')
end = timeit.default_timer()
print(f'Elapsed time: {end - start} seconds')
```
:::

::: {.cell .markdown}
`<a id='3-2'>`{=html}`</a>`{=html}

### 3.2 - Memory Storage Capacity {#32---memory-storage-capacity}

When caching data in memory storage, you should monitor your memory
storage capacity so that you don\'t exceed it. You can check how the
memory usage changes using the terminal command `htop`.

![image](./images/memory_usage.png)

As you continue with the next few steps to store new data in memory and
clear memory from the cache, you can revisit this terminal to see how
the memory usage changes. Press the `Q` key in the terminal to exit this
view.
:::

::: {.cell .markdown}
3.2.1. First, you are going to define a function to check on the
available memory storage.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
def get_memory_usage():
    """
    Get total memory and memory usage
    """
    with open('/proc/meminfo', 'r') as mem:
        ret = {}
        tmp = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) == 'MemTotal:':
                ret['total'] = int(sline[1])
            elif str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                tmp += int(sline[1])
        ret['free'] = tmp
        ret['used'] = int(ret['total']) - int(ret['free'])
        ret['percentage'] = int(ret['used'])/int(ret['total'])
    return ret

print(get_memory_usage())
```
:::

::: {.cell .markdown}
3.2.2. Now delete the dataframes running the following cell:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
del df
del df_cache
del df_after
del df_new
del df_updated
del df_version_1
```
:::

::: {.cell .markdown}
3.2.3. Check the memory usage again:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
print(get_memory_usage())
```
:::

::: {.cell .markdown}
Understanding memory storage, its capacity, and how to optimize memory
usage is critical for maximizing performance. Monitoring tools can
provide insights into memory usage patterns, helping you identify
memory-intensive processes or leaks. Optimization strategies include
efficient memory management techniques, caching mechanisms and resource
management. By implementing these strategies, you can improve overall
pipeline performance, minimize resource wastage, and ensure a smooth
computing experience.
:::

::: {.cell .markdown}
`<a id='4'>`{=html}`</a>`{=html}

## 4 - Block Storage {#4---block-storage}

**Block storage** is a type of data storage method commonly used in
computing systems, particularly in enterprise environments and cloud
computing infrastructures. In block storage, data is stored in
fixed-sized units called blocks, each with a unique identifier, which
are typically managed by a storage area network (SAN) or server-based
operating systems. Additionally, Hard Disk Drives (HDD) and Solid State
Drives (SSD) that are attached to a computer physically or via a network
are examples of block storage devices.

One of the key features of block storage is direct access to individual
blocks of data. This means that applications can read from or write to
specific blocks without having to access the entire file or dataset.
This direct access enables faster data retrieval and manipulation,
making block storage particularly suitable for applications that require
high-performance storage and computing. Examples include databases,
virtual machine storage, and enterprise storage systems.
:::

::: {.cell .markdown}
![image](./images/block_storage.png)
:::

::: {.cell .markdown}
While block storage offers high performance through direct data access,
it introduces complexity in terms of deployment, management, and
maintenance, often requiring specialized expertise and inciting higher
expenses. Additionally, block storage lacks inherent awareness of file
structures, necessitating additional overhead for data organization
within applications or file systems.

To simulate a block storage system, you have a simplified block storage
server in an EC2 instance. Using a Python class called
`BlockStorageClient` you will be able to connect to the server and send
and receive files. When you send a file, the file will be divided into
similar size blocks and each one will be sent to the server. For this
exercise, you will upload the files located in the `data` folder to the
server and then check that they were uploaded.
:::

::: {.cell .markdown}
Run the following code to get the link to the AWS console.

*Note*: For security reasons, the URL to access the AWS console will
expire every 15 minutes. Please, rerun this code cell to obtain a new
active link.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
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
3.1. In the AWS console go to **CloudFormation**. Click on the
alphanumeric stack name and navigate to the **Outputs** tab. Copy the
value of the `BlockInstanceDNS` key (**without the colon and port at the
end of it**). Replace the placeholder `<SERVER-IP>` in the following
cell with the value you just copied to instantiate the
`BlockStorageClient`.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
server_ip = '<SERVER-IP>'
server_port = 9090
```
:::

::: {.cell .markdown}
Run this cell just once to connect to the server.

*Note*: If the connection is refused, go to the terminal and run the
script to restart the server. Then try to connect again in a few
minutes.

``` bash
cd ..
source scripts/restart_server.sh
```
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
client = BlockStorageClient(server_ip, server_port)
client.connect()
```
:::

::: {.cell .markdown}
3.2. Upload the first file to the block storage server using the
`send_file` function.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
client.send_file('data/employees.csv')
```
:::

::: {.cell .markdown}
3.3. Move to the `src/client_blocks` folder and list the files and
folders. You\'ll see how the `data/employee.csv` file is divided into
blocks of similar sizes.
:::

::: {.cell .markdown}
``` bach
cd ~/project/src/client_blocks/employees
ls -alh
```
:::

::: {.cell .markdown}
3.4. Upload the second file to the server
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
client.send_file('data/employees_sample.csv')
```
:::

::: {.cell .markdown}
3.5. You can list the files in the block storage server using the
`list_files` function.

*Note*: If you encounter an empty list when running the cell in step
3.5, or an error stating that the file doesn\'t exist in step 3.6, it is
likely due to a connection issue. To resolve this, please rerun the
cells in steps 3.1 and 3.2 to re-establish the connection.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
file_list = client.list_files()
print(file_list)
```
:::

::: {.cell .markdown}
3.6. Now, retrieve one of the files from the server, using the
`receive_file` function. Then check that the file has been received in
the `client_files` folder, along with the blocks in `client_blocks`
folder.

*Note*: You may need to click on **Enable Downloads** in the pop up
window.
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
client.receive_file("employees.csv")
```
:::

::: {.cell .markdown}
3.7. Finally, after you are done with the requests to the server, close
the connection with the server:
:::

::: {.cell .code vscode="{\"languageId\":\"python\"}"}
``` python
client.close()
```
:::

::: {.cell .markdown}
This is just a simulation of using block storage over the network, there
are enterprise-ready and cloud solutions that improve the speed and
reliability of file transfer making it seem seamless to the users as
they can connect from any operating system and use the solution as an
external storage.
:::

::: {.cell .markdown}
In conclusion, the choice between object, file, memory, and block
storage hinges on weighing their respective advantages and disadvantages
to best suit the needs of Data Engineering projects:

-   Object storage shines with its scalability, cost-effectiveness, and
    simplicity, making it ideal for scenarios requiring vast amounts of
    unstructured data such as backups, archives, and content delivery
    networks.
-   File storage offers ease of access and organization, making it
    suitable for shared file systems and applications reliant on
    hierarchical data structures like file servers.
-   Memory storage, with its lightning-fast access speeds, is
    indispensable for real-time data processing and caching tasks,
    particularly in-memory databases and high-performance computing
    environments.
-   Block storage, providing direct access to data blocks, excels in
    performance-critical applications such as databases, virtualization
    platforms, and enterprise storage systems.

Data Engineers must carefully assess the specific requirements of their
projects to determine the most suitable storage solution, leveraging the
unique strengths of each type to optimize performance, scalability, and
cost efficiency.
:::

::: {.cell .markdown}
:::
