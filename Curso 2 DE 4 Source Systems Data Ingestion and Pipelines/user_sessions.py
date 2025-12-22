import datetime as dt
import json
import logging
from random import randint

import boto3
import requests
from airflow import DAG
from airflow.operators.dummy import EmptyOperator
from airflow.operators.python import PythonOperator

############################# START OF EXERCISE 1 #############################

### START CODE HERE ### (1 line of code)
# Assign the name of your _Raw Data Bucket_ to `DATA_BUCKET` constant
# replacing the placeholder `<RAW-DATA-BUCKET>`
RAW_DATA_BUCKET = "<RAW-DATA-BUCKET>"
### END CODE HERE ###

### START CODE HERE ### (1 line of code)
# Instantiate a boto3 client using method `boto3.client()`, establishing a
# connection with `s3`
client = boto3.client("s3")
### END CODE HERE ###

logger = logging.getLogger()
logger.setLevel("INFO")

# Define the directed acyclic graph:
with DAG(
    dag_id="user_sessions",
    ### START CODE HERE ### (1 line of code)
    # Set the `start_date` as a `datetime` object representing a
    # date 7 days before the date 2020-10-17. This can be done using the function
    # `dt.timedelta()` with the parameter `days` equal to `7`
    start_date=dt.datetime(2020, 10, 17) - dt.timedelta(days=7),
    ### END CODE HERE ###
    end_date=dt.datetime(2020, 10, 17),
    schedule="@daily",
    # Setting `catchup=True` (which is the default) in combination with a
    # `start_date` in the past means that, immediately after the DAG is
    # activated, Airflow will execute all the DAG runs corresponding to the
    # days between `start_date` and the current date. You'll take advantage of
    # that to have the DAG automatically run a few times as soon as you
    # activate it:
    catchup=True,
    # max_active_runs defines how many running concurrent instances of a DAG there are allowed to be
    # running. In this case it is set to 1. This means that the catchup will be done
    # day by day.
    max_active_runs=1,
) as dag:
    ############################## END OF EXERCISE 1 ##############################

    start_task = EmptyOperator(task_id="start")

    ############################# START OF EXERCISE 2 #############################

    def get_new_users(**context):

        # `context['ds']` will correspond to the
        # logical date of the DAG run (the date when it was scheduled to run).
        # Setting `start_date` to `context['ds']` indicates that data will be
        # extracted starting at the same date as the DAG is scheduled.
        start_date = f"{context['ds']}"
        # `context["data_interval_end"]` is another built-in variable in
        # Airflow's task context, representing the end of the interval for
        # which the DAG is being executed. It is a datetime object.
        # The strftime("%Y-%m-%d") method formats this datetime object
        # to a string in the YYYY-MM-DD format, making it suitable for use
        # in the API URL.
        end_date = context["data_interval_end"].strftime("%Y-%m-%d")

        # Make a call to the API to retrieve information
        # about the new users between the start_date and end_date.

        ### START CODE HERE ### (1 line of code)
        # In CloudFormation, search for the Output value associated with
        # APIEndpoint to fill the `<API_ENDPOINT>` placeholder.
        response = requests.get(
            f"http://<API_ENDPOINT>/users?start_date={start_date}&end_date={end_date}"
        )
        ### END CODE HERE ###

        assert response.status_code == 200, response.reason

        # Define the name of the object to be created with the initial
        # information about new users:
        file_name = f"new_users_{start_date}.json"

        logger.info(f"Saving file: {file_name}")

        ### START CODE HERE ### (5 lines of code)
        # Call the `put_object` method of the boto3 client by passing the
        # Raw Data Bucket constant to the Bucket parameter, and the content of the
        # API response to the Body parameter with the code `response.content`
        client.put_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"new_users/{start_date}/{file_name}",
            Body=response.content,
        )
        ### END CODE HERE ###
        logger.info(f"File: {file_name} saved successfully")

    # Define the task that fetches the initial information about new users
    # and stores it in S3:
    get_new_users_task = PythonOperator(
        task_id="get_new_users",
        ### START CODE HERE ### (1 line of code)
        # Pass `get_new_users` function which you defined above
        python_callable=get_new_users,
        ### END CODE HERE ###
        retries=3,  # retry the task in case of failure
        retry_delay=dt.timedelta(seconds=1),
    )

    ############################## END OF EXERCISE 2 ##############################

    ############################# START OF EXERCISE 3 #############################

    def get_session(**context):

        # Make a call to the API to retrieve information
        # about the sessions between the start_date and end_date.
        start_date = f"{context['ds']}"
        end_date = context["data_interval_end"].strftime("%Y-%m-%d")

        ### START CODE HERE ### (1 line of code)
        # In CloudFormation, search for the Output value associated with
        # APIEndpoint to fill the `<API_ENDPOINT>` placeholder.
        response = requests.get(
            f"http://<API_ENDPOINT>/sessions?start_date={start_date}&end_date={end_date}"
        )
        ### END CODE HERE ###

        assert response.status_code == 200, response.reason

        # Define the name of the object to be created with the initial
        # information about the sessions.
        file_name = f"sessions_{start_date}.json"

        logger.info(f"Saving file: {file_name}")

        ### START CODE HERE ### (5 lines of code)
        # Call the `put_object` method of the boto3 client by passing the
        # Raw Data Bucket constant to the Bucket parameter, and the content of the
        # API response to the Body parameter with the code `response.content`
        client.put_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"sessions/{start_date}/{file_name}",
            Body=response.content,
        )
        ### END CODE HERE ###
        logger.info(f"File: {file_name} saved successfully")

    # Define the task that fetches the initial information about a session
    # and stores it in S3:
    get_session_task = PythonOperator(
        task_id="get_session",
        ### START CODE HERE ### (1 line of code)
        # Pass `get_session` function which you defined above
        python_callable=get_session,
        ### END CODE HERE ###
        retries=3,  # retry the task in case of failure
        retry_delay=dt.timedelta(seconds=1),
    )

    ############################## END OF EXERCISE 3 ##############################

    ############################# START OF EXERCISE 4 #############################

    def get_session_info_dict(date: str):
        """
        Fetches the contents of the session information file for the given date
        as a Python dictionary.
        """
        # Remember that the name of the file is of the form
        # "sessions_<LOGICAL_DATE>.json":
        session_info_file_name = f"sessions_{date}.json"

        logger.info(f"Reading the file: {session_info_file_name}")

        ### START CODE HERE ### (4 lines of code)
        # Get the object corresponding to the file using method `get_object()`.
        # Pass the Raw Data Bucket name to the Bucket parameter
        # and the Key with the complete location of the
        # "session_<LOGICAL_DATE>.json" file in the bucket:                
        session_info_file = client.get_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"sessions/{date}/{session_info_file_name}",
        )
        ### END CODE HERE ###

        logger.info(f"File read: {session_info_file_name}")

        assert (
            session_info_file is not None
        ), f"The file {RAW_DATA_BUCKET}/sessions/{date}/{session_info_file_name} does not exist"

        ### START CODE HERE ### (1 line of code)
        # Use the `"Body"` key in the response dictionary from the client
        # `get_object` method. Then, use the `read()` method to obtain
        # the context of the initial information file as a string:
        session_info_string = session_info_file["Body"].read()
        ### END CODE HERE ###

        # Parse the contents of the initial information file, which is JSON, as
        # a Python dictionary:
        session_info = json.loads(session_info_string)

        return session_info

    ############################## END OF EXERCISE 4 ##############################

    ############################# START OF EXERCISE 5 #############################

    def get_user_info(**context):

        ### START CODE HERE ### (1 line of code)
        # Read the initial information about the sessions for the
        # corresponding day using the `get_session_info_dict` function.
        # Remember that the DAG's logical date can be obtained from
        # `context["ds"]`:
        session_info = get_session_info_dict(context["ds"])
        ### END CODE HERE ###

        # Initialize the list that will contain the users' information:
        user_info = []

        # Fill the list users with the information retrieved from the API.
        ### START CODE HERE ### (1 line of code)
        # In CloudFormation, search for the Output value associated with
        # APIEndpoint to fill the `<API_ENDPOINT>` placeholder.

        for session in session_info:
            user_id = session["user_id"]
            print(f"Getting data from ")
            response = requests.get(
                f"http://<API_ENDPOINT>/users_by_id?user_ids={user_id}"
            )
            user_info.append(response.json()[0])
        ### END CODE HERE ###

        # Serialize the list of authors' names as a string representing a JSON
        # array:
        user_info_string = json.dumps(user_info)

        # Construct the users' information file name
        users_file_name = f"user_info_{context['ds']}.json"

        logger.info(f"Saving file: {users_file_name}")

        ### START CODE HERE ### (5 lines of code)
        # Call the `put_object` method of the boto3 client by passing the
        # Raw Data Bucket name to the Bucket parameter, and
        # the user_info_string to the Body parameter
        client.put_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"user_info/{context['ds']}/{users_file_name}",
            Body=user_info_string,
        )
        ### END CODE HERE ###

        logger.info(f"File: {users_file_name} saved")

    get_users_info_task = PythonOperator(
        task_id="get_users_info",
        ### START CODE HERE ### (1 line of code)
        # Pass the `get_user_info` to the `python_callable` parameter
        python_callable=get_user_info,
        ### END CODE HERE ###
    )

    ############################## END OF EXERCISE 5 ##############################

    ############################# START OF EXERCISE 6 #############################

    def save_complete_session(**context):

        ### START CODE HERE ### (1 line of code)
        # Read the initial information about the sessions for the
        # corresponding day using the `get_session_info_dict` function.
        # Remember that the DAG's logical date can be obtained from
        # `context["ds"]`:
        session_info = get_session_info_dict(context["ds"])
        ### END CODE HERE ###

        # Read the information about the users as list of strings:
        users_file_name = f"user_info_{context['ds']}.json"
        users_object = client.get_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"user_info/{context['ds']}/{users_file_name}",
        )
        assert users_object is not None

        users = json.loads(users_object["Body"].read())

        # Convert the list of users to a dictionary for quick lookup
        user_dict = {user["user_id"]: user for user in users}

        # Merge session data with user data
        merged_data = []
        for session in session_info:
            user_id = session["user_id"]
            user_data = user_dict.get(user_id, {})
            user_data_filtered = {
                key: value
                for key, value in user_data.items()
                if key != "user_id"
            }

            # Create a new merged entry
            merged_entry = {**session, **user_data_filtered}

            merged_data.append(merged_entry)

        # Convert to JSON
        complete_sessions_json = json.dumps(merged_data)

        # Prepare the book record file name
        complete_sessions_file_name = f"complete_sessions_{context['ds']}.json"

        ### START CODE HERE ### (5 lines of code)
        # Call the `put_object` method of the boto3 client by passing the
        # Raw Data Bucket name to the Bucket parameter, and the
        # `complete_sessions_json` to the Body parameter
        client.put_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"complete_sessions/{context['ds']}/{complete_sessions_file_name}",
            Body=complete_sessions_json,
        )
        ### END CODE HERE ###

    save_complete_session_task = PythonOperator(
        task_id="save_complete_session",
        ### START CODE HERE ### (1 line of code)
        # Pass the `save_complete_session` to the `python_callable` parameter
        python_callable=save_complete_session,
        ### END CODE HERE ###
    )

    ############################## END OF EXERCISE 6 ##############################

    ############################# START OF EXERCISE 7 #############################

    def clean_up_intermediate_info(**context):
        # Delete the initial information file of the logical date by using the
        # `delete_object` method of the boto3 client by passing the bucket
        # name and Key with the path to the object:
        session_info_file_name = f"sessions_{context['ds']}.json"

        client.delete_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"sessions/{context['ds']}/{session_info_file_name}",
        )

        users_file_name = f"user_info_{context['ds']}.json"

        ### START CODE HERE ### (4 lines of code)
        # Delete the users' info file of the logical date by using the
        # `delete_object` method of the boto3 client by passing the bucket
        # name and Key with the path to the object:
        client.delete_object(
            Bucket=RAW_DATA_BUCKET,
            Key=f"user_info/{context['ds']}/{users_file_name}",
        )
        ### END CODE HERE ###

    cleanup_task = PythonOperator(
        task_id="cleanup",
        ### START CODE HERE ### (1 line of code)
        # Pass the `clean_up_intermediate_info` to the `python_callable` parameter
        python_callable=clean_up_intermediate_info,
        ### END CODE HERE ###
    )

    ############################## END OF EXERCISE 7 ##############################

    ############################# START OF EXERCISE 8 #############################

    ### START CODE HERE ### (1 line of code)
    # Define the `end` task as a dummy operator with the `task_id`
    # equal to `"end"`:
    end_task = EmptyOperator(task_id="end")
    ### END CODE HERE ###

    ############################## END OF EXERCISE 8 ##############################

    ############################# START OF EXERCISE 9 #############################

    ### START CODE HERE ### (~ 4 lines of code)
    # Define the task dependencies with the `>>` operator to obtain the desired
    # DAG:
    start_task >> [get_new_users_task, get_session_task]
    get_session_task >> get_users_info_task >> save_complete_session_task
    [get_new_users_task, save_complete_session_task] >> cleanup_task
    cleanup_task >> end_task
    ### END CODE HERE ###

############################## END OF EXERCISE 9 ##############################
