import streamlit as st
import pandas as pd
import io
import boto3
import requests
from datetime import datetime
import uuid
import logging
from botocore.config import Config





delay = 10 * 60 * 60



@st.cache_resource
def get_session_info(region, env_name):
    
    logging.basicConfig(level=logging.INFO)

    try:
        # Initialize MWAA client and request a web login token
        my_config = Config(
        region_name = 'us-east-2',
        signature_version = 'v4',
        retries = {
        'max_attempts': 10,
        'mode': 'standard'
        }
        )
        mwaa = boto3.client('mwaa', region_name=region, config=my_config)
        print("before Response", mwaa)
        try:
            response = mwaa.create_web_login_token(Name=env_name)
        except Exception as error:
            print("Exception" , error)
        print("after Response")
        # Extract the web server hostname and login token
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"] 
        print("web_server_host_name", web_server_host_name)
        print("web_token", web_token)
        # Construct the URL needed for authentication 
        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}

        # Make a POST request to the MWAA login url using the login payload
        response = requests.post(
            login_url,
            data=login_payload,
            timeout=10
        )

        print("response.status_code", response.status_code)

        # Check if login was succesfull 
        if response.status_code == 200:
        
            # Return the hostname and the session cookie 
            # session_cookie_global = response.cookies["session"]
            
            if 'session_cookie_global' not in st.session_state:
                print("there is no session_cookie_global in the st.session state setting that in")
                st.session_state.session_cookie_global = 'Tempo'
                
            
            st.session_state.session_cookie_global = response.cookies["session"]
            print("st.session_state['session_cookie_global'] in the get function", st.session_state.session_cookie_global)


            if 'web_server_host_name_global' not in st.session_state:
                st.session_state['web_server_host_name_global'] = 'Temp_webHost'
            
            st.session_state["web_server_host_name_global"] = web_server_host_name

            
            return (
                web_server_host_name,
                response.cookies["session"]
            )
        else:
            # Log an error
            print("Failed to log in: HTTP %d", response.status_code)
            return None
    except requests.RequestException as e:
         # Log any exceptions raised during the request to the MWAA login endpoint
        logging.error("Request failed: %s", str(e))
        return None
    except Exception as e:
        # Log any other unexpected exceptions 
        logging.error("An unexpected error occurred: %s", str(e))
        return None
    



# S3 bucket details
bucket_name = 'awsairflowbucket'
file_key_X = 'updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx'
file_key_Y = 'Y_New_Processed.xlsx'

# Airflow API details
airflow_base_url = 'https://f6bdcad4-4be8-4e94-ab99-4c057b089cbb.c7.us-east-2.airflow.amazonaws.com/api/v1'
username = 'wstorey@theoremlabs.io'
password = 'Theorem1ab$'
auth = (username, password)
headers = {"Content-type": "application/json"}

# Function to read Excel file from S3
def read_excel_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_excel(io.BytesIO(response['Body'].read()), engine='openpyxl')
    return df

# Read Excel files
df = read_excel_from_s3(bucket_name, file_key_X)
df1 = read_excel_from_s3(bucket_name, file_key_Y)

def convert_df_to_excel(df, sheet_name):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    processed_data = output.getvalue()
    return processed_data

def get_dags(session_cookie, web_server_host_name):

    # web_server_host_name = "f6bdcad4-4be8-4e94-ab99-4c057b089cbb.c7.us-east-2.airflow.amazonaws.com"
    # session_cookie = "973b6fce-1562-4f6d-8ba4-f455ddc5ca4f.5Fh8PwFyfBVZmvs8WzNzRzOuQvE"

    print("Its a session cookie in the get_dags", session_cookie)
    print("hostName int the get_dags", web_server_host_name)
    # Prepare headers and payload for the request
    cookies = {"session": session_cookie}
    json_body = {"conf": {}}

    # Construct the URL for triggering the DAG
    url = f"https://{web_server_host_name}/api/v1/dags" #/{dag_id}/dagRuns"

    # Send the POST request to trigger the DAG
    try:
        response = requests.get(url, cookies=cookies) #, json=json_body)
        # Check the response status code to determine if the DAG was triggered successfully
        if response.status_code == 200:
            # print("DAG triggered successfully.", response.json())
            return response.json()
        else:
            print(f"Failed to trigger DAG: HTTP {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"Request to trigger DAG failed: {str(e)}")



def run_dag(dag_id):
    url = f"{airflow_base_url}/dags/{dag_id}/dagRuns"
    response = requests.post(url, headers=headers, auth=auth)
    if response.status_code == 200:
        st.success("DAG run successfully triggered!")
    else:
        st.error("Failed to trigger DAG run.")



def trigger_dag(dag_id, session_cookie, web_server_host_name):
    url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}/dagRuns"
    dag_run_id = str(uuid.uuid4())
    logical_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "conf": {},
        "dag_run_id": dag_run_id,
        # "data_interval_end": logical_date,
        # "data_interval_start": logical_date,
        "execution_date": logical_date,
        "logical_date": logical_date,
        "note": "string"
    }
    cookies = {"session": session_cookie}
    response = requests.post(url, headers=headers, cookies=cookies, json=payload)
    if response.status_code == 200:
        st.success("DAG run successfully triggered!")
    else:
        st.error("Failed to trigger DAG run.")

def main():
    
    st.title("DAGs Dashboard")
    web_server_host_name, session_cookie = get_session_info("us-east-2", "MyAirflowEnvironment")

    show_dags = st.button("Show DAGs")
    if show_dags:
        print("session_cookie_global in Main", session_cookie)
        # url = f"https://{web_server_host_name}/api/v1/dags"
        dags = get_dags(session_cookie, web_server_host_name)
        st.write("## Available DAGs:")
        for dag in dags["dags"]:
            st.write(f"**DAG ID:** {dag['dag_id']}")

    st.title("Famiology Compute Engine")
    col1, col2 = st.columns(2)

    with col1:
        x_dataset = st.checkbox("Connect to X dataset")
        y_dataset = st.checkbox("Connect to Y dataset")

    with col2:
        x_file = st.checkbox("Processed X file")
        y_file = st.checkbox("Processed Y file")

        show = st.button("Generate Results")
        if show:
            if x_file and x_dataset:
                data = convert_df_to_excel(df, 'Client Profile')
                print("Inside X dataset")
                # url = f"{airflow_base_url}/dags/streamlit_app/dagRuns"
                trigger_dag("updated_dag2", session_cookie, web_server_host_name)
                st.success("DAG triggered successfully!")
                st.markdown("<h2 style='text-align: center;'>Download Updated CSV</h2>", unsafe_allow_html=True)
                st.download_button(
                    label="Press to Download X processed file",
                    data=data,
                    file_name="Processed_X_file.xlsx"
                )
            if y_file and y_dataset:
                data = convert_df_to_excel(df1, 'Family Members')
                print("Inside Y dataset")
                url = f"{airflow_base_url}/dags/streamlit_app/dagRuns"
                trigger_dag("updated_dag2", session_cookie, web_server_host_name)
                st.download_button(
                    label="Press to Download Y processed file",
                    data=data,
                    file_name="Processed_Y_file.xlsx"
                )
                st.success("DAG 'Processed_Y_file' triggered successfully!")

if __name__ == "__main__":
    main()

























# import streamlit as st
# import pandas as pd
# import io
# import boto3
# import requests
# from datetime import datetime
# import uuid
# import logging
# from botocore.config import Config

# import threading
# import time

# # Global variables for session information
# session_cookie_global = ""
# web_server_host_name_global = ""

# # Delay for refreshing session (10 hours)
# delay = 10 * 60 * 60

# def get_session_info(region, env_name):
#     global session_cookie_global, web_server_host_name_global
#     logging.basicConfig(level=logging.INFO)

#     try:
#         # Initialize MWAA client and request a web login token
#         my_config = Config(
#             region_name=region,
#             signature_version='v4',
#             retries={
#                 'max_attempts': 10,
#                 'mode': 'standard'
#             }
#         )
#         mwaa = boto3.client('mwaa', region_name=region, config=my_config)
#         response = mwaa.create_web_login_token(Name=env_name)

#         # Extract the web server hostname and login token
#         web_server_host_name = response["WebServerHostname"]
#         web_token = response["WebToken"]
        
#         # Construct the URL needed for authentication 
#         login_url = f"https://{web_server_host_name}/aws_mwaa/login"
#         login_payload = {"token": web_token}

#         # Make a POST request to the MWAA login url using the login payload
#         response = requests.post(login_url, data=login_payload, timeout=10)

#         # Check if login was successful
#         if response.status_code == 200:
#             # Return the hostname and the session cookie 
#             session_cookie_global = response.cookies["session"]
#             web_server_host_name_global = web_server_host_name
#             return (
#                 web_server_host_name,
#                 session_cookie_global
#             )
#         else:
#             logging.error("Failed to log in: HTTP %d", response.status_code)
#             return None
#     except requests.RequestException as e:
#         logging.error("Request failed: %s", str(e))
#         return None
#     except Exception as e:
#         logging.error("An unexpected error occurred: %s", str(e))
#         return None

# def refresh_session_cookie_task():
#     while True:
#         get_session_info("us-east-2", "MyAirflowEnvironment")
#         time.sleep(delay)

# # Create a thread to run the delayed task
# thread = threading.Thread(target=refresh_session_cookie_task)
# thread.start()

# # S3 bucket details
# bucket_name = 'awsairflowbucket'
# file_key_X = 'updatedX_sheet_FamilyOfficeEntityDataSampleV1.1.xlsx'
# file_key_Y = 'Y_New_Processed.xlsx'

# # Airflow API details
# airflow_base_url = 'https://f6bdcad4-4be8-4e94-ab99-4c057b089cbb.c7.us-east-2.airflow.amazonaws.com/api/v1'
# username = 'wstorey@theoremlabs.io'
# password = 'Theorem1ab$'
# auth = (username, password)
# headers = {"Content-type": "application/json"}

# def read_excel_from_s3(bucket_name, file_key):
#     s3 = boto3.client('s3')
#     response = s3.get_object(Bucket=bucket_name, Key=file_key)
#     df = pd.read_excel(io.BytesIO(response['Body'].read()), engine='openpyxl')
#     return df

# df = read_excel_from_s3(bucket_name, file_key_X)
# df1 = read_excel_from_s3(bucket_name, file_key_Y)

# def convert_df_to_excel(df, sheet_name):
#     output = io.BytesIO()
#     with pd.ExcelWriter(output, engine='openpyxl') as writer:
#         df.to_excel(writer, index=False, sheet_name=sheet_name)
#     processed_data = output.getvalue()
#     return processed_data

# def get_dags(session_cookie, web_server_host_name):
#     cookies = {"session": session_cookie}
#     url = f"https://{web_server_host_name}/api/v1/dags"

#     try:
#         response = requests.get(url, cookies=cookies)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             logging.error(f"Failed to get DAGs: HTTP {response.status_code} - {response.text}")
#             return None
#     except requests.RequestException as e:
#         logging.error(f"Request to get DAGs failed: {str(e)}")
#         return None

# def run_dag(dag_id):
#     url = f"{airflow_base_url}/dags/{dag_id}/dagRuns"
#     response = requests.post(url, headers=headers, auth=auth)
#     if response.status_code == 200:
#         st.success("DAG run successfully triggered!")
#     else:
#         st.error("Failed to trigger DAG run.")

# def trigger_dag(dag_id, session_cookie, web_server_host_name):
#     web_server_host_name, session_cookie = get_session_info(region, env_name)
#     url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}/dagRuns"
#     dag_run_id = str(uuid.uuid4())
#     logical_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#     payload = {
#         "conf": {},
#         "dag_run_id": dag_run_id,
#         "execution_date": logical_date,
#         "logical_date": logical_date,
#         "note": "string"
#     }
#     cookies = {"session": session_cookie}
#     try:
#         response = requests.post(url, headers=headers, cookies=cookies, json=payload)
#         if response.status_code == 200:
#             st.success("DAG run successfully triggered!")
#         else:
#             st.error("Failed to trigger DAG run.")
#     except requests.RequestException as e:
#         st.error(f"Request to trigger DAG failed: {str(e)}")

# def main():
#     st.title("DAGs Dashboard")

#     show_dags = st.button("Show DAGs")
#     if show_dags:
#         dags = get_dags(session_cookie_global, web_server_host_name_global)
#         if dags:
#             st.write("## Available DAGs:")
#             for dag in dags["dags"]:
#                 st.write(f"**DAG ID:** {dag['dag_id']}")

# st.title("Famiology Compute Engine")
# col1, col2 = st.columns(2)

# with col1:
#     x_dataset = st.checkbox("Connect to X dataset")
#     y_dataset = st.checkbox("Connect to Y dataset")

# with col2:
#     x_file = st.checkbox("Processed X file")
#     y_file = st.checkbox("Processed Y file")

# show = st.button("Generate Results")
# if show:
#     if x_file and x_dataset:
#         data = convert_df_to_excel(df, 'Client Profile')
#         if web_server_host_name_global and session_cookie_global:
#             trigger_dag("updated_dag2", session_cookie_global, web_server_host_name_global)
#             st.success("DAG triggered successfully!")
#             st.markdown("<h2 style='text-align: center;'>Download Updated CSV</h2>", unsafe_allow_html=True)
#             st.download_button(
#                 label="Press to Download X processed file",
#                 data=data,
#                 file_name="Processed_X_file.xlsx"
#             )
#     if y_file and y_dataset:
#         data = convert_df_to_excel(df1, 'Family Members')
#         if web_server_host_name_global and session_cookie_global:
#             trigger_dag("updated_dag2", session_cookie_global, web_server_host_name_global)
#             st.download_button(
#                 label="Press to Download Y processed file",
#                 data=data,
#                 file_name="Processed_Y_file.xlsx"
#             )
#             st.success("DAG 'Processed_Y_file' triggered successfully!")

# if __name__ == "__main__":
#     main()
