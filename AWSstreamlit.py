import streamlit as st
import pandas as pd
import io
import boto3
import requests
from datetime import datetime
import uuAWs

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

def get_dags(url):
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()

def run_dag(dag_id):
    url = f"{airflow_base_url}/dags/{dag_id}/dagRuns"
    response = requests.post(url, headers=headers, auth=auth)
    if response.status_code == 200:
        st.success("DAG run successfully triggered!")
    else:
        st.error("Failed to trigger DAG run.")

def trigger_dag(url):
    dag_run_id = str(uuid.uuid4())
    logical_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "conf": {},
        "dag_run_id": dag_run_id,
        "data_interval_end": logical_date,
        "data_interval_start": logical_date,
        "logical_date": logical_date,
        "note": "string"
    }
    response = requests.post(url, headers=headers, auth=auth, json=payload)
    if response.status_code == 200:
        st.success("DAG run successfully triggered!")
    else:
        st.error("Failed to trigger DAG run.")

def main():
    st.title("DAGs Dashboard")

    show_dags = st.button("Show DAGs")
    if show_dags:
        url = f"{airflow_base_url}/dags?limit=100&only_active=true&paused=false"
        dags = get_dags(url)
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
            url = f"{airflow_base_url}/dags/streamlit_app/dagRuns"
            trigger_dag(url)
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
            trigger_dag(url)
            st.download_button(
                label="Press to Download Y processed file",
                data=data,
                file_name="Processed_Y_file.xlsx"
            )
            st.success("DAG 'Processed_Y_file' triggered successfully!")

if __name__ == "__main__":
    main()
