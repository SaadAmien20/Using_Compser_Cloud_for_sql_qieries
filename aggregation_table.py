from datetime import timedelta, date
import datetime
from textwrap import dedent
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator

yesterday = (datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
PROJECT_ID = "DEMO"

DELETE_PARTITION_SQL = """
DELETE FROM `{0}` WHERE sec_received_at>="{1}"
""".format("DEMO.smapi_aggregation_pipeline_demo", yesterday)

AGGREGATE_SQL = """
SELECT  
x_vf_custom_authorization_id,
min(date(sec_received_at))as sec_received_at,
MAX(x_vf_trace_network_bearer) as network_bearer,
MAX(x_vf_trace_os_name) as os_name,
CASE WHEN MAX(gdpr_login)=1 THEN TRUE ELSE FALSE END AS gdpr_login,
CASE
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%404%" THEN "authentication expired"
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%Authentication Failed%" THEN "authentication failed"
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%Authentication failed%" THEN "authentication failed"
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%Authentication Successful%" THEN "authentication succeed"
    WHEN MAX(IF(desc_rank=2, event_description_simple, NULL)) LIKE "%Authentication Successful%" THEN "authentication succeed"
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%loaded%" THEN "user aborted"
    WHEN MAX(IF(desc_rank=1, event_description_simple, NULL)) LIKE "%focused%" THEN "user aborted"
    ELSE "ghost"
END as success,
MAX(IF(desc_rank=1, error_message, NULL)) as error_message,
MAX(IF(desc_rank=1, event_description_simple, NULL)) as last_activity,
MAX(IF(desc_rank=2, event_description_simple, NULL)) as n_1,
MAX(IF(desc_rank=3, event_description_simple, NULL)) as n_2,
MAX(IF(desc_rank=4, event_description_simple, NULL)) as n_3,
MAX(IF(desc_rank=5, event_description_simple, NULL)) as n_4,
MAX(IF(desc_rank=6, event_description_simple, NULL)) as n_5,
MAX(IF(desc_rank=7, event_description_simple, NULL)) as n_6,
MAX(IF(desc_rank=8, event_description_simple, NULL)) as n_7,
MAX(IF(asc_rank=1, event_description_simple, NULL)) as starting_activity,
MAX(x_vf_custom_client_id) AS x_vf_custom_client_id,
TIMESTAMP_MILLIS(MIN(first_timestamp)) as first_timestamp,
TIMESTAMP_MILLIS(MAX(latest_timestamp)) as latest_timestamp,
MAX(latest_timestamp) - MIN(latest_timestamp) as session_time,
DATE(TIMESTAMP_MILLIS(MIN(first_timestamp))) as date_ts,
COUNT(event_description_simple) as activity_count,

FROM
(
    SELECT *,
    row_number() OVER (PARTITION BY x_vf_trace_session_id, x_vf_custom_authorization_id ORDER BY x_vf_trace_timestamp ASC) as asc_rank,
    row_number() OVER (PARTITION BY x_vf_trace_session_id, x_vf_custom_authorization_id ORDER BY x_vf_trace_timestamp DESC) as desc_rank,
    CASE
        WHEN event_description_simple LIKE "%Transaction timeout%" THEN REPLACE(event_description_simple, "Authentication failed: ", "")
        WHEN event_description_simple LIKE "%Authentication: ERROR:%"  THEN REGEXP_EXTRACT(event_description_simple, "ERROR: (.*)")
        WHEN event_description_simple LIKE "%Authentication Failed%" THEN REGEXP_EXTRACT(event_description_simple, "Authentication Failed: (.*)")
        WHEN event_description_simple LIKE "%Authentication failed%" THEN REGEXP_EXTRACT(event_description_simple, "Authentication failed: (.*)")
    END AS error_message
    FROM
    (
        SELECT
        x_vf_custom_authorization_id,
        x_vf_trace_session_id,
        event_description_simple,
        x_vf_trace_timestamp,
        x_vf_event_type,
        CASE WHEN event_description_simple LIKE "GDPR%" THEN 1 ELSE 0 END as gdpr_login,
        CASE WHEN x_vf_custom_client_id!="NA" THEN x_vf_custom_client_id END as x_vf_custom_client_id,
        MAX(x_vf_trace_os_name) OVER (PARTITION BY x_vf_custom_authorization_id) as x_vf_trace_os_name,
        MAX(x_vf_trace_network_bearer) OVER (PARTITION BY x_vf_custom_authorization_id) as x_vf_trace_network_bearer,
        MIN(x_vf_trace_timestamp) OVER (PARTITION BY x_vf_trace_session_id) as first_timestamp,
        MAX(x_vf_trace_timestamp) OVER (PARTITION BY x_vf_trace_session_id) as latest_timestamp,
        sec_received_at
        FROM `demo.smapi_idgateway.events_web`
        WHERE 
        DATE(sec_received_at) >= '{0}' 
       
        
        AND x_vf_custom_authorization_id!="NA" AND x_vf_custom_authorization_id IS NOT NULL

        and (x_vf_custom_authorization_id not in (select distinct x_vf_custom_authorization_id 
        from demo.smapi_aggregation_pipeline_demo` 
        where DATE(sec_received_at) >='{0}' and  success='authentication succeed') and lower(event_description_simple) not like '%succee%')
)
    WHERE 
    x_vf_event_type LIKE "UICustom"
    AND event_description_simple !="Page unloaded"
    AND event_description_simple NOT LIKE "%http%"
    AND event_description_simple NOT LIKE "%request: /web%"
    AND event_description_simple NOT LIKE "%language%"
    AND event_description_simple NOT LIKE "%: response%"
    AND event_description_simple NOT LIKE "%: process%"
    AND event_description_simple NOT LIKE "OTP: backButtonURL%"
    AND event_description_simple NOT LIKE "%Market dropdown%"
    AND event_description_simple NOT LIKE "%cookie set%"
    AND event_description_simple NOT LIKE "% sec%"
)
GROUP BY x_vf_custom_authorization_id 
""".format(yesterday)

default_args = {
    'owner': 'Saad Hassan',
    'retries': 1,
    'depends_on_past': False,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': datetime.datetime(2022, 4, 1),
    'email_on_failure': True,
    'email': ['saad.amien.hassan@gmail.com'],
    'project_id': PROJECT_ID
}

dag = DAG(
    dag_id='Demo_smapi_aggregation_on_table_smapi_aggregation_pipeline_demo',
    default_args=default_args,
    schedule_interval="0 3 * * *")  

delete_partition = BigQueryOperator(
    task_id='delete_existing_partition',
    sql=DELETE_PARTITION_SQL,
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    dag=dag
)
#Basic task in the dag
aggregate_backend_table = BigQueryOperator(
    task_id='aggregate_smapi_table',
    sql=AGGREGATE_SQL,
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    destination_dataset_table='demo.smapi_aggregation_pipeline_demo',
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    dag=dag
)
# [START DOCUMENTATION]
delete_partition.doc_md = dedent(
    """
    this task to avoid dublications when dag run more than once at a day so we delete coming data which greather than or equal yesterday, 
    if there are not data comming todyay wil not remove anything.
  """
)
aggregate_backend_table.doc_md = dedent(
    """
    get sessions from `vf-grp-cpsa-prd-cpsoi-12.vf_smapi_idgateway.events_web` by autherization id 
    """

)
delete_partition >> aggregate_backend_table