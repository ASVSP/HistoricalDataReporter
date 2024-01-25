from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

start_date = datetime(2024, 1, 7, 12, 30, 00)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 1,
    'retries_delay': 180
}

dag = DAG(
    'cropland-fires-processor',
    default_args=default_args,
    schedule_interval=timedelta(days=30),
    catchup=False,
    dagrun_timeout=timedelta(hours=2)
)

name_node_ops = SSHOperator(
    task_id='name_node_task',
    ssh_conn_id='name_node_conn',
    command='cd .. && chmod +x ./asvsp/data/load-data.sh && ./asvsp/data/load-data.sh ',
    dag=dag,
    cmd_timeout=300,
    ssh_hook=SSHHook(
        remote_host='namenode',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

hive_server_ops = SSHOperator(
    task_id='hive_server_task',
    ssh_conn_id='hive_server_conn',
    command='cd .. && /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -f '
            './opt/hive/scripts/asvsp/HiveDWScript.sql ',
    dag=dag,
    cmd_timeout=300,
    ssh_hook=SSHHook(
        remote_host='hive-server',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

pre_process_ops = SSHOperator(
    task_id='pre_process_task',
    ssh_conn_id='spark_master_conn',
    command='cd ../asvsp/scripts/ && ./../../spark/bin/spark-submit ./pre-process.py ',
    dag=dag,
    cmd_timeout=300,
    ssh_hook=SSHHook(
        remote_host='spark-master',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

location_dims_ops = SSHOperator(
    task_id='location_dims_task',
    ssh_conn_id='spark_master_conn',
    command='cd ../asvsp/scripts/ && ./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 '
            './location-dims.py ',
    dag=dag,
    cmd_timeout=600,
    ssh_hook=SSHHook(
        remote_host='spark-master',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

time_dims_ops = SSHOperator(
    task_id='time_dims_task',
    ssh_conn_id='spark_master_conn',
    command='cd ../asvsp/scripts/ && ./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 '
            './time-dims.py ',
    dag=dag,
    cmd_timeout=600,
    ssh_hook=SSHHook(
        remote_host='spark-master',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

gas_dim_ops = SSHOperator(
    task_id='gas_dim_task',
    ssh_conn_id='spark_master_conn',
    command='cd ../asvsp/scripts/ && ./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 '
            './gas-dim.py ',
    dag=dag,
    cmd_timeout=600,
    ssh_hook=SSHHook(
        remote_host='spark-master',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)

gas_emission_fact_ops = SSHOperator(
    task_id='gas_emission_fact_task',
    ssh_conn_id='spark_master_conn',
    command='cd ../asvsp/scripts/ && ./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 '
            './gas-emission-fact.py ',
    dag=dag,
    cmd_timeout=900,
    ssh_hook=SSHHook(
        remote_host='spark-master',
        username='root',
        password='asvsp',
        banner_timeout=10
    )
)
name_node_ops >> hive_server_ops >> pre_process_ops >> location_dims_ops >> time_dims_ops >> gas_dim_ops >> gas_emission_fact_ops
