import time
import os
import pendulum
from databricks.sdk import WorkspaceClient
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from dotenv import load_dotenv
load_dotenv()

# schedule: 0 11 * * * every day at 11:00 AM
# @dag(
#     dag_id="orchestrate",
#     schedule="0 11 * * *",
#     catchup=False,
#     start_date=pendulum.datetime(year=2026, month=6, day=18, tz="Australia/Melbourne")
# )

def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )

        
        job_trigger = ws.jobs.run_now(job_id=950017999885243)
        print(f"✅ Job triggered! Run ID: {job_trigger.run_id}")

        while True:
            job_run = ws.jobs.get_run(job_trigger.run_id)
            
            print(f"Job run status: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")
            
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
            
            time.sleep(5)
        
        return "CDC data ingested"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_proj/target && rm -rf /opt/airflow/walmart_proj/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_proj && dbt source freshness"
    
    silver_technical = BashOperator(
        task_id='silver_technical',  
        cwd='/opt/airflow/walmart_proj',  
        bash_command='dbt run --select silver_t' 
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',  
        cwd='/opt/airflow/walmart_proj',  
        bash_command='dbt test --select silver_t'  
    )

    silver_business = BashOperator(
        task_id='silver_business',  
        cwd='/opt/airflow/walmart_proj',  
        bash_command='dbt run --select silver_b'  
    )

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',  
        cwd='/opt/airflow/walmart_proj',  
        bash_command='dbt test --select silver_b'  
    )
    
    gold = BashOperator(
        task_id='gold',  
        cwd='/opt/airflow/walmart_proj',
        bash_command='dbt run --select gold'  
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_proj',
        bash_command='dbt snapshot'  
    )
     
    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_proj',
        bash_command='dbt run --select gold/fact'  
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical \
        >> silver_technical_tests >> silver_business >> silver_business_tests \
        >> gold >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()