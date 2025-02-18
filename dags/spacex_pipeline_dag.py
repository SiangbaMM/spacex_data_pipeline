"""
SpaceX Data Pipeline DAG

This DAG orchestrates the SpaceX data pipeline:
1. Extract data from SpaceX API using Singer tap
2. Load data into Snowflake
3. Transform data using dbt
"""
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from airflow.models import TaskInstance

from datetime import datetime, timedelta
from typing import Any, List, Optional
from dotenv import load_dotenv
import os


def _get_email_address() -> Optional[str]:
    load_dotenv()
    return os.getenv("SPACEX_EMAIL_ADDRESS")



def _spacex_data_callback_success(context: dict[str, Any]) -> None:
    """
    Fonction de callback en cas de succès d'une tâche.
    
    Args:
        context: Le contexte fourni par Airflow contenant les informations sur l'exécution
    """
    task: TaskInstance = context['task_instance']
    dag = context['dag']
    execution_date = context['execution_date']
    
    html_content = f"""
        <h3>La tâche s'est terminée avec succès</h3>
        <p>Détails :</p>
        <ul>
            <li>Tâche : {task.task_id}</li>
            <li>DAG : {dag.dag_id}</li>
            <li>Date d'exécution : {execution_date}</li>
            <li>Fin d'exécution : {datetime.now()}</li>
            <li>Run ID : {context.get('run_id')}</li>
        </ul>
        <p>Environnement Astro : {context.get('env', 'Non spécifié')}</p>
    """
    
    send_email(
        to=_get_email_address(),
        subject=f"Airflow Success: {task.task_id}",
        html_content=html_content,
    )

def _spacex_data_callback_failure(context: dict[str, Any]) -> None:
    """
    Fonction de callback en cas d'échec d'une tâche.
    
    Args:
        context: Le contexte fourni par Airflow contenant les informations sur l'exécution
    """
    task: TaskInstance = context['task_instance']
    dag = context['dag']
    execution_date = context['execution_date']
    exception = context.get('exception', 'Pas d\'exception capturée')
    
    html_content = f"""
        <h3>La tâche a échoué</h3>
        <p>Détails :</p>
        <ul>
            <li>Tâche : {task.task_id}</li>
            <li>DAG : {dag.dag_id}</li>
            <li>Date d'exécution : {execution_date}</li>
            <li>Fin d'exécution : {datetime.now()}</li>
            <li>Run ID : {context.get('run_id')}</li>
        </ul>
        <p><strong>Erreur :</strong></p>
        <pre>{str(exception)}</pre>
        <p>Veuillez vérifier les logs dans la console Astro pour plus de détails.</p>
        <p>Lien vers les logs : {context.get('task_instance').log_url}</p>
        <p>Environnement Astro : {context.get('env', 'Non spécifié')}</p>
    """
    
    send_email(
        to=_get_email_address(),
        subject=f"⚠️ Airflow Failure: {task.task_id}",
        html_content=html_content,
    )

def _spacex_data_callback_retry(context: dict[str, Any]) -> None:
    """
    Fonction de callback en cas de nouvelle tentative d'une tâche.
    
    Args:
        context: Le contexte fourni par Airflow contenant les informations sur l'exécution
    """
    task: TaskInstance = context['task_instance']
    dag = context['dag']
    execution_date = context['execution_date']
    try_number = context['task_instance'].try_number
    max_tries = task.max_tries if hasattr(task, 'max_tries') else 1
    
    html_content = f"""
        <h3>Nouvelle tentative d'exécution de la tâche</h3>
        <p>Détails :</p>
        <ul>
            <li>Tâche : {task.task_id}</li>
            <li>DAG : {dag.dag_id}</li>
            <li>Date d'exécution : {execution_date}</li>
            <li>Tentative : {try_number} sur {max_tries}</li>
            <li>Heure de la tentative : {datetime.now()}</li>
            <li>Run ID : {context.get('run_id')}</li>
        </ul>
        <p>La tâche va être relancée automatiquement.</p>
        <p>Lien vers les logs : {context.get('task_instance').log_url}</p>
        <p>Environnement Astro : {context.get('env', 'Non spécifié')}</p>
    """
    
    send_email(
        to=_get_email_address(),
        subject=f"🔄 Airflow Retry: {task.task_id}",
        html_content=html_content,
    )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "on_success_callback": _spacex_data_callback_success,
    "on_failure_callback": _spacex_data_callback_failure,
    "on_retry_callback": _spacex_data_callback_retry,
}


@dag(
    dag_id="spacex_pipeline",
    default_args=default_args,
    description="Extract SpaceX data, load to Snowflake, and transform with dbt",
    schedule_interval="0 0 * * *",  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spacex", "dbt", "singer"],
)
def spacex_pipeline() -> None:
    """Space data pipeline main dag"""
    # Start task
    begin = EmptyOperator(task_id="begin")

    # Extract and load data using Singer tap

    extract_load = BashOperator(
        task_id="tap_extract_load",
        bash_command="cd /usr/local/airflow && source ./env_singer/bin/activate && python3 singer_tap/runner/tap_spacex_runner.py",
    )

    transform = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /usr/local/airflow && source ./env_dbt/bin/activate && dbt run -s +marts --full-refresh --project-dir spacex_project/ --profiles-dir spacex_project/",
    )

    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /usr/local/airflow && source ./env_dbt/bin/activate && dbt test --project-dir spacex_project/ --profiles-dir spacex_project/",
    )

    # End task
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    begin >> extract_load >> transform >> test >> end


# Create DAG instance
spacex_pipeline()
