
from airflow.models import TaskInstance, Variable
from airflow.configuration import conf
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Any
from dotenv import load_dotenv


def send_secure_email(subject: str, html_content: str) -> None:
    """Envoie un email en utilisant SMTP sécurisé (Gmail)."""
    # Configuration SMTP
    smtp_info = Variable.get("SMTP_GMAIL_INFO")
    smtp_host = smtp_info["SMTP_HOST"]
    smtp_port = int(smtp_info["SMTP_PORT"])    # 587
    smtp_user = smtp_info["SMTP_PORT"]    # "your.email@gmail.com"  # Remplacez par votre email
    smtp_password = Variable.get("SMTP_PASSWORD") # "your_app_password"  # Remplacez par votre mot de passe d'application

    to: list = list(smtp_user)
    # Création du message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = ", ".join(to)
    
    # Ajout du contenu HTML
    msg.attach(MIMEText(html_content, 'html'))

    try:
        # Connexion au serveur SMTP avec TLS
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        
        # Envoi de l'email
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"Erreur lors de l'envoi de l'email: {str(e)}")
        raise

def _extract_callback_success(context: dict[str, Any]) -> None:
    """Fonction de callback en cas de succès d'une tâche."""
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
    
    send_secure_email(
        subject=f"Airflow Success: {task.task_id}",
        html_content=html_content
    )

def _extract_callback_failure(context: dict[str, Any]) -> None:
    """Fonction de callback en cas d'échec d'une tâche."""
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
    
    send_secure_email(
        to=['your.email@example.com', 'alert.team@example.com'],
        subject=f"⚠️ Airflow Failure: {task.task_id}",
        html_content=html_content
    )

def _extract_callback_retry(context: dict[str, Any]) -> None:
    """Fonction de callback en cas de nouvelle tentative d'une tâche."""
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
    
    send_secure_email(
        to=['your.email@example.com'],
        subject=f"🔄 Airflow Retry: {task.task_id}",
        html_content=html_content
    )
