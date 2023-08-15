import os
import boto3
import json
import urllib3

SECRET_NAME = os.environ['SECRET_NAME']

def get_slack_webhook(secret_name: str):
    print(f"Getting secret {secret_name}")

    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(response['SecretString'])

    return secrets['slack_webhook']

def format_message(etl_args : dict):
    cluster_id = etl_args['portalEtlClusterId']
    step_id = etl_args['currentEtlStepId']
    etl_status = etl_args['etlStatus']
    current_step_status = etl_args['currentEtlStepStatus']
    etl_portal_steps_to_execute = etl_args['etlStepsToExecute']
    etl_portal_current_step = etl_args['currentEtlStep']

    log_message = (
        f"Cluster ID: {cluster_id}\n"
        f"Step ID: {step_id}\n"
        f"Etl Status: {etl_status}\n"
        f"Current Step Status: {current_step_status}\n"
        f"Etl Portal Steps to Execute: {etl_portal_steps_to_execute}\n"
        f"Current Etl Portal Step: {etl_portal_current_step}"
    )

    return log_message

def send_slack_message(webhook_url: str, message: str):
    payload = json.dumps({'text' : message})
    headers = {'Content-Type' : 'application/json'}
    http = urllib3.PoolManager()

    http.request('POST', webhook_url, headers=headers, body=payload)

def notify_portal_etl_status(etl_args: dict, context):
    webhook_url = get_slack_webhook(SECRET_NAME)
    message = format_message(etl_args)
    send_slack_message(webhook_url, message)
    return etl_args
