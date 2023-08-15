import json
import boto3
import os
import requests

SECRET_NAME = os.environ['SECRET_NAME']

def get_slack_webhook(secret_name: str):
    print(f"Getting secret {secret_name}")

    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(response['SecretString'])

    return secrets['slack_webhook']

def format_message(etl_args : dict):
    cluster_id = etl_args['portalEtlClusterId']
    step_id = etl_args['currentEtlPortalStepId']
    etl_status = etl_args['etlStatus']
    current_step_status = etl_args['currentEtlStepStatus']
    etl_portal_steps_to_execute = etl_args['etlPortalStepsToExecute']
    etl_portal_current_step = etl_args['currentEtlPortalStep']

    log_message = (
        f"Cluster ID: {cluster_id}\n"
        f"Step ID: {step_id}\n"
        f"Etl Status: {etl_status}\n"
        f"Current Step Status: {current_step_status}\n"
        f"Etl Portal Steps to Execute: {etl_portal_steps_to_execute}\n"
        f"Current Etl Portal Step: {etl_portal_current_step}"
    )
    print(log_message)

    return log_message

def send_slack_message(webhook_url: str, message: str):
    payload = {'text' : message}
    requests.post(webhook_url, json=payload)

def notify_portal_etl_status(etl_args: dict, context):
    print
    webhook_url = get_slack_webhook(SECRET_NAME)
    message = format_message(etl_args)
    send_slack_message(webhook_url, message)