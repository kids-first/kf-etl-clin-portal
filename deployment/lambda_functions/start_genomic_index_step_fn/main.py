import os
import sys
import json
import boto3

GENOMIC_INDEX_STEP_FN_ARN = os.environ['GENOMIC_INDEX_STEP_FN_ARN']


def start_genomic_index_step_fn(etl_args: dict, context):
    copied_args = dict(etl_args)
    user_input = copied_args.get('input')

    # Remove etl steps to execute. These are not the same steps for the Genomic Index ETL
    user_input.pop('etlStepsToExecute')

    user_custom_chromosomes = user_input.get('chromosomes', [])
    # If user passes all to chromosomes enforce a sample
    if user_custom_chromosomes and user_custom_chromosomes[0] == 'all' and 'sample' not in user_input:
        print('Custom Parameter ERROR..... Sample parameter required when chromosomes set to all')
        sys.exit(-1)

    # Initialize the AWS Step Functions client
    client = boto3.client('stepfunctions')

    input_json_str = json.dumps(user_input)
    try:
        # Start the execution of the state machine with the input JSON
        response = client.start_execution(
            stateMachineArn=GENOMIC_INDEX_STEP_FN_ARN,
            input=input_json_str
        )

        # Print the execution ARN
        print(f"Execution ARN: {response['executionArn']}")

    except Exception as e:
        print(f"Error starting execution: {str(e)}")

    return etl_args
