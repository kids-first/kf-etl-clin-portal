
def notify_variant_etl_status(etl_args, context):
    cluster_id = etl_args['variantEtlClusterId']
    step_id = etl_args['currentEtlVariantStepId']
    etl_status = etl_args['etlStatus']
    current_step_status = etl_args['currentEtlStepStatus']
    etl_variant_steps_to_execute = etl_args['etlVariantStepsToExecute']
    etl_variant_current_step = etl_args['currentEtlVariantStep']

    # Temp will send via slack
    log_message = (
        f"Cluster ID: {cluster_id}\n"
        f"Step ID: {step_id}\n"
        f"Etl Status: {etl_status}\n"
        f"Current Step Status: {current_step_status}\n"
        f"Etl Variant Steps to Execute: {etl_variant_steps_to_execute}\n"
        f"Current Etl Variant Step: {etl_variant_current_step}"
    )
    print(log_message)