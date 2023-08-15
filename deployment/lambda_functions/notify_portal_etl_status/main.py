
def notify_portal_etl_status(etl_args, context):
    cluster_id = etl_args['portalEtlClusterId']
    #step_id = etl_args['currentEtlVariantStepId']
    #etl_status = etl_args['etlStatus']

    # Temp will send via slack
    log_message = (
        f"Cluster ID: {cluster_id}\n"
    )
    print(log_message)
    return etl_args