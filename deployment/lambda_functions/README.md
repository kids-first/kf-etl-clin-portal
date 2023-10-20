# Lambda Functions Overview

### Add Portal ETL EMR Step

**Description**: [Based on whether the current running ETL is Clinical, Genomic, or Genomic Index
this Function determines the next step to submit to the EMR Cluster]

**Runtime**: [Lambda Runtime, e.g., Python 3.9]

**Handler**: [Handler function within the Lambda code, e.g., `main.add_portal_etl_emr_step`]

### Check Portal ETL Status

**Description**: [ Monitors the current status of the EMR Cluster and determines if the ETL failed or completed ]

**Runtime**: [Lambda Runtime, e.g., Python 3.9]

**Handler**: [Handler function within the Lambda code, e.g., `main.check_portal_etl_emr_step_status`]

### Initialize Portal ETL EMR

**Description**: [ Create EMR Cluster ]

**Runtime**: [Lambda Runtime, e.g., Python 3.9]

**Handler**: [Handler function within the Lambda code, e.g., `main.initialize_portal_etl_emr`]

### Notify Portal ETL Status

**Description**: [ Sends Slack Message of the Status of the Portal ETL (whether it passed or failed)]

**Runtime**: [Lambda Runtime, e.g., Python 3.9]

**Handler**: [Handler function within the Lambda code, e.g., `main.notify_portal_etl_status`]


### Start Genomic Index Step Fn

**Description**: [ Attempts to start the Genomic Index StepFn (Genomic Index ETL) runs once the Genomic ETL has completed]

**Runtime**: [Lambda Runtime, e.g., Python 3.9]

**Handler**: [Handler function within the Lambda code, e.g., `main.start_genomic_index_step_fn`]
