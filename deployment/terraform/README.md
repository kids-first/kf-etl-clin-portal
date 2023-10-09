# Portal ETL Terraform INF Code AWS StepFns

## Table of Contents
1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Folder Structure](#folder-structure)

## Introduction
Welcome to Portal ETL INF Terraform ReadMe. This IAC is responsible for creating the necessary INF to run the Portal ETL
via AWS StepFn


## Getting Started
To get started with this Terraform codebase, follow these steps:

1. Clone this repository to your local machine:
   ```
   git clone <repository-url>
   cd <repository-directory>/deployments/terraform
   ```

2. Initialize Terraform in the project directory (KF-Strides QA):
   ```
   terraform init -backend=true -backend-config="kf-strides/us-east-1/qa/backend.conf"
   ```

3. Provision the infrastructure by running (KF-Strides QA):
   ```
   terraform apply -var-file=kf-strides/us-east-1/qa/variables.tfvars   
   ```

5. Review the planned changes, confirm by typing "yes" when prompted, and Terraform will create the resources as specified in your configuration.

## Folder Structure
Describe the structure of your Terraform project, including the purpose of each folder or directory.

```
/
├── config.tf           # Grab Conf files from /bin/conf
├── secret.tf           # AWS SecretManger
├── variables.tf        # Variables used throughout the codebase
├── lambda.tf           # AWS Lambda functions and IAM roles
├── sfn.tf              # AWS Step Functions and IAM resources
├── kf-strides/         # Account Specific variables for Kf-strides
├── include/            # Account Specific variables for Include
├── README.md           # This README file
└── ...
```
