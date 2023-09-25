import sys
from abc import ABC, abstractmethod

import boto3


class PortalEtlEmrStepService(ABC):
    def __init__(self, etl_args: dict):
        self.portal_etl_steps_to_execute = []
        self.etl_args = etl_args

    @abstractmethod
    def get_default_etl_steps_to_execute(self) -> list:
        pass

    @abstractmethod
    def get_next_steps(self, portal_etl_steps_to_execute: list, current_etl_steps: list, study_ids: list) -> list:
        pass

    @abstractmethod
    def get_etl_step_description(self, next_etl_steps: list) -> dict:
        pass

    @abstractmethod
    def get_etl_current_step_names(self, current_etl_steps) -> list:
        pass

    def submit_next_portal_etl_step(self) -> dict:
        portal_etl_steps_to_execute = self.get_etl_steps_to_execute()
        current_etl_steps = self.etl_args.get('currentEtlSteps')
        study_ids = self.etl_args['input'].get('studyIds')
        print(
            f'Attempting to get Next Step in ETL currentStep={current_etl_steps}, list of steps {portal_etl_steps_to_execute}')

        next_etl_steps = self.get_next_steps(portal_etl_steps_to_execute, current_etl_steps, study_ids)
        # Extract Data From Input
        portal_etl_cluster_id = self.etl_args['portalEtlClusterId']

        if next_etl_steps is None or len(next_etl_steps) < 1:
            print('Next Step Could not be defined.... Exiting ETL')
            sys.exit()

        submitted_step_ids = self.__submit_portal_etl_steps_to_emr(portal_etl_cluster_id, next_etl_steps)
        self.etl_args['currentEtlStepIds'] = submitted_step_ids
        self.etl_args['currentEtlSteps'] = self.get_etl_current_step_names(next_etl_steps)
        return self.etl_args

    def get_etl_steps_to_execute(self):
        # Grab Current ETL Step and List of Steps to execute
        user_input = self.etl_args['input']
        etl_portal_steps_to_execute = user_input.get('etlStepsToExecute')
        current_step = self.etl_args.get('currentEtlStep')

        if etl_portal_steps_to_execute is None:
            etl_portal_steps_to_execute = self.get_default_etl_steps_to_execute()
            self.etl_args['input']['etlStepsToExecute'] = etl_portal_steps_to_execute
        # Validate User's Custom ETL Portal Steps before submitting first step (currentEtlStep is None)
        elif current_step is None:
            validate_custom_etl_portal_steps_to_execute(etl_portal_steps_to_execute,
                                                        self.get_default_etl_steps_to_execute())
        return etl_portal_steps_to_execute

    def __submit_portal_etl_steps_to_emr(self, portal_etl_cluster_id: str, next_etl_steps: list) -> list:
        client = boto3.client('emr', region_name='us-east-1')
        response = client.add_job_flow_steps(
            JobFlowId=portal_etl_cluster_id,
            Steps=self.get_etl_step_description(next_etl_steps)
        )
        return response["StepIds"]


def validate_custom_etl_portal_steps_to_execute(custom_etl_portal_steps_to_execute: list, default_portal_steps: list):
    """
    Validates user's custom ETL portal steps to execute.

    Args:
        custom_etl_portal_steps_to_execute (list): List of ETL steps to execute.

    Returns:
        :param custom_etl_portal_steps_to_execute:
        :param default_portal_steps:
    """
    custom_etl_steps_valid = all(
        etl_step.lower() in default_portal_steps for etl_step in custom_etl_portal_steps_to_execute)
    if not custom_etl_steps_valid or len(custom_etl_portal_steps_to_execute) > len(default_portal_steps):
        print(f'Custom Portal ETL Steps not valid: steps input: ${custom_etl_portal_steps_to_execute}')
        sys.exit('Invalid Custom ETL Steps')
    return
