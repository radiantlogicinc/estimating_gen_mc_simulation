import fastworkflow
from fastworkflow.train.generate_synthetic import generate_diverse_utterances
from pydantic import BaseModel, Field

from ...application.control_type import ControlType

# the signature class defines our intent
class Signature:
    class Input(BaseModel):
        time_span: str = Field(
            description="The time span to focus on, 'day', 'month', 'all_time' only possible answers",
            examples=['1', '2', '3'],
            # pattern=r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        )

    plain_utterances = [
        "I want to know how many defects were remediated in the last day",
        "What is the average amount of defects that were remediated per day over the last month?"
        "How many defects are remediated daily looking at data for all time?"
    ]

    @staticmethod
    def generate_utterances(workflow: fastworkflow.Workflow, command_name: str) -> list[str]:
        """This function will be called by the framework to generate utterances for training"""
        return [
            command_name.split('/')[-1].lower().replace('_', ' ')
        ] + generate_diverse_utterances(Signature.plain_utterances, command_name)

# the response generator class processes the command
class ResponseGenerator:
    def _process_command(self, workflow: fastworkflow.Workflow, input: Signature.Input) -> None:
        """Helper function that actually executes the remediated_per_day function.
           It is not required by fastworkflow. You can do everything in __call__().
        """
        # Call the application function
        control_type: ControlType = workflow.command_context_for_response_generation
        control_type.remediated_per_day(time_span=input.time_span)

    def __call__(self, workflow: 
                 fastworkflow.Workflow, 
                 command: str, 
                 command_parameters: Signature.Input) -> fastworkflow.CommandOutput:
        """The framework will call this function to process the command"""
        self._process_command(workflow, command_parameters)
        
        response = (
            f'Context: {workflow.current_command_context_displayname}\n'
            f'Command: {command}\n'
            f'Command parameters: {command_parameters}\n'
            f'Response: The message was printed to the screen'
        )

        return fastworkflow.CommandOutput(
            workflow_id=workflow.id,
            command_responses=[
                fastworkflow.CommandResponse(response=response)
            ]
        )