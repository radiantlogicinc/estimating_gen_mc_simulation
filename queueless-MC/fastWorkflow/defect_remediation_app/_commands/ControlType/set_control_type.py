import fastworkflow
from fastworkflow.train.generate_synthetic import generate_diverse_utterances
from pydantic import BaseModel, Field

from ...application.control_type import ControlType

class Signature:
    """Update the workflow with a new ControlType."""

    class Input(BaseModel):
        control_type: str = Field(
            description="Name of a defect control type",
            examples=['ACC03', 'ACC17', 'ACC28', 'AUTH18', 'AUTH42'],
            default='ACC03'
        )

    plain_utterances = [
        "I'd like to begin a chat session about ACC03 defects",
        "I want to ask questions about my AUTH42 data",
        "I want to learn about AUTH18 defects",
        "Tell me about ACC28 defects"
        "I'm interested in ACC17"
    ]

    @staticmethod
    def generate_utterances(workflow: fastworkflow.Workflow, command_name: str) -> list[str]:
        """Generate training utterances for LLM-based intent matching."""
        return [
            command_name.split('/')[-1].lower().replace('_', ' ')
        ] + generate_diverse_utterances(Signature.plain_utterances, command_name)
    
class ResponseGenerator:
    def _process_command(self, workflow: fastworkflow.Workflow, input: Signature.Input) -> None:
        """Helper function that actually executes the set_control_type function.
        It is not required by fastworkflow. You can do everything in __call__().
        """
        # Call the application function
        control_type: ControlType = workflow.command_context_for_response_generation
        control_type.set_control_type(control_type=input.control_type)

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
            f"Root context set to ControlType('{command_parameters.control_type}').\n"
            f'Now you can call commands exposed in this context.\n'
        )

        return fastworkflow.CommandOutput(
            workflow_id=workflow.id,
            command_responses=[
                fastworkflow.CommandResponse(response=response)
            ]
        )