import fastworkflow
from fastworkflow.train.generate_synthetic import generate_diverse_utterances
from pydantic import BaseModel, Field

from ..application.control_type import ControlType


class Signature:
    """Initialize the workflow with a root ControlType context."""

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
    """Create a ControlType instance and attach it as the root command context."""

    def __call__(
        self,
        workflow: fastworkflow.Workflow,
        command: str,
        command_parameters: Signature.Input,
    ) -> fastworkflow.CommandOutput:
        # Initialize the root context
        workflow.root_command_context = ControlType(command_parameters.control_type)

        response = (
            f'Context: {workflow.current_command_context_displayname}\n'
            f'Command: {command}\n'
            f'Command parameters: {command_parameters}\n'
            f"Root context set to ControlType('{command_parameters.control_type}')."
            f"Now you can call commands exposed in this context."
        )

        return fastworkflow.CommandOutput(
            workflow_id=workflow.id,
            command_responses=[
                fastworkflow.CommandResponse(response=response)
            ]
        )