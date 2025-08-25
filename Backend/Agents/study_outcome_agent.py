import logging
import openai
from typing import BinaryIO, Optional, Any, Generator

from Backend.Database.study_repository import update_study_outcome_by_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True

class StudyOutcomeAgent:
    def __init__(self, api_key: str, prompt_path: str, model: str = "gpt-5") -> None:
        self.api_key = api_key
        self.model = model
        self.client = openai.OpenAI(api_key = api_key)

        try:
            with open(prompt_path, "r", encoding = "utf-8") as f:
                self.prompt = f.read()
        except Exception as e:
            logger.error(f"Error reading prompt file: {e}")
            raise

    # Persist study outcome to the database
    def _append_study_outcome(self, study_id: str, user_id: str, outcome: str, session) -> None:
        if not study_id or not outcome.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        update_study_outcome_by_id(session, study_id, outcome.strip(), user_id)

    def generate_study_outcome(self, file_obj: BinaryIO, user_input: str, prompt: Optional[str] = None, filename: str = "user_health_data.csv") -> Generator[Any, None, None]:
        instructions = prompt if prompt is not None else self.prompt

        try:
            file_obj.seek(0)
            file = self.client.files.create(
                file = (filename, file_obj, "text/csv"),
                purpose = "assistants"
            )
            response = self.client.responses.create(
                model = self.model,
                tools = [
                    {
                        "type": "code_interpreter",
                        "container": {
                            "type": "auto",
                            "file_ids": [file.id]
                        }
                    }
                ],
                instructions = instructions,
                input = user_input,
                stream = True
            )
            for chunk in response:
                yield chunk
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error generate_study_outcome: {e}")
            raise