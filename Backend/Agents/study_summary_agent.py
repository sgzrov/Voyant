import logging
import openai
from typing import Optional, Any, Generator

from Backend.Database.study_repository import update_study_summary_by_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = True

class StudySummaryAgent:
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

    # Persist study summary to the database
    def _append_study_summary(self, study_id: str, user_id: str, summary: str, session) -> None:
        if not study_id or not summary.strip():
            return
        if session is None:
            raise ValueError("A database session must be provided.")
        update_study_summary_by_id(session, study_id, summary.strip(), user_id)

    def generate_study_summary(self, text: str, prompt: Optional[str] = None) -> Generator[Any, None, None]:
        instructions = prompt if prompt is not None else self.prompt

        try:
            response = self.client.responses.create(
                model = self.model,
                input = f"{instructions}\n\nText to summarize:\n{text}",
                stream = True
            )
            for chunk in response:
                yield chunk
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in generate_study_summary: {e}")
            raise