import json
import sys
import types
import unittest
from unittest.mock import patch

import ai


class _ModelAccessError(Exception):
    status_code = 403


class MeetingAiTests(unittest.TestCase):
    def test_transcription_falls_back_to_whisper_when_configured_model_is_unavailable(self):
        attempted_models = []

        def transcribe(*, model, file):
            attempted_models.append(model)
            if model == "gpt-4o-mini-transcribe":
                raise _ModelAccessError("Project does not have access to model; code=model_not_found")
            return types.SimpleNamespace(text="Kelly will call the customer on Friday.")

        parsed = {
            "summary": "The team reviewed customer follow-up.",
            "decisions": [],
            "updates": [],
            "actions": [{"text": "Call the customer on Friday", "owner": "Kelly"}],
        }
        client = types.SimpleNamespace(
            audio=types.SimpleNamespace(transcriptions=types.SimpleNamespace(create=transcribe)),
            responses=types.SimpleNamespace(
                create=lambda **kwargs: types.SimpleNamespace(output_text=json.dumps(parsed))
            ),
        )
        fake_openai = types.SimpleNamespace(OpenAI=lambda api_key: client)

        with (
            patch.dict(sys.modules, {"openai": fake_openai}),
            patch.dict(
                "os.environ",
                {
                    "OPENAI_API_KEY": "test-key",
                    "OPENAI_TRANSCRIPTION_MODEL": "gpt-4o-mini-transcribe",
                },
                clear=False,
            ),
        ):
            result = ai.generate_meeting_actions_from_audio(b"audio", filename="meeting.webm")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(attempted_models, ["gpt-4o-mini-transcribe", "whisper-1"])
        self.assertEqual(result["actions"][0]["owner"], "Kelly")


if __name__ == "__main__":
    unittest.main()
