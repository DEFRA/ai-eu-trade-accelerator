import json
from typing import Any

from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam

from .settings import LLMSettings, settings


class BeatriceLLMClient:
    def __init__(self, llm_settings: LLMSettings | None = None) -> None:
        self.settings = llm_settings or settings
        self.client = OpenAI(
            base_url=self.settings.base_url,
            api_key=self.settings.api_key,
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        response = self.client.embeddings.create(
            model=self.settings.embed_model,
            input=texts,
        )
        return [item.embedding for item in sorted(response.data, key=lambda x: x.index)]

    def list_models(self) -> list[str]:
        models = self.client.models.list()
        return sorted(model.id for model in models.data)

    def complete_text(
        self,
        prompt: str,
        model: str,
        system_prompt: str | None = None,
        temperature: float = 0.1,
    ) -> str:
        messages: list[ChatCompletionMessageParam] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self.client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )
        content = response.choices[0].message.content
        return content or ""

    def complete_json(
        self,
        prompt: str,
        model: str,
        system_prompt: str | None = None,
        temperature: float = 0.0,
    ) -> Any:
        raw = self.complete_text(
            prompt=prompt,
            model=model,
            system_prompt=system_prompt,
            temperature=temperature,
        )
        return self._parse_json(raw)

    def extract_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.local_extract_model,
            system_prompt="You extract structured legal propositions.",
            temperature=0.0,
        )

    def classify_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.local_classify_model,
            system_prompt="You classify legal divergence carefully and tersely.",
            temperature=0.0,
        )

    def guidance_extract_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.guidance_extract_model,
            system_prompt="You extract structured legal propositions.",
            temperature=0.0,
        )

    def guidance_classify_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.guidance_classify_model,
            system_prompt="You explain legal divergence in precise, audit-friendly language.",
            temperature=0.0,
        )

    def guidance_summarise_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.guidance_summarise_model,
            system_prompt="You summarise legal compliance analysis concisely and clearly.",
            temperature=0.0,
        )

    def reason_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.frontier_reason_model,
            system_prompt="You explain legal divergence in precise, audit-friendly language.",
            temperature=0.1,
        )

    def writeup_text(self, prompt: str) -> str:
        return self.complete_text(
            prompt=prompt,
            model=self.settings.frontier_writeup_model,
            system_prompt="You draft concise narrative summaries for legal comparison outputs.",
            temperature=0.2,
        )

    @staticmethod
    def _parse_json(raw: str) -> Any:
        text = raw.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3:
                text = "\n".join(lines[1:-1]).strip()
        return json.loads(text)


def main() -> None:
    client = BeatriceLLMClient()
    print(
        f"Beatrice LLM client ready. Base URL={client.settings.base_url} "
        f"local_extract_model={client.settings.local_extract_model}"
    )


if __name__ == "__main__":
    main()
