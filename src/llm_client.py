from openai import OpenAI
from config.settings import settings
from typing import Optional

class LLMClient:
    def __init__(self):
        self.client = OpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url
        )
        self.model = settings.llm_model

    def chat(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # 使用流式响应，超时时间设置为20分钟（1200秒），适配服务端流式接口超时
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.1,
            max_tokens=settings.llm_max_tokens,
            stream=True,
            timeout=1200
        )

        # 拼接流式响应内容
        full_content = ""
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content is not None:
                full_content += chunk.choices[0].delta.content

        return full_content.strip()
