from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, Any

class PromptManager:
    def __init__(self, template_dir: str = "prompts"):
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def get_prompt(self, template_name: str, **kwargs: Dict[str, Any]) -> str:
        template = self.env.get_template(f"{template_name}.j2")
        return template.render(**kwargs)
