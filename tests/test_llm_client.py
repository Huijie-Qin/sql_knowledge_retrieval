import unittest
from unittest.mock import Mock, patch
from src.llm_client import LLMClient

def test_llm_client_initialization():
    """Test that LLMClient initializes correctly with settings"""
    with patch('src.llm_client.OpenAI') as mock_openai:
        client = LLMClient()
        assert client.model == "gpt-4o-mini"
        mock_openai.assert_called_once()

def test_llm_client_chat_with_only_prompt():
    """Test chat method with only user prompt"""
    with patch('src.llm_client.OpenAI') as mock_openai:
        # Setup mock response
        mock_choice = Mock()
        mock_choice.message.content.strip.return_value = "OK"
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_openai.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient()
        response = client.chat("你好，请返回'OK'")

        # Verify API was called with correct parameters
        mock_openai.return_value.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "你好，请返回'OK'"}],
            temperature=0.1,
            max_tokens=4096
        )
        assert response == "OK"

def test_llm_client_chat_with_system_prompt():
    """Test chat method with system prompt"""
    with patch('src.llm_client.OpenAI') as mock_openai:
        # Setup mock response
        mock_choice = Mock()
        mock_choice.message.content.strip.return_value = "Hello, how can I help?"
        mock_response = Mock()
        mock_response.choices = [mock_choice]
        mock_openai.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient()
        system_prompt = "You are a helpful assistant."
        user_prompt = "Hello"
        response = client.chat(user_prompt, system_prompt)

        # Verify API was called with correct messages
        mock_openai.return_value.chat.completions.create.assert_called_once_with(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            max_tokens=4096
        )
        assert response == "Hello, how can I help?"
