import os

from dotenv import load_dotenv
from groq import Groq


class Agent:
    def __init__(self, system=""):
        self.system = system
        self.messages = []
        if self.system:
            self.messages.append({"role": "system", "content": system})

    def __call__(self, message):
        self.messages.append({"role": "user", "content": message})
        result = self.execute()
        self.messages.append({"role": "assistant", "content": result})
        return result

    def execute(self):
        load_dotenv()
        client = Groq(api_key=os.getenv("GROQ_API_KEY"))

        completion = client.chat.completions.create(
            messages=self.messages,
            model="llama3-70b-8192",
            temperature=0.5
        )

        return completion.choices[0].message.content
