import asyncio
import logging
from typing import Dict

from dotenv import load_dotenv
from google.adk import Runner
from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.sessions import InMemorySessionService
from google.adk.tools import ToolContext
from google.genai import types

from pydantic import BaseModel, Field


import re

# API Key
load_dotenv()

# --- Configure Logging to File ---
LOG_FILE = "output.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# ---------------------------------

APP_NAME = "report_writing_app_v3"
USER_ID = "dev_user_01"
SESSION_ID = "123344"
GEMINI_MODEL = "gemini-2.0-flash"


# --- Input Schema ---
class PlanAgentInput(BaseModel):
    initial_content: str = Field(description="Initial content")

# --- Tool Definition ---
def exit_loop(tool_context: ToolContext):
  """Call this function ONLY when the critique indicates no further changes are needed, signaling the iterative process should end."""
  logger.info(f"[Tool Call] exit_loop triggered by {tool_context.agent_name}")
  tool_context.actions.escalate = True
  # Return empty dict as tools should typically return JSON-serializable output
  return {}

def construct_report_agent(chapters : Dict):
    sub_agents = []

    for idx, chapter in enumerate(chapters.items()):

        write_report_agent = LlmAgent(
            name="WriteReportAgent",
            model=GEMINI_MODEL,
            include_contents='none',
            instruction=f"""You are a Creative Writing Assistant tasked with writing a detailed report for each section from the given outline.
        
            **Outline to write a Detailed Report:**
             ```
             Chapter: {chapter[0]}
             Content: {chapter[1]}
             ```
            Include details as much as possible. Expect *4-5 paragraphs*.
            Include code snippet when you refer any code in your report.
            Do not add explanations. Output only the full final report.
            """,
            description="Writes a detailed report from an outline",
            output_key=f'STATE_REPORT_{idx + 1}',
        )

        sub_agents.append(write_report_agent)

    return sub_agents

# read content from a sql file
def read_sql_file(file_path):
    """
    Reads the content of a .sql file.

    Args:
        file_path (str): The path to the .sql file.

    Returns:
        str: The content of the .sql file as a single string,
             or None if the file cannot be read.
    """
    try:
        with open(file_path, 'r') as file:
            sql_content = file.read()
        return sql_content
    except FileNotFoundError:
        logger.error(f"Error: File not found at {file_path}")
        return None
    except Exception as e:
        logger.error(f"An error occurred while reading the file: {e}")
        return None

# --- Function to Interact with the Agent ---
async def call_agent(initial_content: str):
    """
    Sends an initial content to the agent and runs the workflow.
    """

    # --- Setup Runner and Session ---
    session_service = InMemorySessionService()

    session_service.create_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        session_id=SESSION_ID,
    )

    current_session = session_service.get_session(app_name=APP_NAME,
                                                  user_id=USER_ID,
                                                  session_id=SESSION_ID)

    if not current_session:
        logger.error("Session not found!")
        return

    # Plan agent
    plan_agent = LlmAgent(
        name="PlanningAgent",
        model=GEMINI_MODEL,
        include_contents='none',
        instruction=f"""You are a Creative Writing Assistant tasked with planning a report outline from document.
        Draft an outline from the given content : {initial_content}.
        An outline will start with the overview followed by each chapter of the report (chapter1, chapter2,... etc.)
        and each chapter has sub chapters if needed. Also, include drafting content in each section (2 - 4 sentences).
        Response only with JSON object.
        **For example:**
         ```
           {{
                "Overview": "*content*",
                "Chapter1": {{
                    "content": "*content*"
                }},
                "Chapter1.1": {{
                    "parent": "Chapter1",
                    "content": "*content*"
                }},
                "Chapter1.2": {{
                    "parent": "Chapter1",
                    "content": "*content*"
                }},
                "Chapter2": {{
                    "content": "*content*"
                }},
                "Chapter2.1": {{
                    "parent": "Chapter2",
                    "content": "*content*"
                }},
                "Chapter2.2": {{
                    "parent": "Chapter2",
                    "content": "*content*"
                }}
            }}
         ```
        Do not add explanations. Output only the outline in JSON format.
        """,
        description="Writes an outline of a report from the given initial content",
        input_schema=PlanAgentInput
    )

    plan_agent_runner = Runner(
        agent=plan_agent,
        app_name=APP_NAME,
        session_service=session_service
    )

    import json
    query = json.dumps(PlanAgentInput(initial_content=initial_content).model_dump())

    content = types.Content(role='user', parts=[types.Part(text=query)])

    events = plan_agent_runner.run_async(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    plan_agent_response = "No response captured."
    async for event in events:
        if event.is_final_response() and event.content and event.content.parts:
            logger.info(f"Potential final response from [{event.author}]: {event.content.parts[0].text}")
            plan_agent_response = event.content.parts[0].text

    logger.info("\n--- Plan Agent Interaction Result ---")
    logger.info(f"Plan Agent Final Response: {plan_agent_response}")

    # Write report agent

    if plan_agent_response != "No response captured.":

        pattern = r"```json\s*(.*?)```"
        matches = re.findall(pattern, plan_agent_response, re.DOTALL)

        json_blocks = [match.strip() for match in matches]

        result = "\n".join(json_blocks)

        chapters = json.loads(result)

        sub_agents = construct_report_agent(chapters)

        # Overall sequential pipeline
        report_agent = SequentialAgent(
            name="SequentialWritingReportPipeline",
            sub_agents=sub_agents,
            description="Writes a detailed report from an outline"
        )

        report_agent_runner = Runner(
            agent=report_agent,  # Pass the custom orchestrator agent
            app_name=APP_NAME,
            session_service=session_service
        )

        content = types.Content(role='user', parts=[types.Part(text="")])

        events = report_agent_runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

        report_agent_response = "No response captured."
        for event in events:
            if event.is_final_response() and event.content and event.content.parts:
                logger.info(f"Potential final response from [{event.author}]: {event.content.parts[0].text}")
                report_agent_response = event.content.parts[0].text

        logger.info("\n--- Report Agent Interaction Result ---")
        logger.info(f"Report Agent Final Response: {report_agent_response}")

        logger.info("Final Session State:")

        final_session = session_service.get_session(app_name=APP_NAME,
                                                      user_id=USER_ID,
                                                      session_id=SESSION_ID)

        logger.info(json.dumps(final_session.state, indent=2))
        logger.info("-------------------------------\n")
    else:
        logger.info("No response from Plan Agent")

# --- Run the Agent ---
async def main():
    logger.info("--- Running report agent ---")
    sql_content = read_sql_file("gitlab_crm_touchpoint.sql")
    if sql_content:
        await call_agent(sql_content)
    else:
        logger.warning("Could not read SQL content, agent not run.")


if __name__ == "__main__":
    asyncio.run(main())