import asyncio
import logging
from dotenv import load_dotenv
from google.adk import Runner
from google.adk.agents import LlmAgent, LoopAgent, SequentialAgent
from google.adk.sessions import InMemorySessionService
from google.adk.tools import ToolContext
from google.genai import types

# API Key
load_dotenv()

# --- Configure Logging to File ---
LOG_FILE = "output.txt"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# ---------------------------------

APP_NAME = "doc_writing_app_v3" # New App Name
USER_ID = "dev_user_01"
SESSION_ID = "123344"
SESSION_ID_BASE = "loop_exit_tool_session" # New Base Session ID
GEMINI_MODEL = "gemini-2.0-flash"

STATE_INITIAL_CONTENT = "initial_content"

# outline
STATE_CURRENT_OUTLINE = "current_outline"
STATE_OUTLINE_CRITICISM = "outline_criticism"

# report
STATE_CURRENT_REPORT = "current_report"
STATE_REPORT_CRITICISM = "report_criticism"

COMPLETION_PHRASE = "No major issues found."

# --- Tool Definition ---
def exit_loop(tool_context: ToolContext):
  """Call this function ONLY when the critique indicates no further changes are needed, signaling the iterative process should end."""
  logger.info(f"[Tool Call] exit_loop triggered by {tool_context.agent_name}")
  tool_context.actions.escalate = True
  # Return empty dict as tools should typically return JSON-serializable output
  return {}

# outline
plan_agent = LlmAgent(
    name="PlanningAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Creative Writing Assistant tasked with planning a report outline from document.
    Draft an outline from {{initial_content}}. An outline will start with the overview followed by each chapter of the report (chapter1, chapter2,... etc.)
    and each chapter has sub chapters if needed. Also, include drafting content in each section (2 - 4 sentences).
    **For example:**
     ```
        -Overview: *content*
            -Chapter1: *content*
                -Chapter1.1: *content*
                -Chapter1.2: *content*
            -Chapter2: *content*
                -Chapter2.1: *content*
                -Chapter2.2: *content*
     ```
    Do not add explanations. Output only the outline.
    """,
    description="Writes an outline of a report",
    output_key=STATE_CURRENT_OUTLINE
)

critic_plan_agent = LlmAgent(
    name="CriticPlanAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Constructive Critic AI reviewing an outline for a report. Your goal is balanced feedback.

    **Outline to Review:**
    ```
    {{current_outline}}
    ```

    **Task:**
    Review the planning outline for clarity, engagement, and basic coherence according to the initial content (if known).

    IF you identify 1-2 *clear and actionable* ways the outline could be improved to better capture the content or enhance reader engagement :
    Provide these specific suggestions concisely. Output *only* the critique text.

    ELSE IF the outline is coherent, addresses the content adequately for its draft, and has no glaring errors or obvious omissions:
    Respond *exactly* with the phrase "{COMPLETION_PHRASE}" and nothing else.

    Do not add explanations. Output only the critique OR the exact completion phrase.
    """,
    description="Reviews the current outline, providing critique if clear improvements are needed, otherwise signals completion.",
    output_key=STATE_OUTLINE_CRITICISM
)

refine_plan_agent = LlmAgent(
    name="RefinePlanAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Creative Writing Assistant refining an outline based on feedback OR exiting the process.
    **Current Outline:**
    ```
    {{current_outline}}
    ```
    **Critique/Suggestions:**
    {{outline_criticism}}

    **Task:**
    Analyze the 'Critique/Suggestions'.
    IF the critique is *exactly* "{COMPLETION_PHRASE}":
    You MUST call the 'exit_loop' function. Do not output any text.
    ELSE (the critique contains actionable feedback):
    Carefully apply the suggestions to improve the 'Current Outline'. Output *only* the refined document text.

    Do not add explanations. Either output the refined document OR call the exit_loop function.
    """,
    description="Refines the document based on critique, or calls exit_loop if critique indicates completion.",
    tools=[exit_loop], # Provide the exit_loop tool
    output_key=STATE_CURRENT_OUTLINE # Overwrites state['current_outline'] with the refined version
)

# report
write_report_agent = LlmAgent(
    name="WriteReportAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Creative Writing Assistant tasked with writing a detailed report for each section (4-5 paragraphs) from the given outline.
    
    **Outline to write a Detailed Report:**
     ```
    {{current_outline}}
     ```
    Do not add explanations. Output only the full final report.
    """,
    description="Writes a detailed report from an outline",
    output_key=STATE_CURRENT_REPORT
)

critic_report_agent = LlmAgent(
    name="CriticReportAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Constructive Critic AI reviewing content in the report. Your goal is balanced feedback.

    **Report to Review:**
    ```
    {{current_report}}
    ```

    **Task:**
    Review the report for clarity, engagement, and basic coherence according to the initial content (if known).

    IF you identify 1-2 *clear and actionable* ways the outline could be improved to better capture the content or enhance reader engagement :
    Provide these specific suggestions concisely. Output *only* the critique text.

    ELSE IF the outline is coherent, addresses the content adequately for its report, and has no glaring errors or obvious omissions:
    Respond *exactly* with the phrase "{COMPLETION_PHRASE}" and nothing else.

    Do not add explanations. Output only the critique OR the exact completion phrase.
    """,
    description="Reviews the current report, providing critique if clear improvements are needed, otherwise signals completion.",
    output_key=STATE_REPORT_CRITICISM
)

refine_report_agent = LlmAgent(
    name="RefineReportAgent",
    model=GEMINI_MODEL,
    include_contents='none',
    instruction=f"""You are a Creative Writing Assistant refining a detailed report based on feedback OR exiting the process.
    **Current Report:**
    ```
    {{current_report}}
    ```
    **Critique/Suggestions:**
    {{report_criticism}}

    **Task:**
    Analyze the 'Critique/Suggestions'.
    IF the critique is *exactly* "{COMPLETION_PHRASE}":
    You MUST call the 'exit_loop' function. Do not output any text.
    ELSE (the critique contains actionable feedback):
    Carefully apply the suggestions to improve the 'Current Outline'. Output *only* the refined document text.

    Do not add explanations. Either output the refined document OR call the exit_loop function.
    """,
    description="Refines the document based on critique, or calls exit_loop if critique indicates completion.",
    tools=[exit_loop], # Provide the exit_loop tool
    output_key=STATE_CURRENT_REPORT # Overwrites state['current_report'] with the refined version
)

# Refinement outline loop agent
refinement_outline_loop = LoopAgent(
    name="RefinementOutlineLoop",
    # Agent order is crucial: Critique first, then Refine/Exit
    sub_agents=[
        critic_plan_agent,
        refine_plan_agent,
    ],
    max_iterations=3 # Limit loops
)

# Refinement report loop agent
refinement_report_loop = LoopAgent(
    name="RefinementReportLoop",
    sub_agents=[
        critic_report_agent,
        refine_report_agent,
    ],
    max_iterations=3 # Limit loops
)

# Overall sequential pipeline
report_agent = SequentialAgent(
    name="IterativeWritingPipeline",
    sub_agents=[
        plan_agent,
        refinement_outline_loop,
        write_report_agent,
        refinement_report_loop
    ],
    description="Writes a detailed report from an outline and then iteratively refines it with critique using an exit tool."
)


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
    Sends an initial content to the agent (overwriting the initial one if needed)
    and runs the workflow.
    """

    # --- Setup Runner and Session ---
    session_service = InMemorySessionService()
    initial_state = {"initial_content": f"{initial_content}"}
    session = session_service.create_session(
        app_name=APP_NAME,
        user_id=USER_ID,
        session_id=SESSION_ID,
        state=initial_state  # Pass initial state here
    )
    # logger.info(f"Initial session state: {session.state}")

    runner = Runner(
        agent=report_agent,  # Pass the custom orchestrator agent
        app_name=APP_NAME,
        session_service=session_service
    )

    current_session = session_service.get_session(app_name=APP_NAME,
                                                  user_id=USER_ID,
                                                  session_id=SESSION_ID)
    if not current_session:
        logger.error("Session not found!")
        return

    # current_session.state["initial_content"] = initial_content

    # logger.info(f"Updated session state initial content")

    content = types.Content(role='user', parts=[types.Part(text=f"{initial_content}")])

    events = runner.run(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    final_response = "No final response captured."
    for event in events:
        if event.is_final_response() and event.content and event.content.parts:
            logger.info(f"Potential final response from [{event.author}]: {event.content.parts[0].text}")
            final_response = event.content.parts[0].text

    logger.info("\n--- Agent Interaction Result ---")
    logger.info(f"Agent Final Response: {final_response}")

    final_session = session_service.get_session(app_name=APP_NAME,
                                                user_id=USER_ID,
                                                session_id=SESSION_ID)
    logger.info("Final Session State:")
    import json
    logger.info(json.dumps(final_session.state, indent=2))
    logger.info("-------------------------------\n")

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