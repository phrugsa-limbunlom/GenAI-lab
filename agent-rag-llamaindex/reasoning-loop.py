import os

from llama_index.core.agent import FunctionCallingAgentWorker, AgentRunner

from helper import get_openai_api_key
import nest_asyncio
from utils import get_doc_tools
from llama_index.llms.openai import OpenAI

if __name__ == '__main__':
    OPENAI_API_KEY = get_openai_api_key()
    nest_asyncio.apply()

    current_dir = os.getcwd()
    file_path = os.path.join(current_dir, "document", "metagpt.pdf")

    vector_tool, summary_tool = get_doc_tools(file_path, "metagpt")

    llm = OpenAI(api_key=OPENAI_API_KEY, model="gpt-3.5-turbo", temperature=0)

    agent_worker = FunctionCallingAgentWorker.from_tools(
        [vector_tool, summary_tool],
        llm=llm,
        verbose=True
    )
    agent = AgentRunner(agent_worker)

    response = agent.query(
        "Tell me about the agent roles in MetaGPT, "
        "and then how they communicate with each other."
    )

    print(response.source_nodes[0].get_content(metadata_mode="all"))

    response = agent.chat(
        "Tell me about the evaluation datasets used."
    )

    print(response)

    response = agent.chat(
        "Tell me the results over one of the above datasets."
    )
    print(response)

    # low-level debugging and control
    agent_worker = FunctionCallingAgentWorker.from_tools(
        [vector_tool, summary_tool],
        llm=llm,
        verbose=True
    )

    agent = AgentRunner(agent_worker)

    task = agent.create_task(
        "Tell me about the agent roles in MetaGPT, "
        "and then how they communicate with each other."
    )

    step_output = agent.run_step(task.task_id)

    completed_steps = agent.get_completed_step(task.task_id)

    print(f"Num completed for task {task.task_id}: {len(completed_steps)}")
    print(completed_steps[0].output.sources[0].raw_output)

    upcoming_steps = agent.get_upcoming_steps(task.task_id)
    print(f"Num upcoming steps for task {task.task_id}: {len(upcoming_steps)}")
    print(upcoming_steps[0])

    step_output = agent.run_step(
        task.task_id, input="What about how agents share information?"
    )

    step_output = agent.run_step(task.task_id)

    print(step_output.is_last)

    response = agent.finalize_response(task.task_id)

    print(str(response))
