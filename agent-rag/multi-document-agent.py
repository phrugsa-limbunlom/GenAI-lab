import nest_asyncio
from llama_index.core import VectorStoreIndex
from llama_index.core.agent import AgentRunner
from llama_index.core.agent import FunctionCallingAgentWorker
from llama_index.core.objects import ObjectIndex
from llama_index.llms.openai import OpenAI

from helper import get_openai_api_key
from utils import get_paper_tools

if __name__ == '__main__':
    OPENAI_API_KEY = get_openai_api_key()
    nest_asyncio.apply()

    papers = [
        "metagpt.pdf",
        "longlora.pdf",
        "selfrag.pdf",
    ]

    papers2 = [
        "metagpt.pdf",
        "longlora.pdf",
        "loftq.pdf",
        "swebench.pdf",
        "selfrag.pdf",
        "zipformer.pdf",
        "values.pdf",
        "finetune_fair_diffusion.pdf",
        "knowledge_card.pdf",
        "metra.pdf",
    ]

    paper_tools = get_paper_tools(papers)

    initial_tools = [tool for paper in papers for tool in paper_tools]

    llm = OpenAI(api_key=get_openai_api_key(), model="gpt-3.5-turbo")

    print(len(initial_tools))

    agent_worker = FunctionCallingAgentWorker.from_tools(
        initial_tools,
        llm=llm,
        verbose=True
    )
    agent = AgentRunner(agent_worker)
    response = agent.query(
        "Tell me about the evaluation dataset used in LongLoRA, "
        "and then tell me about the evaluation results"
    )

    response = agent.query("Give me a summary of both Self-RAG and LongLoRA")
    print(str(response))

    # extend the agent with tool retrieval
    paper_tools = get_paper_tools(papers2)
    all_tools = [t for paper in papers for t in paper_tools]

    # index tools to vector store for performance retrival
    obj_index = ObjectIndex.from_tools(
        all_tools,
        index_cls=VectorStoreIndex,
    )

    obj_retriever = obj_index.retrieve(similarity_top_k=3)

    tools = obj_retriever.retrive(
        " Tell me about the eval dataset used in MetaGPT and SWE-Bench"
    )

    print(tools[2].metadata)

    agent_worker = FunctionCallingAgentWorker.from_tools(
        tool_retriever=obj_retriever,
        llm=llm,
        system_prompt=""" \
        You are an agent designed to answer queries over a set of given papers.
        Please always use the tools provided to answer a question. Do not rely on prior knowledge.\
        """,
        verbose=True
    )

    agent = AgentRunner(agent_worker)
    response = agent.query(
        "Tell me about the evaluation dataset used "
        "in MetaGPT and compare it against SWE-Bench"
    )
    print(str(response))

    response = agent.query(
        "Compare and contrast the LoRA papers (LongLoRA, LoftQ). "
        "Analyze the approach in each paper first. "
    )
    print(str(response))
