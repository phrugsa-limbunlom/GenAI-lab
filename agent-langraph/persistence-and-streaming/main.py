import asyncio
import os

from dotenv import load_dotenv
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from Agent import Agent

if __name__ == '__main__':
    load_dotenv()

    prompt = """You are a smart research assistant. Use the search engine to look up information. \
    You are allowed to make multiple calls (either together or in sequence). \
    Only look up information when you are sure of what you want. \
    If you need to look up some information before asking a follow up question, you are allowed to do that!
    """

    openai_api_key = os.getenv("OPENAI_API_KEY")
    model = ChatOpenAI(api_key=openai_api_key, model="gpt-4o")

    tavily_api_key = os.getenv("TAVILY_API_KEY")
    tool = TavilySearchResults(api_key=tavily_api_key, max_results=2)

    # Initialize SqliteSaver from context manager
    with SqliteSaver.from_conn_string(":memory:") as memory:
        abot = Agent(model, [tool], system=prompt, checkpointer=memory)

        messages = [HumanMessage(content="What is the weather in sf?")]
        thread = {"configurable": {"thread_id": "1"}}

        stream = abot.graph.stream({"messages": messages}, thread)

        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v['messages'])

        messages = [HumanMessage(content="What about in la?")]
        thread = {"configurable": {"thread_id": "1"}}  # track the previous question from the same thread
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)

        messages = [HumanMessage(content="Which one is warmer?")]
        thread = {"configurable": {"thread_id": "1"}}
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)

        # different thread means LLM does not have the context from previous question
        messages = [HumanMessage(content="Which one is warmer?")]
        thread = {"configurable": {"thread_id": "2"}}
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)

    # Streaming tokens with AsyncSqliteSaver
    with AsyncSqliteSaver.from_conn_string(":memory:") as memory:
        abot = Agent(model, [tool], system=prompt, checkpointer=memory)

        messages = [HumanMessage(content="What is the weather in SF?")]
        thread = {"configurable": {"thread_id": "4"}}


        async def process_events(messages, thread):
            async for event in abot.graph.astream_events({"messages": messages}, thread, version="v1"):
                kind = event["event"]
                if kind == "on_chat_model_stream":
                    content = event["data"]["chunk"].content
                    if content:
                        # Empty content in the context of OpenAI means
                        # that the model is asking for a tool to be invoked.
                        # So we only print non-empty content
                        print(content, end="|")


        asyncio.run(process_events(messages, thread))
