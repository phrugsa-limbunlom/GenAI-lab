import os

from dotenv import load_dotenv
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

from Agent import Agent

if __name__ == "__main__":
    load_dotenv()

    prompt = """You are a smart research assistant. Use the search engine to look up information. \
    You are allowed to make multiple calls (either together or in sequence). \
    Only look up information when you are sure of what you want. \
    If you need to look up some information before asking a follow up question, you are allowed to do that!
    """

    tavily_api_key = os.getenv("TAVILY_API_KEY")

    tool = TavilySearchResults(tavily_api_key=tavily_api_key, max_results=4)  # increased number of results
    print(type(tool))
    print(tool.name)

    openai_api_key = os.getenv("OPENAI_API_KEY")
    model = ChatOpenAI(api_key=openai_api_key, model="gpt-3.5-turbo")

    bot = Agent(model, [tool], system=prompt)

    # from IPython.display import Image
    # Image(bot.graph.get_graph().draw_png())

    messages = [HumanMessage(content="What is the weather in sf?")]
    result = bot.graph.invoke({"messages": messages})

    print(result)
    print(result['message'][-1].content)

    messages = [HumanMessage(content="What is the weather in SF and LA?")]
    result = bot.graph.invoke({"messages": messages})

    print(result['messages'][-1].content)

    query = "Who won the super bowl in 2024? In what state is the winning team headquarters located? \
    What is the GDP of that state? Answer each question."
    messages = [HumanMessage(content=query)]

    model = ChatOpenAI(model="gpt-4o")  # requires more advanced model
    bot = Agent(model, [tool], system=prompt)
    result = bot.graph.invoke({"messages": messages})
    print(result['messages'][-1].content)