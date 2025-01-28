import os

from dotenv import load_dotenv
from langchain_community.tools import TavilySearchResults
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver

from Agent import Agent

if __name__ == '__main__':

    _ = load_dotenv()

    TAVILY_API_KEY = os.getenv('TAVILY_API_KEY')
    tool = TavilySearchResults(api_key=TAVILY_API_KEY, max_results=2)

    with SqliteSaver.from_conn_string(":memory:") as memory:

        prompt = """You are a smart research assistant. Use the search engine to look up information. \
        You are allowed to make multiple calls (either together or in sequence). \
        Only look up information when you are sure of what you want. \
        If you need to look up some information before asking a follow up question, you are allowed to do that!
        """

        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        model = ChatOpenAI(api_key=OPENAI_API_KEY, model="gpt-3.5-turbo")
        abot = Agent(model, [tool], system=prompt, checkpointer=memory)

        messages = [HumanMessage(content="Whats the weather in SF?")]
        thread = {"configurable": {"thread_id": "1"}}
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)

        print(abot.graph.get_state(thread))

        print(abot.graph.get_state(thread).next)

        for event in abot.graph.stream(None, thread):
            for v in event.values():
                print(v)

        # continue after interrupt

        for event in abot.graph.stream(None, thread):
            for v in event.values():
                print(v)

        print(abot.graph.get_state(thread))
        print(abot.graph.get_state(thread).next)

        messages = [HumanMessage("Whats the weather in LA?")]
        thread = {"configurable": {"thread_id": "2"}}
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)
        while abot.graph.get_state(thread).next:
            print("\n", abot.graph.get_state(thread), "\n")
            _input = input("proceed?")
            if _input != "y":
                print("aborting")
                break
            for event in abot.graph.stream(None, thread):
                for v in event.values():
                    print(v)

        # modify state
        messages = [HumanMessage("Whats the weather in LA?")]
        thread = {"configurable": {"thread_id": "3"}}
        for event in abot.graph.stream({"messages": messages}, thread):
            for v in event.values():
                print(v)

        print(abot.graph.get_state(thread))

        current_values = abot.graph.get_state(thread)

        print(current_values.values['messages'][-1])
        print(current_values.values['messages'][-1].tool_calls)

        _id = current_values.values['messages'][-1].tool_calls[0]['id']

        # update message with state id
        current_values.values['messages'][-1].tool_calls = [
            {'name': 'tavily_search_results_json',
             'args': {'query': 'current weather in Louisiana'},
             'id': _id}
        ]

        print(abot.graph.update_state(thread, current_values.values))

        print(abot.graph.get_state(thread))

        for event in abot.graph.stream(None, thread):
            for v in event.values():
                print(v)

    # time travel
    states = []
    for state in abot.graph.get_state_history(thread):
        print(state)
        print('--')
        states.append(state)

    to_replay = states[-3]
    print(to_replay)  # back to previous state (the latest state is -1)

    to_replay = states[-1]
    print(to_replay)

    # go back in time and edit
    _id = to_replay.values['messages'][-1].tool_calls[0]['id']
    to_replay.values['messages'][-1].tool_calls = [{'name': 'tavily_search_results_json',
                                                    'args': {'query': 'current weather in LA, accuweather'},
                                                    'id': _id}]  # update query in the tool

    branch_state = abot.graph.update_state(to_replay.config, to_replay.values)

    for event in abot.graph.stream(None, branch_state):
        for k, v in event.items():
            if k != "__end__":
                print(v)

    # add messages to a state at a given time
    print(to_replay)

    _id = to_replay.values['messages'][-1].tool_calls[0]['id']

    state_update = {"messages": [ToolMessage(
        tool_call_id=_id,
        name="tavily_search_results_json",
        content="54 degree celcius",
    )]}

    # as_node : update the state as were in action node (no need to go to action node after update)
    branch_and_add = abot.graph.update_state(
        to_replay.config,
        state_update,
        as_node="action")

    for event in abot.graph.stream(None, branch_and_add):
        for k, v in event.items():
            print(v)