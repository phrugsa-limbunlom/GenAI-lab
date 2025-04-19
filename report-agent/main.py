import functools
import json
import os
from typing import List, Dict

from dotenv import find_dotenv, load_dotenv
from mistralai import Mistral

def find_sub_folders(path: str) -> List[str] | None:
    """List all the subfolders under the path directory."""
    # Exclude common non-code directories
    folders = [path + "/" + f.name for f in os.scandir(path)
               if f.is_dir() and f.name not in [".git", ".idea", "venv", ".venv", "db", "__pycache__"]]
    if not folders:
        return None
    return folders

def analyze_folder_structure(messages: List[Dict], folders : List[str]) -> List[Dict]:
    """Recursively find the sub folders until no more directory (no more tools call)."""

    print(messages)
    response = analyze_structure_agent.chat.complete(
        model=model,
        messages=messages
    )
    msg = response.choices[0].message
    print(msg)

    if not msg.tool_calls:
        messages.append(msg)
        return messages

    # Build tool response messages
    tool_messages = []
    for tool_call in msg.tool_calls:
        function_name = tool_call.function.name
        params = json.loads(tool_call.function.arguments)
        result = ",".join(names_to_functions[function_name](**params) or [])

        folders.append(result)

        print(f"\nfunction_name: {function_name}\nfunction_params: {params}\nfunction_result: {result}")

        tool_messages.append({
            "role": "tool",
            "name": function_name,
            "content": result,
            "tool_call_id": tool_call.id
        })

    # Append assistant message and all tool messages
    messages.append(msg)
    messages.extend(tool_messages)

    # Recurse until no more directories
    return analyze_folder_structure(messages, folders)

if __name__ == "__main__":
    source = "C:/Users/asus/PycharmProjects/GenAI-lab"

    load_dotenv(find_dotenv())
    api_key = os.environ["MISTRAL_API_KEY"]
    model = "mistral-large-latest"

    # analyze folder structure agent
    analyze_structure_agent = Mistral(api_key=api_key)

    analyze_structure_query =  (f" Here is the path directory : {source}."
                                " Analyze folder structure in the codebase from the path directory given. "
                                " Using tool to find sub folders from the list of folders you have until you can't get any directory from the tool."
                                " For example: You get all of these directories from tool 'analyze_folder_structure' - path/folder1, path/folder2, path/folder3."
                                " You have to go further to each directory until you cannot find directory anymore."
                                " For example: analyze_folder_structure(path/folder1) to get path/folder1/sub_folder1, path/folder1/sub_folder2,"
                                "              analyze_folder_structure(path/folder2) to get path/folder1/sub_folder1, path/folder1/sub_folder2,"
                                "              analyze_folder_structure(path/folder3) to get path/folder1/sub_folder1, path/folder1/sub_folder2")


    # define the function tool
    tools = [
        {
            "type": "function",
            "function": {
                "name": "find_sub_folders",
                "description": "List all the sub folders under the path directory",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "path directory."}
                    },
                    "required": ["path"],
                },
            },
        }
    ]

    # initial query
    messages = [{"role": "user", "content": analyze_structure_query}]
    response = analyze_structure_agent.chat.complete(
        model=model,
        messages=messages,
        tools=tools,
        tool_choice="any",
        parallel_tool_calls=False,
    )
    messages.append(response.choices[0].message)

    names_to_functions = {
        "find_sub_folders": functools.partial(find_sub_folders)
    }

    # list of all directories
    folders = []

    # initial tool calls
    initial_tool_msgs = []
    for tool_call in response.choices[0].message.tool_calls:
        function_name = tool_call.function.name
        params = json.loads(tool_call.function.arguments)
        result = ",".join(names_to_functions[function_name](**params) or [])

        folders.append(result)
        print(f"\nfunction_name: {function_name}\nfunction_params: {params}\nfunction_result: {result}")

        initial_tool_msgs.append({
            "role": "tool",
            "name": function_name,
            "content": result,
            "tool_call_id": tool_call.id
        })

    messages.extend(initial_tool_msgs)

    full_messages = analyze_folder_structure(messages, folders)

    # final_reply = full_messages[-1].content
    # print("Final structured report:\n", final_reply)

    # construct folder structure agent
    construct_structure_query = (f" Here are the path directories : {folders}"
                                " output.txt the folder structure for further analysis. The output.txt format is "
                                " path/folder1"
                                "    - path/folder1/sub_folder1"
                                "    - path/folder1/sub_folder2"
                                " path/folder2"
                                "    - path/folder2/sub_folder1"
                                "    - path/folder2/sub_folder2"
                                " path/folder3"
                                "    - path/folder3/sub_folder1"
                                "    - path/folder3/sub_folder2"
                                "as an example")

    print(construct_structure_query)

    construct_structure_agent = Mistral(api_key=api_key)

    new_messages = [{"role": "user", "content": construct_structure_query}]

    response = construct_structure_agent.chat.complete(
        model=model,
        messages=new_messages,
        parallel_tool_calls=False,
    )
    structure = response.choices[0].message.content
    print(structure)

    # write report agent
    write_report_query = ("You are a helpful assistant. Write a detail report from the folder structure given. Explain and elaborate them in details."
                          f" Here is the folder structure : {structure}")
    print(write_report_query)

    write_report_agent = Mistral(api_key=api_key)
    
    new_messages = [{"role": "user", "content": write_report_query}]
    response = write_report_agent.chat.complete(
        model=model,
        messages=new_messages,
        parallel_tool_calls=False,
    )
    print(response.choices[0].message.content)