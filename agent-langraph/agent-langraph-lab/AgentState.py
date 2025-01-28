from typing import TypedDict, Annotated
from langchain_core.messages import AnyMessage
import operator

class AgentState(TypedDict):
    message: Annotated[list[AnyMessage], operator.add]
