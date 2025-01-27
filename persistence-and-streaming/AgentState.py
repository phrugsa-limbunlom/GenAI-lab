from typing import TypedDict

from langchain_core.messages import AnyMessage
from typing_extensions import Annotated
import operator

class AgentState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]