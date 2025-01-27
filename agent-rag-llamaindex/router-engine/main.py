from llama_index.core import SimpleDirectoryReader, Settings, SummaryIndex, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.query_engine import RouterQueryEngine
from llama_index.core.selectors import LLMSingleSelector
from llama_index.core.tools import QueryEngineTool
from llama_index.legacy import OpenAIEmbedding
from llama_index.llms.openai import OpenAI

from helper import get_openai_api_key
import nest_asyncio

from utils import get_router_query_engine

if __name__ == '__main__':
    OPENAI_API_KEY = get_openai_api_key()

    nest_asyncio.apply()

    # load data
    documents = SimpleDirectoryReader(input_files=["metagpt.pdf"]).load_data()

    # define llm and embedding model
    splitter = SentenceSplitter(chunk_size=1024)
    nodes = splitter.get_nodes_from_documents(documents)

    Settings.llm = OpenAI(api_key=OPENAI_API_KEY, model="gpt-3.5-turbo")
    Settings.embed_model = OpenAIEmbedding(model="text-embedding-ada-002")

    # define summary and vector index
    summary_index = SummaryIndex(nodes)
    vector_index = VectorStoreIndex(nodes)

    # define query tools and set metadata
    summary_query_engine = summary_index.as_query_engine(
        response_mode="tree_summarize",
        use_async=True,
    )
    vector_query_engine = vector_index.as_query_engine()

    # for summarize query
    summary_tool = QueryEngineTool.from_defaults(
        query_engine=summary_query_engine,
        description=(
            "Useful for summarization questions related to MetaGPT"
        ),
    )

    # for Q&A query
    vector_tool = QueryEngineTool.from_defaults(
        query_engine=vector_query_engine,
        description=(
            "Useful for retrieving specific context from the MetaGPT paper."
        ),
    )

    # define router query engine
    router_query_engine = RouterQueryEngine(
        selector=LLMSingleSelector.from_defaults(),
        query_engine_tools=[summary_tool, vector_tool],
        verbose=True
    )

    response = router_query_engine.query("What is the summary of the document?")
    print(str(response))

    print(len(response.source_nodes))

    # call from utils
    query_engine = get_router_query_engine("metagpt.pdf")

    response = query_engine.query("Tell me about the ablation study results?")
    print(str(response))
