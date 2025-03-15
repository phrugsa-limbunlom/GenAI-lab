import os
from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

load_dotenv(find_dotenv())

required_env_vars = {
    "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
    "SNOWFLAKE_ACCOUNT" : os.getenv("SNOWFLAKE_ACCOUNT")
}

snowflake_database = os.getenv("SNOWFLAKE_DATABASE")


# Check for missing or empty environment variables
missing_vars = [var for var, value in required_env_vars.items() if not value or len(str(value).strip()) == 0]
if missing_vars:
    raise ValueError(f"Missing or empty required environment variables: {', '.join(missing_vars)}")

conn_params = {
    "user": quote_plus(required_env_vars["SNOWFLAKE_USER"]),
    "password": quote_plus(required_env_vars["SNOWFLAKE_PASSWORD"]),
    "account": quote_plus(required_env_vars["SNOWFLAKE_ACCOUNT"]),
    "database": snowflake_database,
    "warehouse": quote_plus(required_env_vars["SNOWFLAKE_WAREHOUSE"]),
    "role": quote_plus(required_env_vars["SNOWFLAKE_ROLE"]),
}

# Construct Snowflake connection string
connection_string = (
    f"snowflake://{conn_params['user']}:{conn_params['password']}@"
    f"{conn_params['account']}/{conn_params['database']}"
    f"?warehouse={conn_params['warehouse']}&role={conn_params['role']}"
    # f"&schema={conn_params['schema']}"
)

print("\nAttempting to connect to Snowflake...")
try:
    
    engine = create_engine(connection_string)
    
    # Test connection and get available schemas
    with engine.connect() as conn:
        print("Successfully connected to Snowflake!")

        # Get available schemas using proper SQLAlchemy text construct
        result = conn.execute(text("SHOW SCHEMAS"))
        schemas = result.fetchall()
        
        db_list = []
        for schema in schemas:

            if schema[1] in ["CUSTOMER", "PRODUCT","MARKETING","TRANSACTION"]:
                print("\nAvailable schemas:", schema[1])

                result = conn.execute(text(f"SHOW TABLES IN SCHEMA {snowflake_database}.{schema[1]}"))
                tables = result.fetchall()

                print(f"\nTables in {schema[1]} schema:", [table[1] for table in tables])
            
                # Initialize database connection for LangChain
                db = SQLDatabase.from_uri(
                    connection_string,
                    schema=schema[1],  # Explicitly set schema
                    sample_rows_in_table_info=3  # Include sample rows for better context
                )

                db_list.append(db)

    print(f"Successfully created database connection object")

except Exception as e:
    print(f"Error connecting to database: {str(e)}")
    print("\nConnection string (with password hidden):", 
          connection_string.replace(conn_params['password'], '***'))
    raise

# Install Ollama: https://ollama.com/ and run the following command:
# ollama serve
# ollama pull deepseek-r1   

llama2_chat = ChatOllama(model="deepseek-r1")

llm = llama2_chat

def get_schema(_):
    try:

        print("\ndb_list:", db_list)

        schema_info = "".join([db.get_table_info() for db in db_list])
       
        print("\nSchema Information:", schema_info)

        return schema_info
    except Exception as e:
        print(f"Error getting schema information: {str(e)}")
        raise

def run_query(query):
    return db.run(query)


template = """Analyze and answer the user's question: {question} using the schema provided below:
{schema}.

Response with the detailed answer to the question, and providing the SQL query using only fields and tables represented in the schema as the part of the response.
Please noted that the SQL query has to use only the exact fields and tables' names from the schema.

Here is the example of question and response:

question ="Do we use any GEO location data in our analysis?"

response = "Yes, GEO location data is used in the analysis. Here's how:

PROD.common_prep.prep_location_country: This table transforms MaxMind country data to map countries to regions (EMEA, AMER, LATAM, APAC).
PROD.restricted_safe_common_prep.prep_sfdc_account: This table uses account_demographics_geo__c for account GEO.
PROD.restricted_safe_common_prep.prep_crm_opportunity: The sfdc_opportunity CTE includes crm_opp_owner_geo_stamped, parent_crm_account_geo which are used in final table."

"""

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", "You are a helpful assistant that can answer questions about the data in Snowflake."),
        ("human", template),
    ]
)

sql_response = (
    RunnablePassthrough.assign(schema=get_schema)
    | prompt
    | llm.bind(stop=["\nSQLResult:"])
    | StrOutputParser()
)

response = sql_response.invoke({"question": "How can I trace a customer's journey across touchpoints, transactions, and feedback?"})

# Noted: The response in <think> </think> is the thinking stage of Deepseek model.
print(response)