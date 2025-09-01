import os
from dotenv import load_dotenv
import google.generativeai as genai
import sqlparse
import json
import re
from openai import OpenAI
from functools import lru_cache

# Load environment variables
load_dotenv()

# Configure the Gemini API
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

# Configure OpenRouter for DeepSeek
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
SITE_URL = os.getenv("SITE_URL", "https://querytoSQL.app")
SITE_NAME = os.getenv("SITE_NAME", "QueryToSQL Assistant")

# Model configuration - set to "gemini" or "deepseek"
LLM_MODEL = os.getenv("LLM_MODEL", "gemini")

# Global variables to store the initialized model instances
_gemini_model = None
_openrouter_client = None

def init_model():
    """Initialize the chosen LLM model and return a global instance"""
    global _gemini_model, _openrouter_client
    
    if LLM_MODEL == "gemini":
        if not GOOGLE_API_KEY:
            raise ValueError("Google API key not found. Please set the GOOGLE_API_KEY environment variable.")
        
        # Use existing model instance if available
        if _gemini_model is None:
            _gemini_model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')
        return _gemini_model
    elif LLM_MODEL == "deepseek":
        if not OPENROUTER_API_KEY:
            raise ValueError("OpenRouter API key not found. Please set the OPENROUTER_API_KEY environment variable.")
        
        # Use existing client instance if available
        if _openrouter_client is None:
            _openrouter_client = OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=OPENROUTER_API_KEY,
            )
        return _openrouter_client
    else:
        raise ValueError(f"Unsupported model: {LLM_MODEL}. Set LLM_MODEL to 'gemini' or 'deepseek'")

# Initialize the model at module import time
try:
    init_model()
    print(f"Successfully initialized {LLM_MODEL} model at startup")
except Exception as e:
    print(f"Warning: Failed to initialize {LLM_MODEL} model at startup: {str(e)}")

def call_llm(prompt, temperature=0.2):
    """
    Generic function to call the configured LLM
    
    Args:
        prompt: The prompt to send to the LLM
        temperature: Temperature for generation (lower is more deterministic)
        
    Returns:
        The LLM's response as text
    """
    if LLM_MODEL == "gemini":
        model = init_model()  # Will use the cached instance
        # Respect temperature to reduce first-run variability and improve determinism when set to 0.0
        try:
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(temperature=temperature)
            )
        except Exception:
            # Fallback for older SDKs that may not support types.GenerationConfig
            response = model.generate_content(prompt)
        return getattr(response, "text", str(response))
    elif LLM_MODEL == "deepseek":
        client = init_model()  # Will use the cached instance
        completion = client.chat.completions.create(
            extra_headers={
                "HTTP-Referer": SITE_URL,
                "X-Title": SITE_NAME,
            },
            model="deepseek/deepseek-r1-0528:free",
            temperature=temperature,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        return completion.choices[0].message.content
    else:
        raise ValueError(f"Unsupported model: {LLM_MODEL}")

def extract_query_entities(query):
    """
    Analyze the query to identify key entities and intent
    
    Args:
        query: The natural language query
        
    Returns:
        Dictionary with extracted entities and intent
    """
    prompt = f"""Analyze this query about an invoice reporting system and extract:
1. The main tables that would be involved
2. The filtering conditions (if any)
3. The attributes or columns being requested
4. The intent (SELECT, COUNT, SUM, AVG, etc.)

Query: "{query}"

Respond with a JSON object containing these keys: tables, conditions, attributes, intent.
"""
    
    response_text = call_llm(prompt)
    
    try:
        # Try to extract JSON from the response
        json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
        if json_match:
            json_text = json_match.group(1)
        else:
            # Try to find content between curly braces
            json_match = re.search(r'(\{.*\})', response_text, re.DOTALL)
            if json_match:
                json_text = json_match.group(1)
            else:
                # Fallback to the entire text
                json_text = response_text
                
        entities = json.loads(json_text)
        return entities
    except:
        # If JSON parsing fails, return a simple structure
        return {
            "tables": [],
            "conditions": [],
            "attributes": [],
            "intent": "SELECT"
        }

@lru_cache(maxsize=128)
def nl_to_sql(query, schema_description, relationships_description=None, table_descriptions=None, 
              column_samples=None, join_paths=None, query_patterns=None):
    """
    Convert a natural language query to SQL using the configured LLM.
    This function is cached to avoid repeated LLM calls for the same query.
    
    Args:
        query: The natural language query from the user
        schema_description: Database schema description
        relationships_description: Description of table relationships
        table_descriptions: Description of tables
        column_samples: Sample data for columns
        join_paths: Common join patterns
        query_patterns: Common query patterns
        
    Returns:
        The generated SQL query
    """
    # Create a prompt that focuses only on SQL generation
    system_prompt = """You are an expert SQL generator for Microsoft SQL Server (MSSQL). Your task is to convert natural language questions into syntactically correct MSSQL queries based on the provided schema and context.
    
Guidelines:
1. ALWAYS output ONLY valid MSSQL syntax without any explanations or comments.
2. The user is asking about job details, so ALWAYS query the GetJobDetails_FieldService view.
3. Use appropriate WHERE clauses to filter based on the user's request.
4. Ensure column references are fully qualified (e.g., jd.ColumnName) and use "jd" as the alias for GetJobDetails_FieldService.
5. Use proper MSSQL-specific syntax (TOP instead of LIMIT, GETDATE() instead of NOW(), etc.).
6. Format SQL properly with each clause on a new line for readability.
7. Use IS NULL checks instead of = NULL.
8. Add proper ORDER BY clauses for logical sorting.
"""
    
    if table_descriptions:
        system_prompt += "\n\nView Description:\n" + table_descriptions
    
    system_prompt += "\n\nView Schema:\n" + schema_description
    
    if relationships_description:
        system_prompt += "\n\nContextual Information:\n" + relationships_description
    
    if join_paths:
        system_prompt += "\n\nCommon Join Patterns:\n" + join_paths
    
    if query_patterns:
        system_prompt += "\n\nCommon Query Patterns:\n" + query_patterns
    
    if column_samples:
        system_prompt += "\n\nColumn Sample Data:\n" + column_samples
    
    # Add SQL examples to guide the model's output
    examples = """
Example 1:
User query: "Show me all jobs for customer ABC with their operators and vehicles"
SQL:
```sql
SELECT 
    jd.CustomerName,
    jd.JobId,
    jd.Operation,
    jd.Operator,
    jd.Vehicle,
    jd.ShiftDate
FROM GetJobDetails_FieldService jd
WHERE jd.CustomerName LIKE '%ABC%'
ORDER BY jd.ShiftDate DESC
```

Example 2:
User query: "Find all jobs with total amount greater than 1000 in the last month"
SQL:
```sql
SELECT 
    jd.CustomerName,
    jd.JobId,
    jd.Total,
    jd.ShiftDate,
    jd.Operation
FROM GetJobDetails_FieldService jd
WHERE 
    jd.Total > 1000
    AND CONVERT(date, jd.ShiftDate) >= DATEADD(month, -1, GETDATE())
ORDER BY jd.Total DESC
```
"""
    
    system_prompt += "\n\n" + examples
    
    # Add final instruction and user query
    prompt = f"{system_prompt}\n\nNow, convert this query to SQL: {query}\n\nSQL:"
    
    # Call the configured LLM and get the response
    sql_query = call_llm(prompt, temperature=0.0)
    
    # Clean up the SQL if it's wrapped in a code block
    if sql_query.startswith("```sql"):
        sql_query = sql_query.split("```sql")[1].split("```")[0].strip()
    elif sql_query.startswith("```"):
        sql_query = sql_query.split("```")[1].split("```")[0].strip()
    
    # Format the SQL nicely
    try:
        sql_query = sqlparse.format(
            sql_query, 
            reindent=True, 
            keyword_case='upper'
        )
    except:
        # If formatting fails, return the original query
        pass
        
    return sql_query 