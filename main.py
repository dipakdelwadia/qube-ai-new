from fastapi import FastAPI, HTTPException, Header, Request, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from typing import List, Dict, Optional, Any, Union
import re
import logging
import urllib.parse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(process)d] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %z'
)

logger = logging.getLogger(__name__)

from flow import create_nl_to_sql_flow, create_sql_generation_flow
from nodes import GenerateInsights
from uuid import uuid4
import threading
import time

# --- Pydantic Models ---
class QueryRequest(BaseModel):
    query: str
    conversation_history: Optional[List[Dict[str, str]]] = None
    environment: Optional[str] = None  # Add optional environment field
    show_charts: Optional[bool] = False  # Add optional show_charts field

class ChartData(BaseModel):
    type: str  # 'bar', 'line', 'pie', 'doughnut', 'area', 'scatter', 'kpi', 'pivot', etc.
    title: Optional[str] = None
    labels: Optional[List[str]] = None  # Not required for KPI widgets
    datasets: Optional[List[Dict[str, Any]]] = None  # Not required for KPI widgets
    # KPI-specific fields
    value: Optional[Union[int, float, str]] = None  # For KPI widgets
    format: Optional[str] = None  # For KPI widgets (number, text, currency, etc.)
    # Pivot-specific fields
    data: Optional[List[Dict[str, Any]]] = None  # For pivot tables and raw data
    numeric_columns: Optional[List[str]] = None  # For pivot tables
    categorical_columns: Optional[List[str]] = None  # For pivot tables
    # Common options
    options: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    question: Optional[str] = None
    conversation_history: List[Dict[str, str]] = []
    chart: Optional[ChartData] = None
    insights: Optional[str] = None  # Add insights field
    follow_up_questions: Optional[List[str]] = None  # Add follow-up questions field
    request_id: Optional[str] = None  # Background insights job id

class SQLQueryRequest(BaseModel):
    sql_query: str
    environment: Optional[str] = None

# --- Greeting Patterns and Responses ---
# Dictionary of greeting patterns and their hard-coded responses
GREETING_PATTERNS = {
    # Basic greetings
    r"^\s*hi\s*$|^\s*hello\s*$|^\s*hey\s*$": "Hello! How can I help you with your job-related queries today?",
    r"^\s*yo\s*$|^\s*sup\s*$|^\s*what'?s up\s*$": "Hey there! What job information would you like to know about?",
    r"^\s*greetings\s*$|^\s*howdy\s*$": "Greetings! I'm here to assist with your job database queries.",
    
    # Time-specific greetings
    r"^\s*good morning\s*$": "Good morning! How can I assist you with job information today?",
    r"^\s*good afternoon\s*$": "Good afternoon! What job information would you like to know?",
    r"^\s*good evening\s*$": "Good evening! How can I help you with your job queries?",
    r"^\s*good night\s*$": "Good night! Feel free to come back anytime for job information.",
    
    # Farewells
    r"^\s*bye\s*$|^\s*goodbye\s*$|^\s*see you\s*$": "Goodbye! Feel free to come back if you have more questions.",
    r"^\s*later\s*$|^\s*see ya\s*$|^\s*cya\s*$": "See you later! I'll be here when you need more job information.",
    r"^\s*take care\s*$|^\s*farewell\s*$": "Take care! I'm always here to help with your job queries.",
    
    # Gratitude
    r"^\s*thanks\s*$|^\s*thank you\s*$|^\s*thx\s*$": "You're welcome! Is there anything else you'd like to know?",
    r"^\s*appreciate it\s*$|^\s*thanks a lot\s*$": "Happy to help! Do you need any other job information?",
    r"^\s*great job\s*$|^\s*well done\s*$": "Thank you for the feedback! What other job details would you like to know?",
    
    # Status inquiries
    r"^\s*how are you\s*$": "I'm doing well, thank you! I'm ready to help you with your job-related queries.",
    r"^\s*how'?s it going\s*$|^\s*how are things\s*$": "Everything's going smoothly! Ready to assist with your job database questions.",
    
    # Bot identity
    r"^\s*who are you\s*$|^\s*what are you\s*$": "I'm your job database assistant, designed to help you query job information quickly and efficiently.",
    r"^\s*what'?s your name\s*$": "I'm your Job Query Assistant. I don't have a personal name, but I'm here to help with all your job data needs!",
    
    # Help requests
    r"^\s*help\s*$": "I can help you query job information. Try asking questions like 'Show me all jobs for customer X' or 'What are the pending jobs?'",
    r"^\s*i need help\s*$|^\s*assist me\s*$": "I'm here to help! You can ask me about job details, customer information, or specific job statuses.",
    r"^\s*what can you do\s*$|^\s*what do you do\s*$": "I can search and retrieve job information from the database. Try asking about specific jobs, customers, or job statuses.",
    
    # Capabilities
    r"^\s*examples\s*$|^\s*sample queries\s*$": "Here are some example queries you can try:\n- Show all jobs for customer Smith\n- List pending jobs\n- Find jobs scheduled for next week\n- Show completed jobs for technician John",
    r"^\s*features\s*$|^\s*capabilities\s*$": "I can search jobs by customer, status, date, technician, and more. I can also provide detailed information about specific jobs when you need it.",
    
    # Miscellaneous
    r"^\s*ok\s*$|^\s*okay\s*$|^\s*k\s*$": "Great! What job information would you like to know?",
    r"^\s*cool\s*$|^\s*awesome\s*$|^\s*nice\s*$": "Glad you think so! How can I help you with job data today?",
    r"^\s*test\s*$": "The system is working correctly. What job information would you like to query?",
    r"^\s*start\s*$|^\s*begin\s*$": "I'm ready to help you query job information. What would you like to know?"
}

def check_for_greeting(query):
    """Check if the query matches any greeting pattern and return the appropriate response"""
    query = query.lower().strip()
    for pattern, response in GREETING_PATTERNS.items():
        if re.match(pattern, query, re.IGNORECASE):
            return response
    return None

def is_meaningless_query(query, conversation_history=None):
    """
    Check if the query appears to be meaningless gibberish or too vague and return clarification request if needed.
    
    Args:
        query: The user's input query
        conversation_history: Optional conversation history to check for context
        
    Returns:
        str or None: Clarification message if query is meaningless, None otherwise
    """
    if not query or not isinstance(query, str):
        return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    query = query.strip()
    
    # Check for empty or very short queries
    if len(query) < 2:
        return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    # CONTEXT-AWARE CHECK: If we have conversation history, check if this could be a follow-up response
    if conversation_history and len(conversation_history) > 0:
        # Get the last assistant message to see if it was asking a question
        last_assistant_msg = None
        for msg in reversed(conversation_history):
            if msg.get("role") == "assistant":
                last_assistant_msg = msg.get("content", "")
                break
        
        if last_assistant_msg:
            # More flexible approach: Check if the last assistant message contains question indicators
            # that typically suggest it's asking for follow-up information
            question_patterns = [
                r'\b(?:which|what|when|where|who|how)\b',  # Question words
                r'\?',  # Contains question mark
                r'\bspecify\b',  # "Please specify..."
                r'\binterested\b',  # "interested in"
                r'\bchoose\b',  # "choose from"
                r'\bselect\b',  # "select a"
                r'\bprovide\b',  # "provide more"
                r'\benter\b',   # "enter the"
                r'\btell me\b', # "tell me"
                r'\blet me know\b',  # "let me know"
                r'\bneed\b.*\bmore\b',  # "need more information"
                r'\bplease\b.*\b(?:clarify|explain|detail)\b'  # "please clarify/explain/detail"
            ]
            
            last_msg_lower = last_assistant_msg.lower()
            
            # Check if the last message seems like a question or request for clarification
            is_likely_question = any(re.search(pattern, last_msg_lower) for pattern in question_patterns)
            
            if is_likely_question:
                # This could be a valid follow-up response
                # Check if the current query could be a reasonable answer
                
                # Allow various types of reasonable short responses:
                # 1. Numbers (years, IDs, quantities, etc.)
                if re.match(r'^\d+$', query):
                    return None  # Numbers are valid follow-up responses
                
                # 2. Simple text responses (names, statuses, simple phrases)
                if re.match(r'^[a-zA-Z][a-zA-Z0-9\s\-_.,]{0,100}$', query) and len(query.split()) <= 10:
                    return None  # Simple text responses are valid
                
                # 3. Common status/option words
                common_answers = [
                    'yes', 'no', 'all', 'none', 'pending', 'completed', 'cancelled', 
                    'active', 'inactive', 'high', 'medium', 'low', 'urgent', 'normal',
                    'today', 'yesterday', 'tomorrow', 'this week', 'last week', 'next week',
                    'this month', 'last month', 'next month', 'this year', 'last year'
                ]
                
                if query.lower() in common_answers:
                    return None  # Common answer words are valid
                
                # 4. Date-like patterns
                if re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', query) or \
                   re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', query):
                    return None  # Date patterns are valid
    
    # Check for very vague single-word queries that need clarification
    vague_words = {
        'show': "What would you like me to show you? For example, you could ask: 'Show me all jobs for a specific customer', 'Show me pending jobs', or 'Show me jobs scheduled for this week'.",
        'get': "What information would you like me to get? For example: 'Get all jobs for customer Smith', 'Get completed jobs', or 'Get jobs with high priority'.",
        'list': "What would you like me to list? For example: 'List all customers', 'List pending jobs', or 'List jobs for today'.",
        'find': "What would you like me to find? For example: 'Find jobs for customer Jones', 'Find overdue jobs', or 'Find jobs with specific comments'.",
        'display': "What would you like me to display? For example: 'Display all active jobs', 'Display customer information', or 'Display job details for ticket 123'.",
        'search': "What would you like me to search for? For example: 'Search for jobs by customer name', 'Search for jobs with specific service types', or 'Search for jobs by date range'.",
        'give': "What information would you like me to give you? For example: 'Give me all jobs for this month', 'Give me customer details', or 'Give me job status updates'.",
        'tell': "What would you like me to tell you? For example: 'Tell me about pending jobs', 'Tell me job details for customer ABC', or 'Tell me about overdue tickets'.",
        'check': "What would you like me to check? For example: 'Check job status for ticket 456', 'Check customer information', or 'Check completed jobs for today'."
    }
    
    # Convert to lowercase for checking
    query_lower = query.lower().strip()
    
    # Check if the query is just a single vague word
    if query_lower in vague_words:
        return vague_words[query_lower]
    
    # Only check for pure gibberish patterns - be much more restrictive
    # Remove the blanket number/symbol check as numbers can be valid responses
    
    # Check for repeated characters (like "aaaaaaa") - only very obvious cases
    if re.match(r'^(.)\1{6,}$', query):  # Increased threshold from 5 to 6
        return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    # Remove the repetitive pattern check as it was too aggressive
    
    # Check for keyboard mashing patterns - only very extreme cases
    if len(query) > 10:  # Only check longer queries
        consonant_sequences = re.findall(r'[bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ]{7,}', query)  # Increased from 4 to 7
        if consonant_sequences and len(''.join(consonant_sequences)) > len(query) * 0.8:  # Increased threshold from 0.5 to 0.8
            return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    # Check for random character sequences (high entropy)
    # Remove common English words and check what remains
    common_words = {'the', 'and', 'or', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'job', 'jobs', 'customer', 'show', 'get', 'find', 'list', 'all', 'some', 'any', 'what', 'when', 'where', 'who', 'how', 'why', 'is', 'are', 'was', 'were', 'has', 'have', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'can', 'may', 'might', 'must', 'please', 'me', 'my', 'this', 'that', 'these', 'those'}
    
    # Split query into words and filter out common words
    words = re.findall(r'\b[a-zA-Z]+\b', query.lower())
    uncommon_words = [word for word in words if word not in common_words and len(word) > 2]
    
    # Check for very short queries with only common words (likely too vague)
    if len(words) <= 2 and all(word in common_words for word in words):
        # Examples: "show me", "get all", "find some"
        if any(vague in words for vague in ['show', 'get', 'find', 'list', 'display', 'search', 'give', 'tell', 'check']):
            return "I need more details about what you're looking for. Could you be more specific? For example: 'Show me jobs for customer Smith', 'Get all pending jobs', or 'Find jobs scheduled for today'."
    
    # If we have uncommon words, check if they look like gibberish
    if uncommon_words:
        gibberish_count = 0
        for word in uncommon_words:
            # Check for lack of vowels in longer words
            if len(word) > 4 and not re.search(r'[aeiou]', word):
                gibberish_count += 1
            # Check for unusual letter patterns
            elif re.search(r'[bcdfghjklmnpqrstvwxyz]{4,}|[xyz]{2,}|[qx][^u]|[cdfghjklmnpqrstvwxyz]{3}[cdfghjklmnpqrstvwxyz]', word):
                gibberish_count += 1
        
        # If most uncommon words appear to be gibberish
        if gibberish_count > len(uncommon_words) * 0.7:
            return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    # Check for queries that are just spaces or special characters
    if re.match(r'^[\s\W]+$', query):
        return "I couldn't understand your request. Could you please specify what information you are looking for regarding job details? For example, are you interested in a specific customer, job ID, service type, or perhaps jobs with certain comments?"
    
    # If query passed all checks, it's probably meaningful
    return None

def detect_environment_from_headers(headers):
    """
    Detect environment only from X-OpsFlo-Env header.
    Returns a tuple of (detected_env, source).
    """
    # Check only for X-OpsFlo-Env header
    headers_lower = {k.lower(): v for k, v in headers.items()}
    if 'x-opsflo-env' in headers_lower:
        env_value = headers_lower['x-opsflo-env'].upper()
        # Map to expected environment values
        env_mapping = {
            "LOCALHOST": "dev",  # Map to dev for consistent return values
            "QA": "qa",
            "PDNM": "qa",
            "HULK": "qa",
            "DEMO": "demo",
            "NEWDEMO": "newdemo",
            "PRID-QA": "prid-qa", # Added new environment
            "PRID-UAT": "prid-uat",  # Added new PRID-UAT environment
            "DEV": "dev",
            "UAT": "uat",
            "UNKNOWN": "qa"
        }
        mapped_env = env_mapping.get(env_value, "qa")
        return mapped_env, f"x-opsflo-env:{env_value}"
    
    # If no X-OpsFlo-Env header, return None
    return None, "No X-OpsFlo-Env header found"

# --- FastAPI App ---
app = FastAPI()
# --- Background insights store ---
INSIGHTS_STORE: Dict[str, Dict[str, Any]] = {}
INSIGHTS_TTL_SECONDS = 600
INSIGHTS_LOCK = threading.Lock()

def _cleanup_insights_store():
    now = time.time()
    with INSIGHTS_LOCK:
        to_delete = [rid for rid, v in INSIGHTS_STORE.items() if v.get("expires_at", 0) < now]
        for rid in to_delete:
            INSIGHTS_STORE.pop(rid, None)

def _generate_insights_task(request_id: str, shared_snapshot: Dict[str, Any]) -> None:
    try:
        # Ensure minimal required fields
        shared = {
            "show_charts": shared_snapshot.get("show_charts", False),
            "chart_data": shared_snapshot.get("chart_data"),
            "query_results": shared_snapshot.get("query_results", []),
            "query": shared_snapshot.get("query", ""),
            "sql_query": shared_snapshot.get("sql_query", ""),
            "db_name": shared_snapshot.get("db_name")
        }

        node = GenerateInsights()
        node.run(shared)

        with INSIGHTS_LOCK:
            INSIGHTS_STORE[request_id] = {
                "status": "ready",
                "insights": shared.get("insights"),
                "follow_up_questions": shared.get("follow_up_questions", []),
                "expires_at": time.time() + INSIGHTS_TTL_SECONDS
            }
    except Exception as e:
        logger.exception(f"Background insights generation failed: {e}")
        with INSIGHTS_LOCK:
            INSIGHTS_STORE[request_id] = {
                "status": "error",
                "error": "Failed to generate insights.",
                "expires_at": time.time() + INSIGHTS_TTL_SECONDS
            }

# Configure CORS
origins = [
    "http://localhost",
    "http://localhost:8000",
    "https://demo.ops-flo.com",
    "https://newdemo.ops-flo.com",  # Added newdemo
    "https://qa.ops-flo.com",
    "https://uat.ops-flo.com",
    "https://dev.ops-flo.com",
    "null" # To allow requests from local file system (for testing chat-widget.js)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"message": "AI Assistant API is running."}

@app.post("/api/ask", response_model=QueryResponse)
def ask_question(request: QueryRequest, req: Request, 
                 x_ops_env: str = Header(None, alias="X-OpsFlo-Env"),
                 background_tasks: BackgroundTasks = None):
    """
    Receives a natural language query, processes it through the AI flow,
    and returns either database results or a follow-up question.
    """
    try:
        # Check for explicit environment header first
        if x_ops_env:
            logger.info(f"Using explicit X-OpsFlo-Env header: {x_ops_env}")
            # Map environment values to database names
            env_to_db = {
                "LOCALHOST": "EV1_WEB_OPRS_DEMO_QA",
                "QA": "EV1_WEB_OPRS_DEMO_QA",
                "PDNM": "EV1_WEB_OPRS_DEMO_QA",
                "HULK": "EV1_WEB_OPRS_DEMO_QA",
                "DEMO": "EV1_WEB_OPRS_DEMO_PROD",  # Changed to connect to PROD database
                "NEWDEMO": "ETest_PRID",
                "PRID-QA": "ETest_PRID",  # Added new environment with different database
                "PRID-UAT": "EUAT_PRID", # Added new PRID-UAT environment with EUAT_PRID database
                "DEV": "EV1_WEB_OPRS_DEMO_DEV",
                "UAT": "EV1_WEB_OPRS_DEMO_UAT",
                "UNKNOWN": "EV1_WEB_OPRS_DEMO_QA"
            }
            
            # Get the database name from the mapping, default to QA if not found
            db_name = env_to_db.get(x_ops_env.upper(), "EV1_WEB_OPRS_DEMO_QA")
                
            # Log the selected database and proceed with processing
            logger.info(f"Selected database from X-OpsFlo-Env: {db_name}")
            
            # Initialize conversation history if not provided
            conversation_history = request.conversation_history or []
            
            # Check if the query matches any greeting pattern
            greeting_response = check_for_greeting(request.query)
            if greeting_response:
                # Add user query to conversation history
                if not (conversation_history and conversation_history[-1]["role"] == "user" and conversation_history[-1]["content"] == request.query):
                    conversation_history.append({
                        "role": "user",
                        "content": request.query
                    })
                
                # Add greeting response to conversation history
                conversation_history.append({
                    "role": "assistant",
                    "content": greeting_response
                })
                
                # Return the response without initializing the flow
                return QueryResponse(
                    question=greeting_response,
                    conversation_history=conversation_history
                )
            
            # Check if the query is meaningless and ask for clarification
            clarification_message = is_meaningless_query(request.query, conversation_history)
            if clarification_message:
                # Add user query to conversation history
                if not (conversation_history and conversation_history[-1]["role"] == "user" and conversation_history[-1]["content"] == request.query):
                    conversation_history.append({
                        "role": "user",
                        "content": request.query
                    })
                
                # Add clarification request to conversation history
                conversation_history.append({
                    "role": "assistant",
                    "content": clarification_message
                })
                
                # Return the clarification request without initializing the flow
                return QueryResponse(
                    question=clarification_message,
                    conversation_history=conversation_history
                )
            
            # Initialize the flow only if not a greeting
            flow = create_nl_to_sql_flow()
            shared = {
                "query": request.query,
                "db_name": db_name,
                "conversation_history": conversation_history,
                "show_charts": request.show_charts  # Pass the show_charts parameter to shared
            }
            
            # Run the flow (ends after SQL execution; chart_data prepared synchronously)
            flow.run(shared)
            
            # Extract the result or error from the shared store
            result = QueryResponse(
                conversation_history=shared.get("conversation_history", [])
            )
            
            if "error_message" in shared:
                result.error = shared["error_message"]
            elif "question" in shared:
                result.question = shared["question"]
            elif "follow_up_question" in shared:
                result.question = shared["follow_up_question"]
            elif "query_results" in shared:
                result.data = shared["query_results"]
                # Include chart data if available and charts are enabled
                if "chart_data" in shared and shared.get("show_charts", False):
                    chart_dict = shared["chart_data"]
                    if chart_dict:
                        # Clean labels to ensure no None values before creating ChartData
                        if "labels" in chart_dict and chart_dict["labels"]:
                            chart_dict["labels"] = [str(label) if label is not None else "Unknown" for label in chart_dict["labels"]]
                        
                        # Convert dictionary to ChartData object to avoid Pydantic serialization warning
                        result.chart = ChartData(**chart_dict)
                        
                        # Kick off background insights generation and return request_id
                        try:
                            request_id = str(uuid4())
                            result.request_id = request_id
                            snapshot = {
                                "show_charts": shared.get("show_charts", False),
                                "chart_data": shared.get("chart_data"),
                                "query_results": shared.get("query_results", []),
                                "query": shared.get("query", ""),
                                "sql_query": shared.get("sql_query", ""),
                                "db_name": shared.get("db_name")
                            }
                            with INSIGHTS_LOCK:
                                INSIGHTS_STORE[request_id] = {"status": "pending", "expires_at": time.time() + INSIGHTS_TTL_SECONDS}
                            if background_tasks is not None:
                                background_tasks.add_task(_generate_insights_task, request_id, snapshot)
                            else:
                                # Fallback: start a thread
                                threading.Thread(target=_generate_insights_task, args=(request_id, snapshot), daemon=True).start()
                        except Exception as e:
                            logger.exception(f"Failed to start insights background job: {e}")
            
            return result
        else:
            # If no X-OpsFlo-Env header was provided, this is an error
            error_message = "Missing X-OpsFlo-Env header. Please specify the environment."
            logger.warning(error_message)
            return QueryResponse(
                error=error_message,
                conversation_history=request.conversation_history or []
            )
    except Exception as e:
        logger.exception(f"Error processing query: {e}")
        return QueryResponse(
            error=f"An unexpected error occurred: {str(e)}",
            conversation_history=request.conversation_history or []
        )
@app.get("/api/insights/{request_id}")
def get_insights_status(request_id: str):
    _cleanup_insights_store()
    with INSIGHTS_LOCK:
        entry = INSIGHTS_STORE.get(request_id)
        if not entry:
            return {"status": "not_found"}
        if entry.get("status") == "ready":
            return {
                "status": "ready",
                "insights": entry.get("insights"),
                "follow_up_questions": entry.get("follow_up_questions", [])
            }
        if entry.get("status") == "error":
            return {"status": "error", "error": entry.get("error", "Unknown error")}
        return {"status": "pending"}

@app.post("/api/ask_condition", response_model=QueryResponse)
def ask_condition(request: QueryRequest, req: Request, x_ops_env: str = Header(None, alias="X-OpsFlo-Env")):
    """
    Receives a natural language query, processes it through the AI flow to generate SQL,
    and returns only the LLM-generated SQL query without executing it.
    """
    try:
        # Check for explicit environment header first
        if x_ops_env:
            logger.info(f"Using explicit X-OpsFlo-Env header: {x_ops_env}")
            # Map environment values to database names
            env_to_db = {
                "LOCALHOST": "EV1_WEB_OPRS_DEMO_QA",
                "QA": "EV1_WEB_OPRS_DEMO_QA",
                "PDNM": "EV1_WEB_OPRS_DEMO_QA",
                "HULK": "EV1_WEB_OPRS_DEMO_QA",
                "DEMO": "EV1_WEB_OPRS_DEMO_PROD",  # Changed to connect to PROD database
                "NEWDEMO": "ETest_PRID",  # Added new environment with different database
                "PRID-QA": "ETest_PRID", # Added new PRID-QA environment with ETest_PRID database
                "PRID-UAT": "EUAT_PRID", # Added new PRID-UAT environment with EUAT_PRID database
                "DEV": "EV1_WEB_OPRS_DEMO_DEV",
                "UAT": "EV1_WEB_OPRS_DEMO_UAT",
                "UNKNOWN": "EV1_WEB_OPRS_DEMO_QA"
            }
            
            # Get the database name from the mapping, default to QA if not found
            db_name = env_to_db.get(x_ops_env.upper(), "EV1_WEB_OPRS_DEMO_QA")
                
            # Log the selected database and proceed with processing
            logger.info(f"Selected database from X-OpsFlo-Env: {db_name}")
            
            # Initialize conversation history if not provided
            conversation_history = [] # request.conversation_history or []
            
            # Check if the query is meaningless and ask for clarification
            clarification_message = is_meaningless_query(request.query, conversation_history)
            if clarification_message:
                # Return the clarification request without processing
                return QueryResponse(
                    question=clarification_message,
                    conversation_history=conversation_history
                )
            
            # Create the flow for SQL generation only
            flow = create_sql_generation_flow()
            
            # Initialize the shared state
            shared = {
                "query": request.query,
                "db_name": db_name
            }
            
            if conversation_history:
                shared["conversation_history"] = conversation_history
                
            # Execute flow up to SQL generation only
            flow.run(shared)
            
            # Process the result and return response
            response = QueryResponse(conversation_history=shared.get("conversation_history", []))
            
            # Check if we have an AI response with SQL
            if "sql_query" in shared:
                # Return only the generated SQL query
                response.data = [{
                    "sql_query": shared["sql_query"]
                }]
                return response
            elif "question" in shared:
                # Return the question if more clarification is needed
                response.question = shared["question"]
                return response
                    
            if "error_message" in shared and shared["error_message"]:
                response.error = shared["error_message"]
                return response
                
            raise HTTPException(status_code=500, detail="Failed to generate SQL query.")
            
        # Use default database if X-OpsFlo-Env header is not present
        logger.info("No X-OpsFlo-Env header provided. Using default database.")
        db_name = "EV1_WEB_OPRS_DEMO_QA"  # Default database
            
        # Log the selected database
        logger.info(f"Selected database: {db_name}")

        # Initialize conversation history if not provided
        conversation_history = [] # request.conversation_history or []
        
        # Check if the query is meaningless and ask for clarification
        clarification_message = is_meaningless_query(request.query, conversation_history)
        if clarification_message:
            # Return the clarification request without processing
            return QueryResponse(
                question=clarification_message,
                conversation_history=conversation_history
            )
        
        # Create the flow for SQL generation only
        flow = create_sql_generation_flow()
        
        # Initialize the shared state with query and conversation history
        shared = {
            "query": request.query,
            "db_name": db_name
        }
        
        # Add conversation history if provided
        if conversation_history:
            shared["conversation_history"] = conversation_history
        
        # Execute the flow
        flow.run(shared)
        
        # Process the result
        response = QueryResponse(conversation_history=shared.get("conversation_history", []))
        
        # Check if we have an AI response with SQL
        if "sql_query" in shared:
            # Return only the generated SQL query
            response.data = [{
                "sql_query": shared["sql_query"]
            }]
            return response
        elif "question" in shared:
            # Return the question if more clarification is needed
            response.question = shared["question"]
            return response
        
        # Check if there was an error
        if "error_message" in shared and shared["error_message"]:
            response.error = shared["error_message"]
            return response
        
        # Fallback for unexpected cases
        raise HTTPException(status_code=500, detail="Failed to generate SQL query.")

    except Exception as e:
        # Catch any other exceptions
        logger.error(f"ask_condition endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 