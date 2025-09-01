from db_utils import (
    get_schema_description, 
    get_relationships_description, 
    execute_query,
    get_table_descriptions,
    get_column_data_samples_description,
    get_common_join_paths,
    get_common_query_patterns,
    get_job_details_view_info
)
from gemini_utils import nl_to_sql, call_llm
import json
import re
import logging
import time
logger = logging.getLogger(__name__)

def format_title_text(text):
    """
    Format column names and other text for display in chart titles.
    Converts camelCase, PascalCase, and snake_case to proper title case with spaces.
    
    Args:
        text (str): The text to format
        
    Returns:
        str: Formatted text with proper spacing and capitalization
    """
    if not text or not isinstance(text, str):
        return str(text) if text else ""
    
    # Handle camelCase and PascalCase by inserting spaces before capital letters
    # but not at the beginning of the string
    spaced_text = re.sub(r'(?<!^)(?=[A-Z])', ' ', text)
    
    # Handle snake_case by replacing underscores with spaces
    spaced_text = spaced_text.replace('_', ' ')
    
    # Handle multiple spaces or other separators
    spaced_text = re.sub(r'[_\-]+', ' ', spaced_text)
    
    # Clean up multiple spaces
    spaced_text = re.sub(r'\s+', ' ', spaced_text)
    
    # Convert to title case (first letter of each word capitalized)
    formatted_text = spaced_text.strip().title()
    
    return formatted_text

class Node:
    """Base Node class that defines the interface for all nodes"""
    
    def prep(self, shared):
        """Prepare data from shared store for execution"""
        return None
        
    def exec(self, prep_res):
        """Execute the node's core logic"""
        return None
    
    def post(self, shared, prep_res, exec_res):
        """Post-process the results and update shared store"""
        return "default"  # Default next action
    
    def run(self, shared):
        """Run the node's full cycle"""
        prep_res = self.prep(shared)
        exec_res = self.exec(prep_res)
        action = self.post(shared, prep_res, exec_res)
        return action or "default"


class GetUserQuery(Node):
    """Node to get the user's natural language query"""
    
    def post(self, shared, prep_res, exec_res):
        # The natural language query is already in the shared store
        # from the frontend
        
        # Initialize conversation history if it doesn't exist
        if "conversation_history" not in shared:
            shared["conversation_history"] = []
        
        # Ensure the current query is always added to the conversation history
        # so that it's available for the prompt generation in ConvertToSQL.
        current_query = shared.get("query")
        if current_query:
            # Avoid adding a duplicate of the last message, in case the client already added it.
            if not shared["conversation_history"] or shared["conversation_history"][-1].get("content") != current_query or shared["conversation_history"][-1].get("role") != "user":
                shared["conversation_history"].append({
                    "role": "user",
                    "content": current_query
                })
        
        return "default"


class GetDatabaseSchema(Node):
    """Node to fetch and prepare schema information for the GetJobDetails_FieldService view"""
    
    def prep(self, shared):
        # Pass the db_name to the exec method
        return shared.get("db_name")

    def exec(self, db_name):
        # Fetch information only for the GetJobDetails_FieldService view
        job_details_view_info = get_job_details_view_info(db_name=db_name)
        
        # If there was an error fetching view info, handle it
        if "error" in job_details_view_info:
            return {"error": job_details_view_info["error"]}

        # Structure the context to be used by downstream nodes
        if db_name == "ETest_PRID":
            view_description = "A consolidated view in ETest_PRID database that provides complete field service job information with columns like CustomerName, Lease, Well, ServiceType, Employee, ShiftNo, TicketNo, ShiftDate, JobId, Total, JobStatus, Operator, OperationArea, SalesPerson, TruckNo, TrailerNo, and JobComments. Use this view for all queries."
        else:
            view_description = "A consolidated view that joins multiple tables to provide complete field service job information. Use this view for all queries."
        
        return {
            "schema": job_details_view_info.get("schema", ""),
            "table_descriptions": {"GetJobDetails_FieldService": view_description},
            "relationships": "All necessary table relationships are pre-joined in the GetJobDetails_FieldService view.",
            "column_samples": "Sample data is included in the schema description.",
            "join_paths": "Not applicable; use the GetJobDetails_FieldService view directly.",
            "query_patterns": "Query the GetJobDetails_FieldService view directly using WHERE clauses."
        }
    
    def post(self, shared, prep_res, exec_res):
        if "error" in exec_res:
            shared["error_message"] = exec_res["error"]
            return "error"
            
        shared["schema_info"] = exec_res
        return "default"


class ConvertToSQL(Node):
    """Node to convert natural language to SQL using Gemini, focused on the GetJobDetails_FieldService view"""
    
    def prep(self, shared):
        # Prepare the context, which is now simplified to the GetJobDetails_FieldService view
        # and includes conversation history
        return {
            "query": shared["query"],
            "schema_info": shared["schema_info"],
            "conversation_history": shared["conversation_history"]
        }
    
    def exec(self, prep_res):
        # Generate SQL using the simplified context from the GetJobDetails_FieldService view
        schema_info = prep_res["schema_info"]
        conversation_history = prep_res["conversation_history"]
        
        # Format conversation history for the prompt
        conversation_text = ""
        for message in conversation_history:
            role = "User" if message["role"] == "user" else "Assistant"
            conversation_text += f"{role}: {message['content']}\n\n"
        
        # Create a prompt that instructs the model to either generate SQL or ask for clarification
        prompt = f"""You are a helpful assistant that provides business insights. Your primary goal is to answer questions based on the available data.

**Security Persona and Rules:**
1.  **Never reveal your identity:** Do not identify yourself as an AI, bot, or model.
2.  **Never mention technical details:** Do not mention SQL, databases, views, columns, or any other implementation details. Your responses should be purely business-focused.
3.  **Focus on business insights:** Frame your answers in terms of business metrics, trends, and summaries. For example, instead of saying "I will query the database," say "Let me look up the latest job figures for you."
4.  **Ask for clarification naturally:** If you need more information, ask in a conversational, non-technical way. For example, instead of "Which column should I filter on?", ask "Are you interested in a specific service type or region?"
5.  **Be concise and direct:** Provide the information the user asked for without unnecessary conversational filler.

**User Conversation History:**
{conversation_text}

Based on the conversation history and the user's latest query, you have two options:
1.  **Generate a response:** If you have enough information, provide a direct answer to the user's question.
2.  **Ask a clarifying question:** If the user's request is ambiguous or lacks necessary detail, ask a follow-up question to get the information you need.

IMPORTANT: Before generating SQL, carefully analyze if the user's query makes sense in the context of job database queries. You MUST ask for clarification if the query:
- Contains only vague action words like "show", "get", "list", "find", "display" without specifying WHAT to show/get/list
- Is meaningless gibberish or random characters  
- Is completely unrelated to job information, customers, service types, or database queries
- Is too vague or unclear to understand the intent (e.g., "show something", "get data")
- Is missing critical information needed to form a meaningful database query
- Contains only common words without specific details (e.g., "show me", "get all")

Examples of queries that REQUIRE clarification:
- "show" (What do you want me to show?)
- "get all" (Get all what? Jobs? Customers? Which specific information?)
- "list" (List what specifically?)
- "find something" (Find what? Be specific about what you're looking for)
- "display data" (What data should I display?)

Examples of queries that are CLEAR enough for SQL generation:
- "show me all jobs for customer Smith"
- "get pending jobs" 
- "list all customers"
- "find jobs scheduled for today"
- "display revenue data by month"

Guidelines for SQL generation:
1. ALWAYS query the GetJobDetails_FieldService view when generating SQL. Use "jd" as the alias for GetJobDetails_FieldService.
2. Use ONLY the exact column names provided in the schema. Do not invent column names or use variations (e.g., use 'StartDate', not 'serviceStartDate').
3. When filtering on categorical columns like 'ServiceType', 'JobStatus', or 'OperationArea', use the exact values provided in the 'Key Column Values' section below.
4. Use proper MSSQL-specific syntax (TOP instead of LIMIT, GETDATE() instead of NOW(), etc.).
5. When searching for text in comments or descriptions, use LIKE with wildcards ('%text%').
6. For date-related queries, use the 'StartDate' and 'EndDate' columns.
7.  **County vs. OperationArea**: When a user refers to a "county" (e.g., "dunn county", "county named dunn"), this maps directly to the `County` column. `OperationArea` refers to a broader business region. Do not confuse the two. The `County` column stores the name only (e.g., 'DUNN'), so format your query accordingly.

View Schema and Data Context:
{schema_info["schema"]}

You MUST respond in the following JSON format:
```json
{{
  "type": "sql",
  "content": "SELECT * FROM GetJobDetails_FieldService jd WHERE jd.CustomerName = 'Example'"
}}
```

OR

```json
{{
  "type": "question",
  "content": "Which customer are you interested in?"
}}
```
"""

        # Call the LLM to get the response with deterministic settings to avoid first-run variability
        response_text = call_llm(prompt, temperature=0.0)
        
        # Extract the JSON object from the response
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
        
        try:
            result = json.loads(json_text)
            # Validate that the result has the required keys
            if "type" not in result or "content" not in result:
                raise ValueError("Invalid response format")
            # Log the generated SQL if present
            if result["type"] == "sql":
                logger.info(f"Generated SQL Query: {result['content']}")
            return result
        except Exception as e:
            # If parsing fails, return a meaningful clarification request
            return {
                "type": "question",
                "content": f"I'm having trouble understanding your query. Could you please rephrase it? (Error: {str(e)})"
            }
    
    def post(self, shared, prep_res, exec_res):
        # Store the AI's response in the shared store
        shared["ai_response"] = exec_res
        
        # Add the AI's response to the conversation history
        if exec_res["type"] == "question":
            shared["conversation_history"].append({
                "role": "assistant",
                "content": exec_res["content"]
            })
        
        return "default"


class DecideNextAction(Node):
    """Node to decide the next action based on the AI's response"""
    
    def prep(self, shared):
        return shared["ai_response"]
    
    def exec(self, ai_response):
        # Simply pass through the AI response
        return ai_response
    
    def post(self, shared, prep_res, exec_res):
        if exec_res["type"] == "sql":
            # If the AI generated SQL, store it and proceed to execution
            shared["sql_query"] = exec_res["content"]
            return "execute_sql"
        else:
            # If the AI asked a question, return to the user
            shared["question"] = exec_res["content"]
            return "ask_question"


class ExecuteSQL(Node):
    """Node to execute the SQL query and return the results"""
    
    def prep(self, shared):
        # Get the SQL query and show_charts preference from the shared store
        return {
            "sql_query": shared.get("sql_query"),
            "db_name": shared.get("db_name"),
            "show_charts": shared.get("show_charts", False),
            "query": shared.get("query", "")  # Add original query for chart generation
        }
    
    def exec(self, prep_res):
        # Execute the SQL query against the database
        sql_query = prep_res["sql_query"]
        db_name = prep_res["db_name"]
        show_charts = prep_res["show_charts"]
        query_text = prep_res.get("query", "")  # Get original query for chart generation
        
        try:
            # Note: response is a pandas DataFrame
            response = execute_query(sql_query, db_name=db_name)
            
            # Format column names for better display in tables
            formatted_columns = {col: format_title_text(col) for col in response.columns}
            response_formatted = response.rename(columns=formatted_columns)
            
            # If successful execution, convert the DataFrame to a list of dictionaries with formatted column names
            records = response_formatted.to_dict(orient="records")
            
            # Generate chart data only if charts are enabled
            chart_data = None
            if show_charts:
                # Pass the original query for keyword detection and use original DataFrame for chart generation
                chart_data = self._generate_chart_data(response, query_text)
            
            return {"data": records, "chart": chart_data}
            
        except Exception as e:
            logger.error(f"SQL Execution Error: {e}\nQuery: {sql_query}")
            # Return a generic, user-friendly error message
            return {"error": "I encountered an issue while processing your request. Please try rephrasing your question."}
            
    def _select_value_columns_for_chart(self, df, numeric_cols, query_text=""):
        """
        Intelligently select which numeric columns should be used as VALUE SERIES vs LABELS/AXES.
        Uses a combination of pattern matching and semantic analysis.
        """
        if not numeric_cols:
            return []
        
        query_lower = query_text.lower()
        
        # Step 1: Hard rules for obvious exclusions (fast and reliable)
        definite_exclusions = []
        for col in numeric_cols:
            col_lower = col.lower().replace(' ', '').replace('_', '')
            # These are almost never chart values
            if any(term in col_lower for term in ['month', 'year', 'day', 'date', 'id', 'sequence', 'number']):
                # But allow if the query specifically asks for this type of data
                if not any(term in query_lower for term in ['month', 'year', 'day', 'date']):
                    definite_exclusions.append(col)
        
        # Step 2: Semantic matching based on query intent
        remaining_cols = [col for col in numeric_cols if col not in definite_exclusions]
        
        if not remaining_cols:
            # If we excluded everything, fall back to original list but be more selective
            remaining_cols = numeric_cols
        
        # Step 3: Score based on query relevance
        column_scores = {}
        
        # Extract key entities from query (what user wants to compare/measure)
        query_entities = self._extract_meaningful_entities(query_text)
        
        for col in remaining_cols:
            score = 0
            col_clean = col.lower().replace(' ', '').replace('_', '')
            
            # Positive scoring for value indicators
            value_terms = ['jobs', 'sales', 'revenue', 'count', 'total', 'amount', 'cost', 'profit', 'hours']
            for term in value_terms:
                if term in col_clean:
                    score += 30
            
            # Heavy boost for columns that match query entities
            for entity in query_entities:
                entity_clean = entity.lower().replace(' ', '').replace('_', '')
                if entity_clean in col_clean or col_clean in entity_clean:
                    score += 100
                # Partial match
                elif any(word in col_clean for word in entity_clean.split() if len(word) > 2):
                    score += 50
            
            column_scores[col] = score
        
        # Step 4: Select top scoring columns
        selected_columns = [col for col in remaining_cols if column_scores.get(col, 0) > 0]
        
        # Sort by score and limit results
        selected_columns.sort(key=lambda x: column_scores.get(x, 0), reverse=True)
        
        # For comparison queries, be more selective
        if self._detect_comparison_intent(query_text):
            return selected_columns[:4]  # Max 4 comparison series
        else:
            return selected_columns[:6]  # Max 6 series for non-comparison
    
    def _extract_meaningful_entities(self, query_text):
        """
        Extract meaningful entities from query that likely represent data columns.
        Uses pattern recognition optimized for database column names.
        """
        import re
        entities = []
        
        # Pattern 1: Quoted strings
        quoted = re.findall(r'"([^"]*)"', query_text)
        entities.extend(quoted)
        
        # Pattern 2: "A vs B" or "A and B" patterns
        vs_patterns = [
            r'(\w+(?:\s+\w+)*)\s+(?:vs|versus)\s+(\w+(?:\s+\w+)*)',
            r'comparing\s+(\w+(?:\s+\w+)*)\s+(?:vs|versus|and|with)\s+(\w+(?:\s+\w+)*)',
            r'(\w+(?:\s+\w+)*)\s+and\s+(\w+(?:\s+\w+)*)'
        ]
        
        for pattern in vs_patterns:
            matches = re.findall(pattern, query_text, re.IGNORECASE)
            for match in matches:
                entities.extend([m.strip() for m in match])
        
        # Pattern 3: Capitalized sequences (like "P3 New", "P3 Rerun")
        cap_entities = re.findall(r'\b[A-Z][A-Za-z0-9]*(?:\s+[A-Z][A-Za-z0-9]*)*\b', query_text)
        entities.extend(cap_entities)
        
        # Pattern 4: Technical terms that might be column names
        technical_terms = re.findall(r'\b(?:total|sum|count|number)\s+(?:of\s+)?(\w+(?:\s+\w+)*)', query_text.lower())
        entities.extend(technical_terms)
        
        # Clean and deduplicate
        clean_entities = []
        for entity in entities:
            entity = entity.strip()
            if len(entity) > 1 and entity.lower() not in ['the', 'and', 'or', 'by', 'of', 'in', 'for', 'with']:
                clean_entities.append(entity)
        
        return list(set(clean_entities))  # Remove duplicates

    def _select_primary_value_column(self, df, numeric_cols, query_text=""):
        """
        Intelligently select the primary value column based on user's query intent.
        Enhanced version that works with the new multi-column selection logic.
        """
        if not numeric_cols:
            return None
            
        if len(numeric_cols) == 1:
            return numeric_cols[0]
        
        # Use the new smart column selection and return the top choice
        smart_columns = self._select_value_columns_for_chart(df, numeric_cols, query_text)
        return smart_columns[0] if smart_columns else numeric_cols[0]
        """
        Intelligently select the primary value column based on user's query intent.
        Prioritizes columns that match the query keywords over just taking the first column.
        """
        if not numeric_cols:
            return None
            
        if len(numeric_cols) == 1:
            return numeric_cols[0]
        
        # Extract key terms from the user's query
        query_lower = query_text.lower()
        
        # Define priority keywords and their associated column patterns
        value_keywords = {
            'sales': ['sales', 'revenue', 'total_sales', 'totalsales', 'amount', 'value'],
            'revenue': ['revenue', 'income', 'earnings', 'total_revenue'],
            'count': ['count', 'number', 'total', 'quantity', 'qty'],
            'cost': ['cost', 'expense', 'price', 'amount'],
            'profit': ['profit', 'margin', 'gain'],
            'volume': ['volume', 'quantity', 'amount'],
            'hours': ['hours', 'time', 'duration'],
            'rate': ['rate', 'percentage', 'percent'],
            'job': ['jobs', 'job_count', 'jobcount', 'number_of_jobs'],
            'total': ['total', 'sum', 'aggregate', 'overall']
        }
        
        # First, try to match query keywords with column names
        for keyword, patterns in value_keywords.items():
            if keyword in query_lower:
                for col in numeric_cols:
                    col_lower = col.lower().replace(' ', '').replace('_', '')
                    for pattern in patterns:
                        if pattern.replace('_', '') in col_lower:
                            return col
        
        # If no keyword match, try semantic matching with column names
        # Prioritize columns with value-related terms
        priority_scores = {}
        for col in numeric_cols:
            col_lower = col.lower()
            score = 0
            
            # High priority terms that indicate values
            high_priority = ['total', 'sales', 'revenue', 'amount', 'value', 'sum', 'count']
            for term in high_priority:
                if term in col_lower:
                    score += 10
            
            # Medium priority terms
            medium_priority = ['price', 'cost', 'profit', 'volume', 'quantity']
            for term in medium_priority:
                if term in col_lower:
                    score += 5
            
            # Penalize time-related or ID columns
            low_priority = ['year', 'month', 'day', 'date', 'time', 'id', 'number', 'sequence']
            for term in low_priority:
                if term in col_lower:
                    score -= 5
            
            priority_scores[col] = score
        
        # Return the column with the highest score
        if priority_scores:
            best_col = max(priority_scores, key=priority_scores.get)
            if priority_scores[best_col] > 0:  # Only return if positive score
                return best_col
        
        # Fallback: return first non-time/ID column, or first column if none found
        non_time_id_cols = []
        for col in numeric_cols:
            col_lower = col.lower()
            if not any(term in col_lower for term in ['year', 'month', 'day', 'date', 'time', 'id', 'sequence']):
                non_time_id_cols.append(col)
        
        return non_time_id_cols[0] if non_time_id_cols else numeric_cols[0]

    def _generate_chart_data(self, df, query_text=""):
        """
        Enhanced chart generation with simplified core chart types for improved accuracy.
        
        The chart selection has been streamlined to focus on core, highly interpretable chart types:
        - Bar/Column charts for categorical comparisons
        - Line/Area charts for trends and time series
        - Pie/Doughnut charts for part-to-whole relationships (limited categories)
        - Scatter plots for bivariate analysis
        - Stacked Column charts for segmented data
        - KPI widgets for single metrics
        - Combo charts for mixed visualizations
        
        This reduces ambiguity in chart selection and improves AI model accuracy.
        """
        # If no data or too much data, don't create chart
        if df is None or df.empty or len(df) > 100:
            return None
            
        # Check if we have sufficient columns for visualization
        if len(df.columns) < 1:
            return None
        
        # Get numeric and non-numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        non_numeric_cols = df.select_dtypes(exclude=['number']).columns.tolist()
        
        # Check for keyword-based chart selection first (like Zoho Ask Zia)
        chart_type_result = self._detect_chart_type_from_keywords(query_text.lower())
        primary_chart_type, secondary_chart_type = chart_type_result if chart_type_result[0] else (None, None)
        
        # Check for comparison keywords in the query
        is_comparison_query = self._detect_comparison_intent(query_text.lower())
        
        # Check for single metric KPI
        if len(df) == 1 and len(numeric_cols) == 1:
            return self._create_kpi_widget(df, numeric_cols[0])
        
        # Geo charts are disabled in simplified configuration
        # geo_chart = self._detect_geo_chart(df, non_numeric_cols, query_text.lower())
        # if geo_chart:
        #     return geo_chart
        
        # Apply keyword-based chart selection if detected
        if primary_chart_type:
            # Try to create the primary chart type first
            primary_chart = self._create_chart_from_keyword(df, primary_chart_type, numeric_cols, non_numeric_cols, is_comparison_query, query_text)
            
            # If primary chart creation fails or the chart type is not suitable for the data
            if primary_chart is None and secondary_chart_type:
                logger.info(f"Primary chart type '{primary_chart_type}' not suitable for data, falling back to '{secondary_chart_type}'")
                return self._create_chart_from_keyword(df, secondary_chart_type, numeric_cols, non_numeric_cols, is_comparison_query, query_text)
            elif primary_chart:
                return primary_chart
            
        # If we have exactly 2 columns, one potentially being a category/label and the other a value
        if len(df.columns) == 2:
            # If we have one numeric and one non-numeric column, ideal for bar/pie/column chart
            if len(numeric_cols) == 1 and len(non_numeric_cols) == 1:
                # Use non-numeric column as labels
                label_col = non_numeric_cols[0]
                value_col = self._select_primary_value_column(df, numeric_cols, query_text)
                labels = [str(x) if x is not None else "Unknown" for x in df[label_col].tolist()]
                data = df[value_col].tolist()
                
                # Enhanced chart type selection
                chart_type = self._determine_optimal_chart_type(data, labels)
                
                # Override chart type if user specified one and it's for comparison
                if primary_chart_type and is_comparison_query:
                    chart_type = primary_chart_type
                    
                # Check if this looks like time series data (months, dates, etc.)
                if any(time_term in label_col.lower() for time_term in ['month', 'date', 'time', 'day', 'quarter']):
                    chart_type = "column"  # Use column chart for time-based data
                
                return {
                    "type": chart_type,
                    "title": f"{format_title_text(value_col)} by {format_title_text(label_col)}",
                    "labels": labels,
                    "datasets": [{
                        "label": format_title_text(value_col),
                        "data": data,
                        "backgroundColor": 'rgba(254, 99, 131, 0.8)',  # #fe6383 from config
                        "borderColor": 'rgba(254, 99, 131, 1)',
                        "borderWidth": 2
                    }],
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                }
            
            # If we have two numeric columns, check if one is time-related
            if len(numeric_cols) == 2:
                time_related_cols = [col for col in numeric_cols if any(time_term in col.lower() 
                                   for time_term in ['year', 'month', 'day', 'date', 'time', 'quarter'])]
                
                if time_related_cols:
                    # One column is time-related, treat the other as the value
                    time_col = time_related_cols[0]
                    remaining_cols = [col for col in numeric_cols if col != time_col]
                    value_col = self._select_primary_value_column(df, remaining_cols, query_text)
                    
                    # Create labels from time column and data from value column
                    labels = [str(x) if x is not None else "Unknown" for x in df[time_col].astype(str).tolist()]
                    data = df[value_col].tolist()
                    
                    return {
                        "type": "column",
                        "title": f"{format_title_text(value_col)} by {format_title_text(time_col)}",
                        "labels": labels,
                        "datasets": [{
                            "label": value_col,
                            "data": data,
                            "backgroundColor": 'rgba(202, 203, 206, 0.8)',  # #cacbce from config
                            "borderColor": 'rgba(202, 203, 206, 1)',
                            "borderWidth": 2
                        }],
                        "options": {
                            "responsive": True,
                            "maintainAspectRatio": False
                        }
                    }
                else:
                    # Both columns are actual values - create scatter plot
                    x_col = numeric_cols[0]
                    y_col = numeric_cols[1]
                    
                    # Create a scatter plot
                    return {
                        "type": "scatter",
                        "title": f"{format_title_text(y_col)} vs {format_title_text(x_col)}",
                        "labels": [str(x) if x is not None else "Unknown" for x in df.index.tolist()],  # Use index as labels for scatter
                        "datasets": [{
                            "label": f"{format_title_text(y_col)} vs {format_title_text(x_col)}",
                            "data": df[y_col].tolist(),
                            "backgroundColor": 'rgba(54, 162, 235, 0.5)',  # #36a2eb from config
                            "borderColor": 'rgba(54, 162, 235, 1)',
                            "pointRadius": 5,
                            "pointHoverRadius": 7,
                            "showLine": False
                        }],
                        "options": {
                            "responsive": True,
                            "maintainAspectRatio": False,
                            "scales": {
                                "x": {
                                    "title": {
                                        "display": True,
                                        "text": x_col
                                    },
                                    "type": "linear",
                                    "position": "bottom"
                                }
                            }
                        }
                    }
                
        # Check for time-series data (date column + numeric columns)
        date_cols = [col for col in df.columns if df[col].dtype in ['datetime64[ns]', 'object'] and 
                     any(date_term in col.lower() for date_term in ['date', 'time', 'year', 'month', 'day'])]
        
        if date_cols and len(df.select_dtypes(include=['number']).columns) >= 1:
            # Time-series data. If comparison intent and multiple numeric columns exist,
            # create a multi-series line chart; otherwise fall back to single series.
            date_col = date_cols[0]
            available_numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            labels = [str(d) if d is not None else "Unknown" for d in df[date_col].tolist()]

            # Try multi-series when appropriate
            filtered_cols = self._select_value_columns_for_chart(df, available_numeric_cols, query_text)
            if (is_comparison_query and len(filtered_cols) >= 2) or len(filtered_cols) >= 2:
                # Use config.js palette
                config_colors = [
                    'rgba(254, 99, 131, 0.8)',
                    'rgba(54, 162, 235, 0.8)',
                    'rgba(76, 192, 192, 0.8)',
                    'rgba(255, 159, 64, 0.8)',
                    'rgba(153, 102, 255, 0.8)'
                ]
                datasets = []
                for i, col in enumerate(filtered_cols[:5]):
                    color = config_colors[i % len(config_colors)]
                    datasets.append({
                        "label": format_title_text(col),
                        "data": df[col].tolist(),
                        "borderColor": color.replace('0.8', '1'),
                        "backgroundColor": color,
                        "tension": 0.1,
                        "fill": False
                    })
                return {
                    "type": "line",
                    "title": "Multi-series trend over time",
                    "labels": labels,
                    "datasets": datasets,
                    "options": {"responsive": True, "maintainAspectRatio": False}
                }

            # Single-series fallback
            numeric_col = self._select_primary_value_column(df, available_numeric_cols, query_text)
            data = df[numeric_col].tolist()
            chart_type = "line"
            if len(data) > 5 and sum(1 for i in range(1, len(data)) if data[i] >= data[i-1]) > 0.7 * len(data):
                chart_type = "area"
            return {
                "type": chart_type,
                "title": f"{format_title_text(numeric_col)} over time",
                "labels": labels,
                "datasets": [{
                    "label": format_title_text(numeric_col),
                    "data": data,
                    "fill": chart_type == "area",
                    "borderColor": 'rgba(255, 204, 85, 1)',
                    "backgroundColor": 'rgba(255, 204, 85, 0.2)',
                    "tension": 0.1
                }],
                "options": {"responsive": True, "maintainAspectRatio": False}
            }
            
        # For data with multiple numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_cols) >= 2:
            # Smart column detection for time-based queries
            time_related_cols = [col for col in df.columns if any(time_term in col.lower() 
                               for time_term in ['year', 'month', 'day', 'date', 'time', 'quarter'])]
            
            value_cols = [col for col in numeric_cols if not any(time_term in col.lower() 
                         for time_term in ['year', 'month', 'day', 'date', 'time', 'quarter'])]
            
            month_cols = [col for col in df.columns if 'month' in col.lower()]
            
            # Special handling for monthly revenue queries
            if month_cols and value_cols:
                month_col = month_cols[0]
                value_col = self._select_primary_value_column(df, value_cols, query_text)  # Use intelligent selection
                
                # Create meaningful labels for monthly data
                labels = []
                for _, row in df.iterrows():
                    year = row.get('JobYear', row.get('Year', ''))
                    month = row.get('JobMonth', row.get('Month', ''))
                    if year and month:
                        # Create "YYYY-MM" format labels
                        labels.append(f"{int(year)}-{int(month):02d}")
                    else:
                        # Fallback to string representation of month column
                        month_val = row[month_col]
                        labels.append(str(month_val) if month_val is not None else "Unknown")
                
                data = df[value_col].tolist()
                
                return {
                    "type": "column",  # Use column chart for monthly data
                    "title": f"{format_title_text(value_col)} by Month",
                    "labels": labels,
                    "datasets": [{
                        "label": format_title_text(value_col),
                        "data": data,
                        "backgroundColor": 'rgba(76, 192, 192, 0.7)',  # #4ac0c0 from config
                        "borderColor": 'rgba(76, 192, 192, 1)',
                        "borderWidth": 2
                    }],
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                }
            
            # If we have year column but it's constant (same year for all rows), treat it as single-year data
            if time_related_cols and value_cols:
                year_cols = [col for col in time_related_cols if 'year' in col.lower()]
                if year_cols:
                    year_col = year_cols[0]
                    # Check if all years are the same
                    unique_years = df[year_col].nunique()
                    if unique_years == 1:
                        # Single year data - find the best label column
                        potential_label_cols = [col for col in df.columns if col.lower() in 
                                           ['month', 'monthname', 'sales_month', 'salesmonth'] or 
                                           ('month' in col.lower() and col not in numeric_cols)]
                        
                        if potential_label_cols:
                            label_col = potential_label_cols[0]
                            value_col = self._select_primary_value_column(df, value_cols, query_text)
                            
                            labels = [str(x) if x is not None else "Unknown" for x in df[label_col].astype(str).tolist()]
                            data = df[value_col].tolist()
                            
                            return {
                                "type": "column",
                                "title": f"{format_title_text(value_col)} by {format_title_text(label_col)} ({df[year_col].iloc[0]})",
                                "labels": labels,
                                "datasets": [{
                                    "label": format_title_text(value_col),
                                    "data": data,
                                    "backgroundColor": 'rgba(255, 159, 64, 0.8)',  # #ff9f40 from config
                                    "borderColor": 'rgba(255, 159, 64, 1)',
                                    "borderWidth": 2
                                }],
                                "options": {
                                    "responsive": True,
                                    "maintainAspectRatio": False
                                }
                            }
            
            # If we have an index or id-like column that works as a label
            potential_label_cols = [col for col in df.columns if col.lower() in 
                               ['id', 'name', 'category', 'type', 'region', 'location']]
            
            label_col = potential_label_cols[0] if potential_label_cols else df.columns[0]
            labels = [str(x) if x is not None else "Unknown" for x in df[label_col].astype(str).tolist()]
            
            # Check for specific patterns in data to determine chart type
            
            # Check for cyclic data (could benefit from scatter plots)
            # Cyclic data often has first and last values that are similar
            # or columns with similar patterns repeating
            is_cyclic = False
            if len(numeric_cols) >= 3:
                # Check if any column has similar start/end values
                for col in numeric_cols[:3]:  # Check first 3 numeric columns
                    if abs(df[col].iloc[0] - df[col].iloc[-1]) < 0.2 * df[col].std():
                        is_cyclic = True
                        break
            
            # Check for comparative data (good for scatter or column charts)
            # Comparative data often has multiple metrics for each category
            is_comparative = len(numeric_cols) >= 3 and len(df) <= 10
            
            # Check for stacked data potential (parts of a whole across categories)
            # Use intelligent column selection to avoid including irrelevant metrics
            filtered_numeric_cols = self._select_value_columns_for_chart(df, numeric_cols, query_text)
            
            # If no columns selected by smart logic, fall back to basic filtering
            if not filtered_numeric_cols:
                filtered_numeric_cols = [col for col in numeric_cols if not any(time_term in col.lower() 
                                       for time_term in ['year', 'month', 'day', 'date', 'time', 'quarter'])]
            
            # If we only have one actual value column after filtering, create single-series chart
            if len(filtered_numeric_cols) == 1:
                value_col = self._select_primary_value_column(df, filtered_numeric_cols, query_text)
                data = df[value_col].tolist()
                
                # Create meaningful labels for time-based data
                labels = []
                if month_cols and 'year' in df.columns.str.lower().tolist():
                    # Create month-year labels
                    for _, row in df.iterrows():
                        year = None
                        month = None
                        for col in df.columns:
                            if 'year' in col.lower():
                                year = row[col]
                            elif 'month' in col.lower():
                                month = row[col]
                        if year and month:
                            labels.append(f"{int(year)}-{int(month):02d}")
                        else:
                            first_val = row.iloc[0]
                            labels.append(str(first_val) if first_val is not None else "Unknown")  # Fallback
                else:
                    # Use the first non-value column as labels
                    label_col = [col for col in df.columns if col != value_col][0] if len(df.columns) > 1 else df.columns[0]
                    labels = [str(x) if x is not None else "Unknown" for x in df[label_col].astype(str).tolist()]
                
                return {
                    "type": "column",
                    "title": f"{format_title_text(value_col)} by category",
                    "labels": labels,
                    "datasets": [{
                        "label": format_title_text(value_col),
                        "data": data,
                        "backgroundColor": 'rgba(66, 185, 130, 0.7)',  # #42b982 from config
                        "borderColor": 'rgba(66, 185, 130, 1)',
                        "borderWidth": 2
                    }],
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                }
            
            is_stackable = len(filtered_numeric_cols) >= 2 and len(df) >= 2
            
            # Enhanced logic for comparison queries
            # Check if we have a comparison query but insufficient data structure for multi-series
            if is_comparison_query and len(filtered_numeric_cols) < 2:
                # For comparison queries with single numeric column, we need to check if 
                # the data can be split for comparison (e.g., billable vs non-billable)
                
                # Check if column names or query text suggests categories that should be split
                comparison_keywords_in_query = ['billable.*non.?billable', 'vs', 'versus', 'comparing.*']
                import re
                has_comparison_pattern = any(re.search(pattern, query_text.lower()) for pattern in comparison_keywords_in_query)
                
                if has_comparison_pattern:
                    # Add a note to the chart title indicating limitation
                    chart_note = " (Note: For true comparison, SQL should return separate columns for each category)"
                    
                    # Create regular single-series chart with informative title
                    if len(filtered_numeric_cols) == 1 and len(non_numeric_cols) >= 1:
                        label_col = non_numeric_cols[0]
                        value_col = filtered_numeric_cols[0]
                        labels = [str(x) if x is not None else "Unknown" for x in df[label_col].tolist()]
                        data = df[value_col].tolist()
                        
                        chart_type = primary_chart_type or "column"
                        
                        return {
                            "type": chart_type,
                            "title": f"{format_title_text(value_col)} by {format_title_text(label_col)}{chart_note}",
                            "labels": labels,
                            "datasets": [{
                                "label": format_title_text(value_col),
                                "data": data,
                                "backgroundColor": 'rgba(66, 185, 130, 0.7)',  # #42b982 from config
                                "borderColor": 'rgba(66, 185, 130, 1)',
                                "borderWidth": 2
                            }],
                            "options": {
                                "responsive": True,
                                "maintainAspectRatio": False
                            }
                        }
            
            # If user requested comparison and we have multiple numeric columns, prefer multi-series
            if is_comparison_query and len(filtered_numeric_cols) >= 2:
                # For comparison queries, prefer side-by-side columns or grouped bars
                if primary_chart_type in ['bar', 'column'] or not primary_chart_type:
                    chart_type = "column"  # Use column for better comparison visualization
                elif primary_chart_type == 'stackedColumn':
                    chart_type = "stackedColumn"  # Keep stacked if explicitly requested
                else:
                    chart_type = primary_chart_type or "column"
            else:
                # Determine chart type based on data characteristics - using enabled types only
                chart_type = "bar"  # Default
                
                if len(filtered_numeric_cols) >= 3 and len(df) <= 10:
                    chart_type = "scatter"  # Use scatter for multi-dimensional data
                elif is_stackable and len(df) > 7:
                    chart_type = "column"  # Use column for better readability with many categories
                elif is_stackable:
                    chart_type = "stackedColumn"
            
            # Create a multi-series chart using only actual value columns
            datasets = []
            # Use colors from config.js palette
            config_colors = [
                'rgba(254, 99, 131, 0.7)',   # #fe6383
                'rgba(202, 203, 206, 0.7)',  # #cacbce
                'rgba(153, 102, 255, 0.7)',  # #9966ff
                'rgba(54, 162, 235, 0.7)',   # #36a2eb
                'rgba(255, 204, 85, 0.7)',   # #ffcc55
                'rgba(76, 192, 192, 0.7)',   # #4ac0c0
                'rgba(255, 159, 64, 0.7)',   # #ff9f40
                'rgba(66, 185, 130, 0.7)'    # #42b982
            ]
            
            # For comparison queries with 2 series, use highly contrasting colors from config
            if is_comparison_query and len(filtered_numeric_cols) == 2:
                config_colors = [
                    'rgba(254, 99, 131, 0.8)',   # Pink/Red for first series (#fe6383)
                    'rgba(54, 162, 235, 0.8)'    # Blue for second series (#36a2eb)
                ]
            elif is_comparison_query and len(filtered_numeric_cols) >= 3:
                # For multi-series comparison, use highly distinct colors
                config_colors = [
                    'rgba(254, 99, 131, 0.8)',   # Pink/Red (#fe6383)
                    'rgba(54, 162, 235, 0.8)',   # Blue (#36a2eb)
                    'rgba(76, 192, 192, 0.8)',   # Teal (#4ac0c0)
                    'rgba(255, 159, 64, 0.8)',   # Orange (#ff9f40)
                    'rgba(153, 102, 255, 0.8)',  # Purple (#9966ff)
                    'rgba(255, 204, 85, 0.8)',   # Yellow (#ffcc55)
                    'rgba(66, 185, 130, 0.8)',   # Green (#42b982)
                    'rgba(202, 203, 206, 0.8)'   # Gray (#cacbce)
                ]
            
            for i, col in enumerate(filtered_numeric_cols[:5]):  # Use filtered columns only
                color = config_colors[i % len(config_colors)]
                datasets.append({
                    "label": format_title_text(col),  # Use formatted column names
                    "data": df[col].tolist(),
                    "backgroundColor": color,
                    "borderColor": color.replace('0.7', '1').replace('0.8', '1'),
                    "borderWidth": 1
                })
            
            # Create better title for comparison queries
            if is_comparison_query and len(filtered_numeric_cols) >= 2:
                series_names = [format_title_text(col) for col in filtered_numeric_cols[:2]]
                title = f"{' vs '.join(series_names)} comparison"
            else:
                title = "Multi-series comparison"
            
            return {
                "type": chart_type,
                "title": title,
                "labels": labels,
                "datasets": datasets,
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False
                }
            }
        
        return None
    
    def _detect_chart_type_from_keywords(self, query_text):
        """
        Detect chart type from keywords in the query text, with fallbacks to core chart types
        Returns a tuple: (primary_chart_type, secondary_chart_type)
        Only includes chart types that are enabled in config.js
        """
        # Enhanced chart type keywords mapping with primary and secondary chart types
        # Only including charts that are enabled in config.js (marked as true)
        chart_keywords = {
            # Core supported charts (enabled in config.js)
            'pie': {
                'keywords': ['pie', 'pie chart', 'semi pie', 'half pie', 'semi pie chart', 'half pie chart'],
                'secondary': 'doughnut'
            },
            'column': {
                'keywords': ['column', 'column chart', 'vertical bar', 'vertical bars'],
                'secondary': 'bar'
            },
            'bar': {
                'keywords': ['bar', 'bar chart', 'horizontal bar', 'horizontal bars'],
                'secondary': 'column'
            },
            'line': {
                'keywords': ['line', 'line chart', 'trend line', 'trend chart'],
                'secondary': 'area'
            },
            'area': {
                'keywords': ['area', 'area chart', 'filled line'],
                'secondary': 'line'
            },
            'doughnut': {
                'keywords': ['ring', 'ring chart', 'semi ring', 'half ring', 'semi ring chart', 'half ring chart', 'doughnut', 'donut'],
                'secondary': 'pie'
            },
            'scatter': {
                'keywords': ['scatter', 'scatter plot', 'scatter chart', 'dot plot'],
                'secondary': 'line'
            },
            'stackedColumn': {
                'keywords': ['stacked column', 'stacked', 'stacked bar', 'layered'],
                'secondary': 'column'
            },
            # Advanced charts that are enabled in config.js
            'combo': {
                'keywords': ['combo chart', 'combination chart', 'mixed chart'],
                'secondary': 'column'
            },
            'kpi': {
                'keywords': ['kpi', 'metric', 'single value', 'key performance', 'indicator'],
                'secondary': 'column'
            }
            # Note: Disabled charts from config.js are not included:
            # - spline, bubble, stacked, stackedBar, radar, polar, funnel, pyramid, heatmap, boxplot, waterfall
            # - map_scatter, map_bubble, map_pie, map_bubble_pie, pivot, web
        }
        
        # Look for chart type keywords in the query
        for chart_type, chart_info in chart_keywords.items():
            for keyword in chart_info['keywords']:
                if keyword in query_text:
                    primary_type = chart_type
                    secondary_type = chart_info['secondary']
                    return (primary_type, secondary_type)
        
        return (None, None)
    
    def _detect_comparison_intent(self, query_text):
        """
        Detect if the user wants to compare multiple categories or series
        """
        comparison_keywords = [
            'compare', 'comparing', 'comparison', 'vs', 'versus', 'against',
            'difference', 'differences', 'between', 'and', 'contrast',
            'side by side', 'side-by-side', 'breakdown by', 'split by',
            'grouped by', 'segmented by', 'categorized by', 'separated by'
        ]
        
        # Check for explicit comparison keywords
        for keyword in comparison_keywords:
            if keyword in query_text:
                return True
        
        # Check for pattern like "A vs B", "A and B", "A versus B"
        if ' vs ' in query_text or ' versus ' in query_text:
            return True
            
        # Check for specific comparison patterns like "billable vs non-billable"
        import re
        billable_pattern = r'billable\s+(vs|versus|and)\s+non.?billable'
        if re.search(billable_pattern, query_text, re.IGNORECASE):
            return True
        
        # Check for other common comparison patterns
        common_comparisons = [
            r'active\s+(vs|versus|and)\s+inactive',
            r'completed\s+(vs|versus|and)\s+pending',
            r'new\s+(vs|versus|and)\s+(old|existing)',
            r'internal\s+(vs|versus|and)\s+external'
        ]
        
        for pattern in common_comparisons:
            if re.search(pattern, query_text, re.IGNORECASE):
                return True
            
        # Check for " and " pattern but avoid common non-comparison uses
        if ' and ' in query_text:
            # Look for patterns like "X and Y" where X and Y are likely categories
            # Match patterns like "P3 New and P3 Rerun", "Category A and Category B"
            and_pattern = r'\b[A-Z][A-Za-z0-9]*\s+[A-Z][A-Za-z0-9]*\s+and\s+[A-Z][A-Za-z0-9]*\s+[A-Z][A-Za-z0-9]*\b'
            if re.search(and_pattern, query_text.title()):
                return True
        
        # Check for multiple category mentions (like "P3 New" and "P3 Rerun")
        # Look for patterns with spaces and capital letters that suggest categories
        # Enhanced pattern to catch things like "P3 New", "P3 Rerun", "Category A", etc.
        category_pattern = r'\b[A-Z][A-Za-z0-9]*\s+[A-Z][A-Za-z0-9]*\b'
        categories_found = re.findall(category_pattern, query_text.title())
        
        # Filter out common non-category phrases
        filtered_categories = []
        exclude_phrases = ['Job Count', 'Data Table', 'Chart Type', 'By Month', 'Per Month', 'Each Month']
        
        for cat in categories_found:
            if cat not in exclude_phrases:
                filtered_categories.append(cat)
        
        if len(filtered_categories) >= 2:
            return True
        
        # Check for patterns like "X vs Y jobs", "X versus Y adapter"
        vs_pattern = r'\b[A-Za-z0-9]+\s+(vs|versus)\s+[A-Za-z0-9]+\b'
        if re.search(vs_pattern, query_text, re.IGNORECASE):
            return True
            
        return False
    
    def _create_kpi_widget(self, df, metric_col):
        """
        Create a KPI widget for single metric values
        """
        value = df[metric_col].iloc[0]
        
        return {
            "type": "kpi",
            "title": format_title_text(metric_col),
            "value": value,
            "format": "number" if isinstance(value, (int, float)) else "text",
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _detect_geo_chart(self, df, non_numeric_cols, query_text):
        """
        Detect if data contains geographical information for map charts
        """
        # Common geographical column indicators
        geo_indicators = ['state', 'country', 'region', 'city', 'location', 'area', 'territory', 'zone']
        
        geo_cols = []
        for col in non_numeric_cols:
            if any(indicator in col.lower() for indicator in geo_indicators):
                geo_cols.append(col)
        
        if not geo_cols:
            return None
        
        # Determine map chart type based on keywords or data characteristics
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if 'map bubble pie' in query_text and len(numeric_cols) >= 2:
            return self._create_map_bubble_pie_chart(df, geo_cols[0], numeric_cols)
        elif 'map pie' in query_text and len(numeric_cols) >= 2:
            return self._create_map_pie_chart(df, geo_cols[0], numeric_cols)
        elif 'map bubble' in query_text and len(numeric_cols) >= 1:
            return self._create_map_bubble_chart(df, geo_cols[0], numeric_cols[0])
        elif len(geo_cols) > 0:
            return self._create_map_scatter_chart(df, geo_cols[0])
        
        return None
    
    def _create_map_scatter_chart(self, df, geo_col):
        """Create a map scatter chart"""
        return {
            "type": "map_scatter",
            "title": f"Distribution by {format_title_text(geo_col)}",
            "labels": [str(x) if x is not None else "Unknown" for x in df[geo_col].tolist()],
            "datasets": [{
                "label": "Locations",
                "data": [1] * len(df),  # Equal size points
                "backgroundColor": '#1aaa55'
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_map_bubble_chart(self, df, geo_col, value_col):
        """Create a map bubble chart"""
        return {
            "type": "map_bubble",
            "title": f"{format_title_text(value_col)} by {format_title_text(geo_col)}",
            "labels": [str(x) if x is not None else "Unknown" for x in df[geo_col].tolist()],
            "datasets": [{
                "label": format_title_text(value_col),
                "data": df[value_col].tolist(),
                "backgroundColor": '#357cd2'
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_map_pie_chart(self, df, geo_col, numeric_cols):
        """Create a map pie chart"""
        return {
            "type": "map_pie",
            "title": f"Multi-metric comparison by {format_title_text(geo_col)}",
            "labels": [str(x) if x is not None else "Unknown" for x in df[geo_col].tolist()],
            "datasets": [{
                "label": format_title_text(col),
                "data": df[col].tolist()
            } for i, col in enumerate(numeric_cols[:5])],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_map_bubble_pie_chart(self, df, geo_col, numeric_cols):
        """Create a map bubble pie chart"""
        return {
            "type": "map_bubble_pie",
            "title": f"Proportional analysis by {format_title_text(geo_col)}",
            "labels": [str(x) if x is not None else "Unknown" for x in df[geo_col].tolist()],
            "datasets": [{
                "label": format_title_text(col),
                "data": df[col].tolist()
            } for i, col in enumerate(numeric_cols[:5])],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _determine_optimal_chart_type(self, data, labels):
        """
        Determine the optimal chart type based on data characteristics, similar to Zoho Ask Zia logic
        """
        # Check if data represents a sequential process (suitable for bar chart)
        if sorted(data, reverse=True) == data and len(set(data)) == len(data) and len(labels) <= 10:
            return "bar"  # Use bar chart for sequential/ordered data
        
        # For few categories with significant differences, pie chart is better
        if len(labels) <= 8:
            # Check if data has good distribution for pie chart
            max_val = max(data) if data else 0
            min_val = min(data) if data else 0
            if max_val > 0 and (max_val / (min_val + 1)) < 10:  # Not too skewed
                return "pie"
        
        # For many categories, column chart is better than bar for display
        if len(labels) > 10:
            return "column"
        
        # Default to bar chart
        return "bar"
    
    def _create_chart_from_keyword(self, df, chart_type, numeric_cols, non_numeric_cols, is_comparison=False, query_text=""):
        """
        Create chart based on detected keywords, using core chart types only
        """
        if len(numeric_cols) == 0:
            return None
        
        # Get labels and data
        if len(non_numeric_cols) > 0:
            labels = [str(x) if x is not None else "Unknown" for x in df[non_numeric_cols[0]].tolist()]
        else:
            labels = [str(x) if x is not None else "Unknown" for x in df.index.tolist()]
        
        # Use intelligent column selection for single-column charts
        primary_numeric_col = self._select_primary_value_column(df, numeric_cols, query_text)
        
        # Handle different chart types based on keywords
        # Only core supported chart types that are enabled in config.js
        if chart_type == 'stackedColumn':
            return self._create_stacked_chart(df, labels, numeric_cols, 'stackedColumn')
        elif chart_type == 'pie':
            return self._create_pie_chart(df, labels, primary_numeric_col)
        elif chart_type == 'column':
            return self._create_column_chart(df, labels, numeric_cols, is_comparison, query_text)
        elif chart_type == 'bar':
            return self._create_bar_chart(df, labels, numeric_cols, is_comparison, query_text)
        elif chart_type == 'line':
            return self._create_line_chart(df, labels, primary_numeric_col)
        elif chart_type == 'doughnut':
            return self._create_doughnut_chart(df, labels, primary_numeric_col)
        elif chart_type == 'scatter':
            return self._create_scatter_chart(df, labels, numeric_cols)
        elif chart_type == 'combo':
            return self._create_combo_chart(df, labels, numeric_cols)
        elif chart_type == 'area':
            return self._create_area_chart(df, labels, primary_numeric_col)
        elif chart_type == 'kpi':
            return self._create_kpi_widget(df, primary_numeric_col)
        
        # Default fallback for any other unsupported types
        else:
            return self._create_bar_chart(df, labels, numeric_cols, is_comparison, query_text)
    
    def _create_stacked_chart(self, df, labels, numeric_cols, chart_type, colors=None):
        """Create stacked column chart (simplified from stacked bar/column)"""
        # Use colors from config.js palette
        config_colors = [
            'rgba(254, 99, 131, 0.8)',   # #fe6383
            'rgba(202, 203, 206, 0.8)',  # #cacbce
            'rgba(153, 102, 255, 0.8)',  # #9966ff
            'rgba(54, 162, 235, 0.8)',   # #36a2eb
            'rgba(255, 204, 85, 0.8)',   # #ffcc55
            'rgba(76, 192, 192, 0.8)',   # #4ac0c0
            'rgba(255, 159, 64, 0.8)',   # #ff9f40
            'rgba(66, 185, 130, 0.8)'    # #42b982
        ]
        
        datasets = []
        for i, col in enumerate(numeric_cols[:5]):
            color = config_colors[i % len(config_colors)]
            datasets.append({
                "label": format_title_text(col),
                "data": df[col].tolist(),
                "backgroundColor": color,
                "borderColor": color.replace('0.8', '1'),
                "borderWidth": 1
            })
        
        return {
            "type": chart_type,
            "title": "Stacked comparison",
            "labels": labels,
            "datasets": datasets,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False,
                "scales": {
                    "x": {"stacked": True},
                    "y": {"stacked": True}
                }
            }
        }
    
    def _create_pie_chart(self, df, labels, numeric_col, colors=None):
        """Create pie chart"""
        return {
            "type": "pie",
            "title": f"{format_title_text(numeric_col)} distribution",
            "labels": labels,
            "datasets": [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "borderColor": '#ffffff',
                "borderWidth": 2
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_ring_chart(self, df, labels, numeric_col, colors=None):
        """Create ring (doughnut) chart"""
        return {
            "type": "doughnut",
            "title": f"{format_title_text(numeric_col)} distribution",
            "labels": labels,
            "datasets": [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "borderColor": '#ffffff',
                "borderWidth": 2
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_doughnut_chart(self, df, labels, numeric_col, colors=None):
        """Create doughnut chart (same as ring chart)"""
        return self._create_ring_chart(df, labels, numeric_col)
    
    def _create_scatter_chart(self, df, labels, numeric_cols, colors=None):
        """Create scatter chart"""
        if len(numeric_cols) < 1:
            return None
        
        # For single numeric column, use index as x-axis
        if len(numeric_cols) == 1:
            return {
                "type": "scatter",
                "title": f"{format_title_text(numeric_cols[0])} scatter plot",
                "labels": labels,
                "datasets": [{
                    "label": format_title_text(numeric_cols[0]),
                    "data": df[numeric_cols[0]].tolist(),
                    "pointRadius": 5
                }],
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False
                }
            }
        
        # For multiple columns, use first two as x and y
        x_col = numeric_cols[0]
        y_col = numeric_cols[1]
        
        scatter_data = []
        for i in range(len(df)):
            scatter_data.append({
                "x": df[x_col].iloc[i],
                "y": df[y_col].iloc[i]
            })
        
        return {
            "type": "scatter",
            "title": f"{format_title_text(y_col)} vs {format_title_text(x_col)}",
            "labels": labels,
            "datasets": [{
                "label": f"{format_title_text(y_col)} vs {format_title_text(x_col)}",
                "data": scatter_data,
                "pointRadius": 5
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_bar_chart(self, df, labels, numeric_cols, is_comparison=False, query_text=""):
        """Create basic bar chart - supports multi-series for comparisons"""
        # Use colors from config.js palette
        config_colors = [
            'rgba(254, 99, 131, 0.8)',   # #fe6383
            'rgba(202, 203, 206, 0.8)',  # #cacbce
            'rgba(153, 102, 255, 0.8)',  # #9966ff
            'rgba(54, 162, 235, 0.8)',   # #36a2eb
            'rgba(255, 204, 85, 0.8)',   # #ffcc55
            'rgba(76, 192, 192, 0.8)',   # #4ac0c0
            'rgba(255, 159, 64, 0.8)',   # #ff9f40
            'rgba(66, 185, 130, 0.8)'    # #42b982
        ]
        
        # If comparison and multiple numeric columns, create multi-series with smart selection
        if is_comparison and isinstance(numeric_cols, list) and len(numeric_cols) > 1:
            # Use intelligent column selection to avoid irrelevant metrics
            smart_cols = self._select_value_columns_for_chart(df, numeric_cols, query_text)
            if smart_cols:
                numeric_cols = smart_cols
                
            datasets = []
            
            for i, col in enumerate(numeric_cols[:5]):  # Limit to 5 series
                color = config_colors[i % len(config_colors)]
                datasets.append({
                    "label": format_title_text(col),
                    "data": df[col].tolist(),
                    "backgroundColor": color,
                    "borderColor": color.replace('0.8', '1'),
                    "borderWidth": 1
                })
            
            # Create better title for comparison
            if len(numeric_cols) == 2:
                title = f"{format_title_text(numeric_cols[0])} vs {format_title_text(numeric_cols[1])}"
            else:
                title = f"Comparison of {len(numeric_cols)} metrics"
        else:
            # Single series bar chart
            numeric_col = numeric_cols[0] if isinstance(numeric_cols, list) else numeric_cols
            datasets = [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "backgroundColor": config_colors[0],  # Use first config color
                "borderColor": config_colors[0].replace('0.8', '1'),
                "borderWidth": 2
            }]
            title = f"{format_title_text(numeric_col)} by category"
        
        return {
            "type": "bar",
            "title": title,
            "labels": labels,
            "datasets": datasets,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_bubble_chart(self, df, labels, numeric_cols, colors):
        """Create bubble chart"""
        if len(numeric_cols) < 2:
            return None
        
        # Use first two numeric columns for x and y, third for size if available
        x_col = numeric_cols[0]
        y_col = numeric_cols[1]
        size_col = numeric_cols[2] if len(numeric_cols) > 2 else y_col
        
        bubble_data = []
        for i, label in enumerate(labels):
            bubble_data.append({
                "x": df[x_col].iloc[i],
                "y": df[y_col].iloc[i],
                "r": max(5, df[size_col].iloc[i] * 0.1)  # Scale bubble size
            })
        
        return {
            "type": "bubble",
            "title": f"{format_title_text(y_col)} vs {format_title_text(x_col)}",
            "labels": labels,
            "datasets": [{
                "label": "Data points",
                "data": bubble_data,
                "backgroundColor": colors[0],
                "borderColor": '#ffffff',
                "borderWidth": 1
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_bubble_pie_chart(self, df, labels, numeric_cols, colors):
        """Create bubble pie chart (represented as grouped bubble chart)"""
        return self._create_bubble_chart(df, labels, numeric_cols, colors)
    
    def _create_combo_chart(self, df, labels, numeric_cols, colors=None):
        """Create combination chart (bar + line)"""
        if len(numeric_cols) < 2:
            return None
        
        # Use colors from config.js palette
        config_colors = [
            'rgba(254, 99, 131, 0.8)',   # #fe6383
            'rgba(76, 192, 192, 0.8)',   # #4ac0c0
        ]
        
        datasets = []
        # First series as bar
        datasets.append({
            "type": "bar",
            "label": format_title_text(numeric_cols[0]),
            "data": df[numeric_cols[0]].tolist(),
            "backgroundColor": config_colors[0],
            "borderColor": config_colors[0].replace('0.8', '1'),
            "borderWidth": 1
        })
        
        # Second series as line
        datasets.append({
            "type": "line",
            "label": format_title_text(numeric_cols[1]),
            "data": df[numeric_cols[1]].tolist(),
            "borderColor": config_colors[1].replace('0.8', '1'),
            "backgroundColor": config_colors[1],
            "borderWidth": 2,
            "fill": False
        })
        
        return {
            "type": "combo",
            "title": "Combination chart",
            "labels": labels,
            "datasets": datasets,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_area_chart(self, df, labels, numeric_col, colors=None):
        """Create area chart"""
        # Use config color
        config_color = 'rgba(153, 102, 255, 0.8)'  # #9966ff from config
        
        return {
            "type": "area",
            "title": f"{format_title_text(numeric_col)} trend",
            "labels": labels,
            "datasets": [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "backgroundColor": config_color,
                "borderColor": config_color.replace('0.8', '1'),
                "borderWidth": 2,
                "fill": True
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_funnel_chart(self, df, labels, numeric_col, colors):
        """Create funnel chart"""
        return {
            "type": "funnel",
            "title": f"{numeric_col} funnel",
            "labels": labels,
            "datasets": [{
                "label": numeric_col,
                "data": df[numeric_col].tolist(),
                "backgroundColor": colors[:len(labels)],
                "borderColor": '#ffffff',
                "borderWidth": 2
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_web_chart(self, df, labels, numeric_cols, colors):
        """Create web (radar) chart"""
        datasets = []
        for i, col in enumerate(numeric_cols[:3]):  # Limit to 3 series for readability
            datasets.append({
                "label": col,
                "data": df[col].tolist(),
                "backgroundColor": colors[i] + '40',  # Add transparency
                "borderColor": colors[i],
                "borderWidth": 2
            })
        
        return {
            "type": "radar",
            "title": "Web comparison",
            "labels": labels,
            "datasets": datasets,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_pivot_view(self, df, numeric_cols, non_numeric_cols):
        """Create pivot table view"""
        # Format column names for better display in pivot tables
        formatted_columns = {col: format_title_text(col) for col in df.columns}
        df_formatted = df.rename(columns=formatted_columns)
        
        # Also format the column lists
        formatted_numeric_cols = [format_title_text(col) for col in numeric_cols]
        formatted_categorical_cols = [format_title_text(col) for col in non_numeric_cols]
        
        return {
            "type": "pivot",
            "title": "Pivot analysis",
            "data": df_formatted.to_dict(orient="records"),
            "numeric_columns": formatted_numeric_cols,
            "categorical_columns": formatted_categorical_cols,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_column_chart(self, df, labels, numeric_cols, is_comparison=False, query_text=""):
        """Create column chart (vertical bars) - supports multi-series for comparisons"""
        # Use colors from config.js palette
        config_colors = [
            'rgba(254, 99, 131, 0.8)',   # #fe6383
            'rgba(202, 203, 206, 0.8)',  # #cacbce
            'rgba(153, 102, 255, 0.8)',  # #9966ff
            'rgba(54, 162, 235, 0.8)',   # #36a2eb
            'rgba(255, 204, 85, 0.8)',   # #ffcc55
            'rgba(76, 192, 192, 0.8)',   # #4ac0c0
            'rgba(255, 159, 64, 0.8)',   # #ff9f40
            'rgba(66, 185, 130, 0.8)'    # #42b982
        ]
        
        # If comparison and multiple numeric columns, create multi-series with smart selection
        if is_comparison and len(numeric_cols) > 1:
            # Use intelligent column selection to avoid irrelevant metrics
            smart_cols = self._select_value_columns_for_chart(df, numeric_cols, query_text)
            if smart_cols:
                numeric_cols = smart_cols
            
            datasets = []
            
            for i, col in enumerate(numeric_cols[:5]):  # Limit to 5 series
                color = config_colors[i % len(config_colors)]
                datasets.append({
                    "label": format_title_text(col),
                    "data": df[col].tolist(),
                    "backgroundColor": color,
                    "borderColor": color.replace('0.8', '1'),
                    "borderWidth": 1
                })
            
            # Create better title for comparison
            if len(numeric_cols) == 2:
                title = f"{format_title_text(numeric_cols[0])} vs {format_title_text(numeric_cols[1])}"
            else:
                title = f"Comparison of {len(numeric_cols)} metrics"
        else:
            # Single series column chart
            numeric_col = numeric_cols[0] if isinstance(numeric_cols, list) else numeric_cols
            datasets = [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "backgroundColor": config_colors[0],  # Use first config color
                "borderColor": config_colors[0].replace('0.8', '1'),
                "borderWidth": 2
            }]
            title = f"{format_title_text(numeric_col)} by categories"
        
        return {
            "type": "column",
            "title": title,
            "labels": labels,
            "datasets": datasets,
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def _create_line_chart(self, df, labels, numeric_col, colors=None):
        """Create line chart"""
        # Use first color from config.js palette
        config_color = 'rgba(76, 192, 192, 0.8)'  # #4ac0c0 from config
        
        return {
            "type": "line",
            "title": f"{format_title_text(numeric_col)} trend",
            "labels": labels,
            "datasets": [{
                "label": format_title_text(numeric_col),
                "data": df[numeric_col].tolist(),
                "borderColor": config_color.replace('0.8', '1'),
                "backgroundColor": config_color,
                "borderWidth": 2,
                "fill": False
            }],
            "options": {
                "responsive": True,
                "maintainAspectRatio": False
            }
        }
    
    def post(self, shared, prep_res, exec_res):
        # Check for error
        if "error" in exec_res:
            shared["error_message"] = exec_res["error"]
            return "error"
        
        # Store the results in the shared store
        if "data" in exec_res:
            shared["query_results"] = exec_res["data"]
        
        # Store chart data if available
        if "chart" in exec_res and exec_res["chart"] is not None:
            shared["chart_data"] = exec_res["chart"]
            
        return "default"


class GenerateInsights(Node):
    """Node to generate insights and follow-up questions for chart data"""
    
    def _sanitize_followups(self, followups, db_name):
        """
        Replace raw database column names in follow-up questions with
        user-friendly labels (title-cased with spaces) to avoid leakage.
        """
        try:
            if not followups:
                return followups
            view_info = get_job_details_view_info(db_name=db_name) or {}
            columns = view_info.get("columns", []) if isinstance(view_info, dict) else []
            if not columns:
                return followups
            # Sort by length to avoid partial replacements (e.g., Job vs JobId)
            columns_sorted = sorted(columns, key=len, reverse=True)
            import re as _re
            sanitized = []
            for q in followups:
                new_q = str(q)
                # Remove markdown/code styling backticks for cleaner UX
                if '`' in new_q:
                    new_q = new_q.replace('`', '')
                for col in columns_sorted:
                    label = format_title_text(col)
                    pattern = _re.compile(rf"\b{_re.escape(col)}\b", _re.IGNORECASE)
                    new_q = pattern.sub(label, new_q)
                # Within parentheses, drop single quotes around tokens (e.g., 'Texas' -> Texas)
                try:
                    def _strip_quotes_inside_parens(m):
                        inner = m.group(1)
                        inner = _re.sub(r"'([^']+)'", r"\1", inner)
                        return f"({inner})"
                    new_q = _re.sub(r"\(([^)]*)\)", _strip_quotes_inside_parens, new_q)
                except Exception:
                    pass
                # Collapse multiple spaces
                new_q = _re.sub(r"\s{2,}", " ", new_q).strip()
                sanitized.append(new_q)
            return sanitized
        except Exception:
            # Fail-safe: return original
            return followups
    
    def prep(self, shared):
        # Only generate insights if we have charts enabled and chart data
        if not shared.get("show_charts", False) or "chart_data" not in shared:
            return None
            
        return {
            "query": shared.get("query", ""),
            "sql_query": shared.get("sql_query", ""),
            # Use the full result set available for charts (capped earlier to <= 100 rows)
            "data_rows": shared.get("query_results", []) or [],
            "chart_type": shared.get("chart_data", {}).get("type", "chart") if shared.get("chart_data") else "chart",
            "db_name": shared.get("db_name"),
            "chart_data": shared.get("chart_data", {})
        }
    
    def exec(self, prep_res):
        # Skip if no prep data (no charts to analyze)
        if prep_res is None:
            return {"skip": True}
            
        try:
            from gemini_utils import init_model, call_llm
            import json
            import re
            import math
            from collections import Counter
            
            # Optional pandas usage kept local to avoid global dependency side effects
            try:
                import pandas as pd  # type: ignore
            except Exception:
                pd = None
            
            query = prep_res["query"]
            sql_query = prep_res.get("sql_query", "")
            data_rows = prep_res["data_rows"]
            chart_type = prep_res["chart_type"]
            db_name = prep_res.get("db_name")
            
            # Pull view info (cached in db_utils) to guide follow-up questions
            view_info = {}
            try:
                view_info = get_job_details_view_info(db_name=db_name) or {}
            except Exception:
                view_info = {}
            
            # Prepare analysis context focusing on actionable, name-specific insights (avoid simple min/max)
            data_summary = ""
            computed_context = ""
            chart = prep_res.get("chart_data", {}) if isinstance(prep_res, dict) else {}
            if chart and isinstance(chart, dict) and chart.get("labels") and chart.get("datasets"):
                labels = chart.get("labels", [])
                datasets = chart.get("datasets", [])
                try:
                    series_summaries = []
                    for ds in datasets:
                        name = ds.get("label", "Series")
                        values = ds.get("data", [])
                        total = sum(v for v in values if isinstance(v, (int, float))) or 0
                        pairs = [(labels[i], values[i]) for i in range(min(len(labels), len(values))) if isinstance(values[i], (int, float))]
                        pairs_sorted = sorted(pairs, key=lambda x: x[1], reverse=True)
                        top3 = [(p[0], p[1], round((p[1]/total)*100, 2) if total else 0) for p in pairs_sorted[:3]]
                        top3_str = "; ".join([f"{nm} ({val:,} | {pct}%)" for nm, val, pct in top3]) if top3 else ""
                        top_share = round(sum(p[1] for p in pairs_sorted[:3]) / total * 100, 2) if total and len(pairs_sorted) >= 1 else 0
                        series_summaries.append(f"- {name}: top contributors → {top3_str}. Top 3 cover ~{top_share}% of total ({total:,}).")
                    ratio_lines = []
                    if len(datasets) >= 2:
                        s1 = datasets[0]; s2 = datasets[1]
                        n1 = s1.get("label", "Series 1"); n2 = s2.get("label", "Series 2")
                        vals1 = s1.get("data", []); vals2 = s2.get("data", [])
                        ratios = []
                        for i in range(min(len(labels), len(vals1), len(vals2))):
                            a = vals1[i]; b = vals2[i]
                            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a > 0:
                                ratios.append((labels[i], b / a))
                        if ratios:
                            ratios_sorted = sorted(ratios, key=lambda x: x[1], reverse=True)
                            top_ratio = ratios_sorted[:3]
                            ratio_lines.append("- Highest ratios (" + f"{n2} per {n1}" + "): " + "; ".join([f"{nm} ({round(r,2)})" for nm, r in top_ratio]))
                            tot1 = sum(v for v in vals1 if isinstance(v, (int, float))) or 0
                            tot2 = sum(v for v in vals2 if isinstance(v, (int, float))) or 0
                            if tot1 > 0:
                                ratio_lines.append(f"- Overall {n2}/{n1}: {round(tot2/tot1, 2)}")
                    computed_context = "\n".join(["Series concentration:"] + series_summaries + (["Cross-series signals:"] + ratio_lines if ratio_lines else []))
                except Exception:
                    computed_context = ""
            elif data_rows:
                columns = list(data_rows[0].keys())
                num_rows = len(data_rows)
                data_summary = f"Rows: {num_rows}\nColumns: {', '.join(columns)}"
            
            chart_context = f" The data is visualized as a {chart_type} chart." if chart_type else ""
            
            # Include constrained view info to ground follow-up questions to the available view
            view_columns = []
            if isinstance(view_info, dict):
                view_columns = view_info.get("columns", [])
            distinct_values = view_info.get("distinct_values", {}) if isinstance(view_info, dict) else {}

            prompt = f"""You are a data analyst AI assistant. Based on the user's query and the resulting data, provide:

1. **Insights**: A concise, informative summary (2-3 sentences) highlighting key findings, trends, patterns, or notable observations from the data.{chart_context}

2. **Follow-up Questions**: Generate 2-3 relevant follow-up questions that would help the user explore the data further. These must be:
   - Directly related to the current dataset and the user's intent
   - Feasible using ONLY the `GetJobDetails_FieldService` view columns
   - Concrete (reference real column names and, when helpful, example values from the view)

**User Query**: {query}

**Underlying SQL (for context)**:
{sql_query}

**Data Summary**:
{data_summary}

**Computed Context (beyond the chart)**:
{computed_context}

**View Columns**: {', '.join(view_columns) if view_columns else 'unknown'}

**Example Values (subset)**:
{json.dumps({k: v[:5] for k, v in distinct_values.items()}) if distinct_values else '{}'}

**Guidelines**:
- Insights should be specific and actionable, not generic
- Follow-up questions must be executable against the `GetJobDetails_FieldService` view
- Focus on business value and actionable insights
- Keep insights concise but informative
- Make follow-up questions specific enough to be directly actionable

**Response Format**:
Return a JSON object with exactly this structure:
{{
    "insights": "Your concise insights here (2-3 sentences)",
    "follow_up_questions": [
        "Specific follow-up question 1",
        "Specific follow-up question 2", 
        "Specific follow-up question 3"
    ]
}}

**IMPORTANT**: Return ONLY the JSON object, no additional text."""

            # Call the LLM
            response_text = call_llm(prompt, temperature=0.1)
            
            # Extract JSON from response
            response_text = response_text.strip()
            
            # Try to find JSON in the response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                try:
                    result = json.loads(json_str)
                    
                    # Validate the response structure
                    if "insights" in result and "follow_up_questions" in result:
                        # Ensure follow_up_questions is a list
                        if isinstance(result["follow_up_questions"], list):
                            return result
                        else:
                            # Convert to list if it's not
                            result["follow_up_questions"] = [str(result["follow_up_questions"])]
                            return result
                            
                except json.JSONDecodeError:
                    pass
            
            # Fallback if JSON parsing fails
            return {
                "insights": "Analysis completed successfully. The data shows the requested information based on your query.",
                "follow_up_questions": [
                    "What time period would you like to analyze next?",
                    "Would you like to see this data broken down by different categories?",
                    "What additional metrics would be helpful to compare?"
                ]
            }
            
        except Exception as e:
            logger.exception(f"Error generating insights: {e}")
            # Return fallback insights
            return {
                "insights": "Data analysis completed. The results show information relevant to your query.",
                "follow_up_questions": [
                    "What time period would you like to explore?",
                    "Would you like to see different data groupings?",
                    "What additional analysis would be helpful?"
                ]
            }
    
    def post(self, shared, prep_res, exec_res):
        # Skip if no insights were generated
        if exec_res.get("skip", False):
            return "default"
            
        # Store insights and follow-up questions in shared store
        if "insights" in exec_res:
            shared["insights"] = exec_res["insights"]
        if "follow_up_questions" in exec_res:
            # Sanitize to avoid exposing raw column names
            db_name = shared.get("db_name")
            shared["follow_up_questions"] = self._sanitize_followups(exec_res["follow_up_questions"], db_name)
            
        return "default"


class HandleError(Node):
    """Node to handle errors in SQL execution"""
    
    def post(self, shared, prep_res, exec_res):
        # The error is already in shared["error_message"]
        return "complete" 