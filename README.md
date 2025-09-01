# Query to SQL AI Assistant

An AI-powered application that converts natural language queries to SQL using Google's Gemini Generative AI API. This application allows users to query Microsoft SQL Server databases using plain English instead of writing SQL code, with a specific focus on audit report data.

## Features

- Convert natural language queries to SQL using Google Gemini API
- Execute generated SQL queries on a Microsoft SQL Server database
- Provide explanations of SQL queries in plain language
- Display data visualizations for query results
- REST API for integration with other applications
- Environment-based database routing (DEV, UAT, QA, etc.)
- Detailed logging including request referrer information
- Containerized deployment with Docker
- Conversation history to maintain context
- Automatic greeting detection and responses
- Display query results as structured JSON

## Audit Report Tables

The application focuses on the following MSSQL tables for audit reporting:

1. `FormDataPRIDEDispatchTicket` - Main dispatch ticket information
2. `FormDataPRIDEDispatchTicketChild` - Child records for dispatch tickets
3. `FormDataPRIDEDispatchTicketTotal` - Totals and summaries for dispatch tickets
4. `FormDataPRIDCableCompanyCustomer` - Cable company customer information
5. `FormDataPridInvoice` - Invoice information for services
6. `FormDataPridOperatorSetup` - Operator information and setup details
7. `FormDataPridMasterData` - Master data used throughout the system
8. `FormDataPRIDEDispatchTicketComments` - Comments related to dispatch tickets
9. `FormDataPRIDFieldTicketItemMaculaInventoryStock` - Inventory stock information
10. `FormDataPRIDFieldTicketItem` - Field ticket item information
11. `FormDataPRIDServiceType` - Service type definitions

## Architecture

The application is built using a modular flow-based architecture:

```
User Query → Database Schema → NL to SQL Conversion → SQL Validation → SQL Execution → Results + Visualization
```

### Components

- **API Backend**: FastAPI REST API server
- **AI Engine**: Google Gemini API for natural language processing
- **Database**: Microsoft SQL Server with audit report data
- **Flow Engine**: Custom implementation for orchestrating the NL-to-SQL pipeline
- **Visualization**: Syncfusion charts and graphs
- **Docker**: Containerization for easy deployment

## Setup

### Prerequisites

- Python 3.8+
- Google Gemini API key
- Microsoft SQL Server database with audit report tables
- SQL Server authentication credentials
- Syncfusion license key (for data visualizations)
- Docker (for containerized deployment)

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd querytoSQL
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the root directory with the following variables:
   ```
   # API Key
   GOOGLE_API_KEY=your_google_api_key_here

   # Default Database Connection
   MSSQL_USER=your_mssql_username
   MSSQL_PASSWORD=your_mssql_password
   MSSQL_HOST=your_mssql_host
   MSSQL_PORT=your_mssql_port
   MSSQL_DB=your_mssql_database
   
   # Environment-Specific Database Connections
   # DEV Environment
   DEV_MSSQL_HOST=dev_host
   DEV_MSSQL_USER=dev_user
   DEV_MSSQL_PASSWORD=dev_password
   DEV_MSSQL_PORT=1433
   
   # UAT Environment
   UAT_MSSQL_HOST=uat_host
   UAT_MSSQL_USER=uat_user
   UAT_MSSQL_PASSWORD=uat_password
   UAT_MSSQL_PORT=1433
   
   # DEMO Environment (EV1_WEB_OPRS_DEMO_PROD)
   DEMO_MSSQL_HOST=demo_host
   DEMO_MSSQL_USER=demo_user
   DEMO_MSSQL_PASSWORD=demo_password
   DEMO_MSSQL_PORT=1433
   
   # NEWDEMO Environment (ETest_PRID)
   NEWDEMO_MSSQL_HOST=newdemo_host
   NEWDEMO_MSSQL_USER=newdemo_user
   NEWDEMO_MSSQL_PASSWORD=newdemo_password
   NEWDEMO_MSSQL_PORT=1433
   ```

4. Configure the Syncfusion license key:
   
   Edit the `config.js` file in the root directory and replace the placeholder with your Syncfusion license key:
   ```javascript
   const SYNCFUSION_LICENSE_KEY = 'YOUR_SYNCFUSION_LICENSE_KEY_HERE';
   ```

### Docker Deployment

1. Build the Docker image:
   ```
   docker build -t ai-assistant .
   ```

2. Run the container:
   ```
   docker run -d -p 8000:8000 \
     -e GOOGLE_API_KEY=your_google_api_key_here \
     -e MSSQL_HOST=your_mssql_host \
     -e MSSQL_USER=your_mssql_username \
     -e MSSQL_PASSWORD=your_mssql_password \
     -e MSSQL_DB=your_mssql_database \
     -e MSSQL_PORT=1433 \
     -e DEMO_MSSQL_HOST=demo_host \
     -e DEMO_MSSQL_USER=demo_user \
     -e DEMO_MSSQL_PASSWORD=demo_password \
     -e DEMO_MSSQL_PORT=1433 \
     -e NEWDEMO_MSSQL_HOST=newdemo_host \
     -e NEWDEMO_MSSQL_USER=newdemo_user \
     -e NEWDEMO_MSSQL_PASSWORD=newdemo_password \
     -e NEWDEMO_MSSQL_PORT=1433 \
     --name my-assistant-container ai-assistant
   ```
   
   You can configure any of the environment-specific database connections by setting the corresponding environment variables.

3. View logs:
   ```
   docker logs -f my-assistant-container
   ```

## API Usage

### Endpoints

- `GET /`: Health check
- `POST /api/ask`: Submit a natural language query

### Request Format

```json
{
  "query": "Your natural language query here",
  "conversation_history": [
    {"role": "user", "content": "Previous user message"},
    {"role": "assistant", "content": "Previous assistant response"}
  ],
  "environment": "DEV"  // Optional environment parameter
}
```

### Environment Selection

The application supports environment-based routing to different databases using the `X-OpsFlo-Env` header:

```
X-OpsFlo-Env: DEV
```

Supported values:
- LOCALHOST
- QA
- PDNM
- HULK
- DEMO
- NEWDEMO
- DEV
- UAT
- UNKNOWN

Database selection:
- DEV → EV1_WEB_OPRS_DEMO_DEV
- UAT → EV1_WEB_OPRS_DEMO_UAT
- QA, LOCALHOST, PDNM, HULK, UNKNOWN → EV1_WEB_OPRS_DEMO_QA
- DEMO → EV1_WEB_OPRS_DEMO_PROD (connects to a different MSSQL server)
- NEWDEMO → ETest_PRID (connects to a different MSSQL server)

### Response Format

```json
{
  "data": [
    {
      "column1": "value1",
      "column2": "value2",
      ...
    }
  ],
  "question": "Follow-up question (if any)",
  "error": "Error message (if any)",
  "visualization": {
    "type": "bar", 
    "data": [...],
    "xField": "column1",
    "yField": "column2",
    ...
  },
  "conversation_history": [
    {"role": "user", "content": "User query"},
    {"role": "assistant", "content": "Assistant response"}
  ]
}
```

## Visualization Support

The application detects when a query is asking for a visualization (chart, graph, etc.) and automatically generates the appropriate chart type based on the query and data. Supported chart types include:

- **Core Charts** (optimized for accuracy):
  - Bar charts (vertical bars)
  - Column charts (horizontal bars)
  - Line charts
  - Area charts
  - Pie charts
  - Doughnut charts
  - Scatter plots
  - Stacked column charts

- **Advanced Charts**:
  - KPI widgets for single metrics
  - Combination charts (bar + line)

**Note**: The chart selection has been simplified to core types for improved accuracy and better chart selection. Complex chart types like bubble, radar, funnel, and map charts have been disabled to reduce ambiguity in chart selection.

To request a visualization, include visualization-related terms in your query, such as:

- "Show me a chart of monthly sales"
- "Create a bar chart showing job counts by service type"
- "Display a pie chart of job distribution by customer"
- "Visualize the trend of completed jobs over time"
- "Show a scatter plot of revenue vs. duration"
- "Create a stacked column chart comparing performance metrics"
- "Display a KPI for total revenue"

## Example Audit Report Queries

- "Show me all dispatch tickets created in the last 30 days"
- "List invoices with total amounts greater than $5000"
- "Find all service types that require trucks"
- "Show me the top 10 cable company customers by number of dispatch tickets"
- "List all dispatch tickets with comments mentioning 'delay'"
- "Show me dispatch tickets and their corresponding child records"
- "Calculate the average billable hours per service type"
- "Find dispatch tickets that don't have any associated invoice"
- "Create a bar chart showing job counts by service type"
- "Visualize the trend of job completions over the past 6 months"

## Project Structure

```
querytoSQL/
│
├── main.py              # FastAPI application and endpoints
├── nodes.py             # Node classes for the conversion pipeline
├── flow.py              # Flow orchestration
├── gemini_utils.py      # Gemini API integration
├── db_utils.py          # Database utilities for MSSQL integration
├── mssql_utils.py       # MSSQL connection and query execution
├── visualization_utils.py # Utilities for data visualization
├── visualizations.js    # Client-side chart rendering
├── config.js            # Frontend configuration (including Syncfusion license)
├── Dockerfile           # Docker configuration
├── chat-widget.js       # Frontend chat widget
├── chat-widget.css      # Styling for chat widget
├── docs/                # Documentation
│   ├── design.md        # Application design documentation
│   ├── using_getjobdetails_view.md        # GetJobDetails view documentation
│   └── conversational_ai_prd.md          # Product requirements documentation
├── requirements.txt     # Python dependencies
└── README.md            # Documentation
```

## Using the GetJobDetails View

The application uses the `GetJobDetails` view to simplify SQL generation. This view provides a consolidated, business-friendly representation of job details by joining multiple tables in the database system.

### Benefits

- **Simplified SQL Generation**: Reduces the complexity of generated SQL queries
- **Better Readability**: Column names are business-friendly and intuitive
- **Improved Maintainability**: Changes to the underlying schema only need to be updated in the view definition
- **Consistent Results**: Ensures all queries use the same join logic
- **Better Performance**: The database can optimize the view execution

For more details, see [Using the GetJobDetails View](docs/using_getjobdetails_view.md).

## Integration with ASP.NET Applications

To integrate with an ASP.NET application:

```csharp
// Add the X-OpsFlo-Env header to specify the environment
httpClient.DefaultRequestHeaders.Add("X-OpsFlo-Env", "DEV"); // or "UAT", "QA", etc.

// Make the API request
var response = await httpClient.PostAsync("/api/ask", new StringContent(
    JsonConvert.SerializeObject(new {
        query = "Your natural language query"
    }),
    Encoding.UTF8,
    "application/json"
));
``` 