import os
import pandas as pd
from dotenv import load_dotenv
from functools import lru_cache
from mssql_utils import get_db_engine, execute_query as mssql_execute_query
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def execute_query(query, db_name=None):
    """
    Execute an SQL query and return results as a Pandas DataFrame
    """
    try:
        df = mssql_execute_query(query, db_name=db_name)
        return df
    except Exception as e:
        raise e

def get_table_schema(db_name=None):
    """
    Get the schema information for the invoice report tables in the MSSQL database
    """
    table_schema_query = """
    SELECT 
        t.TABLE_NAME as table_name, 
        c.COLUMN_NAME as column_name, 
        c.DATA_TYPE as data_type, 
        c.CHARACTER_MAXIMUM_LENGTH as max_length,
        c.NUMERIC_PRECISION as numeric_precision,
        c.NUMERIC_SCALE as numeric_scale,
        c.IS_NULLABLE as is_nullable,
        c.COLUMN_DEFAULT as column_default,
        CASE 
            WHEN pk.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'PRIMARY KEY'
            WHEN fk.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'FOREIGN KEY'
            WHEN uq.CONSTRAINT_TYPE = 'UNIQUE' THEN 'UNIQUE'
            ELSE NULL
        END as constraint_type
    FROM 
        INFORMATION_SCHEMA.TABLES t
    JOIN 
        INFORMATION_SCHEMA.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME
    LEFT JOIN 
        (SELECT k.TABLE_NAME, k.COLUMN_NAME, tc.CONSTRAINT_TYPE
         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
         JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
             ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY') pk 
        ON c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
    LEFT JOIN 
        (SELECT k.TABLE_NAME, k.COLUMN_NAME, tc.CONSTRAINT_TYPE
         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
         JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
             ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY') fk 
        ON c.TABLE_NAME = fk.TABLE_NAME AND c.COLUMN_NAME = fk.COLUMN_NAME
    LEFT JOIN 
        (SELECT k.TABLE_NAME, k.COLUMN_NAME, tc.CONSTRAINT_TYPE
         FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
         JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
             ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         WHERE tc.CONSTRAINT_TYPE = 'UNIQUE') uq 
        ON c.TABLE_NAME = uq.TABLE_NAME AND c.COLUMN_NAME = uq.COLUMN_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
        AND t.TABLE_CATALOG = DB_NAME()
        AND t.TABLE_NAME IN (
            'FormDataPRIDEDispatchTicket',
            'FormDataPRIDEDispatchTicketChild',
            'FormDataPRIDEDispatchTicketTotal',
            'FormDataPRIDCableCompanyCustomer',
            'FormDataPridInvoice',
            'FormDataPridOperatorSetup',
            'FormDataPridMasterData',
            'FormDataPRIDEDispatchTicketComments',
            'FormDataPRIDFieldTicketItemMaculaInventoryStock',
            'FormDataPRIDFieldTicketItem',
            'FormDataPRIDServiceType'
        )
    ORDER BY 
        t.TABLE_NAME, 
        c.ORDINAL_POSITION;
    """
    
    try:
        df = mssql_execute_query(table_schema_query, db_name=db_name)
        return df
    except Exception as e:
        raise e

def get_schema_description(db_name=None):
    """
    Create a human-readable description of the database schema
    """
    schema_df = get_table_schema(db_name)
    
    # Group by table
    tables = {}
    for table_name in schema_df['table_name'].unique():
        table_columns = schema_df[schema_df['table_name'] == table_name]
        columns = []
        
        for _, row in table_columns.iterrows():
            # More detailed column description with precise data type information
            data_type_desc = row['data_type']
            if row['data_type'] in ['varchar', 'nvarchar', 'char', 'nchar'] and not pd.isna(row['max_length']):
                data_type_desc += f"({row['max_length'] if row['max_length'] != -1 else 'MAX'})"
            elif row['data_type'] in ['decimal', 'numeric'] and not pd.isna(row['numeric_precision']):
                data_type_desc += f"({row['numeric_precision']},{row['numeric_scale']})"
                
            column_desc = f"{row['column_name']} ({data_type_desc})"
            if row['constraint_type'] == 'PRIMARY KEY':
                column_desc += " PRIMARY KEY"
            elif row['constraint_type'] == 'FOREIGN KEY':
                column_desc += " FOREIGN KEY"
            elif row['constraint_type'] == 'UNIQUE':
                column_desc += " UNIQUE"
            
            if row['is_nullable'] == 'NO':
                column_desc += " NOT NULL"
            
            if not pd.isna(row['column_default']):
                column_desc += f" DEFAULT {row['column_default']}"
                
            columns.append(column_desc)
        
        tables[table_name] = columns
    
    # Generate description
    description = "Database Schema:\n\n"
    for table_name, columns in tables.items():
        description += f"Table: {table_name}\n"
        for column in columns:
            description += f"- {column}\n"
        description += "\n"
    
    return description

def get_column_data_samples(db_name=None):
    """
    Get sample data for each column to help with context
    """
    samples = {}
    
    tables = [
        'FormDataPRIDEDispatchTicket',
        'FormDataPRIDEDispatchTicketChild',
        'FormDataPRIDEDispatchTicketTotal',
        'FormDataPRIDCableCompanyCustomer',
        'FormDataPridInvoice',
        'FormDataPridOperatorSetup',
        'FormDataPridMasterData',
        'FormDataPRIDEDispatchTicketComments',
        'FormDataPRIDFieldTicketItemMaculaInventoryStock',
        'FormDataPRIDFieldTicketItem',
        'FormDataPRIDServiceType'
    ]
    
    for table in tables:
        try:
            sample_query = f"""
            SELECT TOP 3 * FROM {table}
            """
            df = mssql_execute_query(sample_query, db_name=db_name)
            
            if not df.empty:
                samples[table] = {}
                for column in df.columns:
                    # Get non-null sample values
                    non_null_values = df[column].dropna()
                    if len(non_null_values) > 0:
                        # For numerical columns, show min, max, avg
                        if pd.api.types.is_numeric_dtype(non_null_values):
                            samples[table][column] = {
                                "sample_values": non_null_values.head(3).tolist(),
                                "type": "numeric",
                                "min": non_null_values.min(),
                                "max": non_null_values.max(),
                                "avg": non_null_values.mean()
                            }
                        # For date columns
                        elif pd.api.types.is_datetime64_dtype(non_null_values):
                            samples[table][column] = {
                                "sample_values": [str(v) for v in non_null_values.head(3).tolist()],
                                "type": "date",
                                "min": str(non_null_values.min()),
                                "max": str(non_null_values.max())
                            }
                        # For string columns
                        else:
                            samples[table][column] = {
                                "sample_values": non_null_values.head(3).tolist(),
                                "type": "string",
                                "distinct_count": non_null_values.nunique()
                            }
                    else:
                        samples[table][column] = {"sample_values": [], "type": "unknown"}
        except Exception as e:
            # If there's an error, just continue to the next table
            samples[table] = {"error": str(e)}
    
    return samples

def get_column_data_samples_description(db_name=None):
    """
    Create a human-readable description of sample data
    """
    samples = get_column_data_samples(db_name)
    
    description = "Sample Data:\n\n"
    for table, columns in samples.items():
        description += f"Table: {table}\n"
        if "error" in columns:
            description += f"  - Could not retrieve samples: {columns['error']}\n"
            continue
            
        for column, data in columns.items():
            description += f"  - {column}: "
            if "sample_values" in data and data["sample_values"]:
                description += f"Examples: {', '.join(str(v) for v in data['sample_values'][:3])}"
                
                if data["type"] == "numeric":
                    description += f" (Range: {data['min']} to {data['max']})"
                elif data["type"] == "date":
                    description += f" (Range: {data['min']} to {data['max']})"
                elif data["type"] == "string" and data["distinct_count"] > 0:
                    description += f" ({data['distinct_count']} distinct values)"
            else:
                description += "No sample data available"
            description += "\n"
        description += "\n"
    
    return description

def get_foreign_key_relationships(db_name=None):
    """
    Get the foreign key relationships between the invoice report tables
    """
    foreign_key_query = """
    SELECT
        fk.TABLE_NAME as table_name, 
        fk.COLUMN_NAME as column_name, 
        pk.TABLE_NAME AS foreign_table_name,
        pk.COLUMN_NAME AS foreign_column_name,
        rc.DELETE_RULE as delete_rule,
        rc.UPDATE_RULE as update_rule
    FROM 
        INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
    WHERE
        fk.TABLE_CATALOG = DB_NAME() AND
        pk.TABLE_CATALOG = DB_NAME() AND
        (fk.TABLE_NAME IN (
            'FormDataPRIDEDispatchTicket',
            'FormDataPRIDEDispatchTicketChild',
            'FormDataPRIDEDispatchTicketTotal',
            'FormDataPRIDCableCompanyCustomer',
            'FormDataPridInvoice',
            'FormDataPridOperatorSetup',
            'FormDataPridMasterData',
            'FormDataPRIDEDispatchTicketComments',
            'FormDataPRIDFieldTicketItemMaculaInventoryStock',
            'FormDataPRIDFieldTicketItem',
            'FormDataPRIDServiceType'
        ) OR pk.TABLE_NAME IN (
            'FormDataPRIDEDispatchTicket',
            'FormDataPRIDEDispatchTicketChild',
            'FormDataPRIDEDispatchTicketTotal',
            'FormDataPRIDCableCompanyCustomer',
            'FormDataPridInvoice',
            'FormDataPridOperatorSetup',
            'FormDataPridMasterData',
            'FormDataPRIDEDispatchTicketComments',
            'FormDataPRIDFieldTicketItemMaculaInventoryStock',
            'FormDataPRIDFieldTicketItem',
            'FormDataPRIDServiceType'
        ))
    """
    
    try:
        df = mssql_execute_query(foreign_key_query, db_name=db_name)
        return df
    except Exception as e:
        raise e

def get_relationships_description(db_name=None):
    """
    Create a human-readable description of the relationships between tables
    """
    relationships_df = get_foreign_key_relationships(db_name)
    
    if relationships_df.empty:
        return "No foreign key relationships found between the invoice report tables."
    
    # Group relationships by parent-child tables for clearer organization
    table_relationships = {}
    for _, row in relationships_df.iterrows():
        parent = row['foreign_table_name']
        child = row['table_name']
        
        if parent not in table_relationships:
            table_relationships[parent] = {"children": []}
        
        table_relationships[parent]["children"].append({
            "table": child,
            "from_column": row['column_name'],
            "to_column": row['foreign_column_name'],
            "delete_rule": row['delete_rule'] if 'delete_rule' in row else "N/A",
            "update_rule": row['update_rule'] if 'update_rule' in row else "N/A"
        })
    
    description = "Table Relationships:\n\n"
    
    # Add parent-child relationships
    description += "Parent-Child Relationships:\n"
    for parent, data in table_relationships.items():
        description += f"- Table '{parent}' is referenced by:\n"
        for child in data["children"]:
            description += f"  * '{child['table']}' through {child['table']}.{child['from_column']} → {parent}.{child['to_column']}\n"
            if child['delete_rule'] != "N/A":
                description += f"    (On Delete: {child['delete_rule']}, On Update: {child['update_rule']})\n"
    
    # Add one-to-many relationships
    description += "\nOne-to-Many Relationships:\n"
    for _, row in relationships_df.iterrows():
        description += f"- One {row['foreign_table_name']} can have many {row['table_name']} records\n"
    
    return description

def get_table_descriptions(db_name=None):
    """
    Create descriptions of each table for better understanding of the invoice report data
    """
    descriptions = {
        "FormDataPRIDEDispatchTicket": "This table contains information about dispatch tickets, including work ticket numbers, service dates, cable company information, and other metadata for service dispatches. It serves as the main record for tracking service requests.",
        
        "FormDataPRIDEDispatchTicketChild": "This table contains child records for dispatch tickets, including information about individual shifts, service types, employees assigned, and ticket status. Each parent dispatch ticket can have multiple child records representing different parts of the service.",
        
        "FormDataPRIDEDispatchTicketTotal": "This table contains totals and summaries for dispatch tickets, including billable hours, mileage, inventory items, and job totals. It provides financial and operational summaries for reporting.",
        
        "FormDataPRIDCableCompanyCustomer": "This table contains information about cable company customers, including customer names, addresses, contact information, and billing preferences. These are the organizations that request services tracked in dispatch tickets.",
        
        "FormDataPridInvoice": "This table contains invoice information for services, including invoice numbers, dates, totals, and links to related dispatch tickets. It's used for billing and financial reporting.",
        
        "FormDataPridOperatorSetup": "This table contains information about operators, including names, classifications, and various requirement flags. Operators are the personnel who perform services on dispatch tickets.",
        
        "FormDataPridMasterData": "This table contains master data used throughout the system, organized by group codes and including text descriptions and values. It serves as a reference table for dropdown lists and standardized values.",
        
        "FormDataPRIDEDispatchTicketComments": "This table contains comments related to dispatch tickets, including user information, timestamps, and comment text. It provides a history of communication and notes about dispatch tickets.",
        
        "FormDataPRIDFieldTicketItemMaculaInventoryStock": "This table contains inventory stock information for field ticket items, including product IDs, quantities, and pricing. It tracks materials used during service calls.",
        
        "FormDataPRIDFieldTicketItem": "This table contains information about field ticket items, including descriptions, part numbers, and pricing information. These represent services or products provided during service calls.",
        
        "FormDataPRIDServiceType": "This table defines the various service types available in the system, including attributes like whether they require trucks or trailers, and billing information. It categorizes the kinds of services that can be performed."
    }
    
    return descriptions

def get_common_join_paths(db_name=None):
    """
    Provide common join paths for queries based on relationships
    """
    join_paths = [
        {
            "description": "Joining dispatch tickets with their child records",
            "tables": ["FormDataPRIDEDispatchTicket", "FormDataPRIDEDispatchTicketChild"],
            "join_condition": "FormDataPRIDEDispatchTicket.Id = FormDataPRIDEDispatchTicketChild.PRIDEDispatchTicketId",
            "use_case": "When you need to see details of services performed for each dispatch ticket"
        },
        {
            "description": "Joining dispatch tickets with their totals",
            "tables": ["FormDataPRIDEDispatchTicket", "FormDataPRIDEDispatchTicketTotal"],
            "join_condition": "FormDataPRIDEDispatchTicket.Id = FormDataPRIDEDispatchTicketTotal.PRIDEDispatchTicketId",
            "use_case": "When you need financial summaries for dispatch tickets"
        },
        {
            "description": "Joining dispatch tickets with customer information",
            "tables": ["FormDataPRIDEDispatchTicket", "FormDataPRIDCableCompanyCustomer"],
            "join_condition": "FormDataPRIDEDispatchTicket.CableCompanyCustomerId = FormDataPRIDCableCompanyCustomer.Id",
            "use_case": "When you need to see customer details for dispatch tickets"
        },
        {
            "description": "Joining dispatch tickets with invoices",
            "tables": ["FormDataPRIDEDispatchTicket", "FormDataPridInvoice"],
            "join_condition": "FormDataPRIDEDispatchTicket.Id = FormDataPridInvoice.PRIDEDispatchTicketId",
            "use_case": "When you need to see billing information for dispatch tickets"
        },
        {
            "description": "Joining dispatch tickets with comments",
            "tables": ["FormDataPRIDEDispatchTicket", "FormDataPRIDEDispatchTicketComments"],
            "join_condition": "FormDataPRIDEDispatchTicket.Id = FormDataPRIDEDispatchTicketComments.PRIDEDispatchTicketId",
            "use_case": "When you need to see notes and communication about dispatch tickets"
        },
        {
            "description": "Joining dispatch ticket children with service types",
            "tables": ["FormDataPRIDEDispatchTicketChild", "FormDataPRIDServiceType"],
            "join_condition": "FormDataPRIDEDispatchTicketChild.ServiceTypeId = FormDataPRIDServiceType.Id",
            "use_case": "When you need to see what specific services were performed"
        }
    ]
    
    description = "Common Join Paths:\n\n"
    for path in join_paths:
        description += f"- {path['description']}:\n"
        description += f"  * Tables: {' and '.join(path['tables'])}\n"
        description += f"  * Join: {path['join_condition']}\n"
        description += f"  * Use Case: {path['use_case']}\n\n"
    
    return description

def get_common_query_patterns(db_name=None):
    """
    Provide common query patterns for the invoice report system
    """
    if db_name == "ETest_PRID" or db_name == "EUAT_PRID":
        patterns = [
            {
                "name": f"Recent Jobs in {db_name} Database",
                "pattern": "SELECT TOP 10 * FROM GetJobDetails_FieldService ORDER BY ShiftDate DESC",
                "description": f"Gets the 10 most recent jobs using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs by Customer in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE CustomerName LIKE '%CustomerName%' ORDER BY ShiftDate DESC",
                "description": f"Finds all jobs for a specific customer using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs by Service Type in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE ServiceType = 'ServiceType' ORDER BY ShiftDate DESC",
                "description": f"Finds all jobs for a specific service type using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs with High Total Amount in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Total > 1000 ORDER BY Total DESC",
                "description": f"Finds jobs with totals greater than a specified amount using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs by Operator in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Operator = 'OperatorName' ORDER BY ShiftDate DESC",
                "description": f"Finds all jobs for a specific operator using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs for a Specific Well in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Well = 'WellName' ORDER BY ShiftDate DESC",
                "description": f"Finds all jobs for a specific well using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs with Comments in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE JobComments LIKE '%keyword%' ORDER BY ShiftDate DESC",
                "description": f"Finds jobs with specific keywords in comments using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Jobs with Specific Comment Text in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE JobComments LIKE '%install%adapter%' ORDER BY ShiftDate DESC",
                "description": f"Finds jobs with multiple specific terms in comments, showing how to search for partial matches with special characters in the {db_name} database"
            },
            {
                "name": f"Jobs by Specific Employee in {db_name} Database",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Employee = 'EmployeeName' ORDER BY ShiftDate DESC",
                "description": f"Finds all jobs for a specific employee using the GetJobDetails_FieldService view in the {db_name} database"
            },
            {
                "name": f"Customer Activity Summary in {db_name} Database",
                "pattern": "SELECT CustomerName, COUNT(JobId) AS JobCount, SUM(Total) AS TotalAmount, MAX(ShiftDate) AS LastJobDate FROM GetJobDetails_FieldService GROUP BY CustomerName ORDER BY TotalAmount DESC",
                "description": f"Summarizes customer activity with job counts and billing totals using the GetJobDetails_FieldService view in the {db_name} database"
            }
        ]
    else:
        patterns = [
            {
                "name": "Recent Dispatch Tickets",
                "pattern": "SELECT TOP 10 * FROM FormDataPRIDEDispatchTicket ORDER BY CreatedDate DESC",
                "description": "Gets the 10 most recent dispatch tickets by creation date"
            },
            {
                "name": "Dispatch Tickets by Date Range",
                "pattern": "SELECT * FROM FormDataPRIDEDispatchTicket WHERE ServiceDate BETWEEN @StartDate AND @EndDate",
                "description": "Filters dispatch tickets by a service date range"
            },
            {
                "name": "Invoices with High Totals",
                "pattern": "SELECT * FROM FormDataPridInvoice WHERE TotalAmount > @Amount ORDER BY TotalAmount DESC",
                "description": "Finds invoices with totals greater than a specified amount"
            },
            {
                "name": "Services by Type",
                "pattern": "SELECT dt.*, st.ServiceTypeName FROM FormDataPRIDEDispatchTicketChild dt JOIN FormDataPRIDServiceType st ON dt.ServiceTypeId = st.Id WHERE st.ServiceTypeName LIKE @ServiceType",
                "description": "Finds dispatch ticket children records by service type name"
            },
            {
                "name": "Customer Activity Summary",
                "pattern": "SELECT c.CustomerName, COUNT(dt.Id) AS TicketCount, SUM(i.TotalAmount) AS TotalBilled FROM FormDataPRIDCableCompanyCustomer c LEFT JOIN FormDataPRIDEDispatchTicket dt ON c.Id = dt.CableCompanyCustomerId LEFT JOIN FormDataPridInvoice i ON dt.Id = i.PRIDEDispatchTicketId GROUP BY c.CustomerName ORDER BY TotalBilled DESC",
                "description": "Summarizes customer activity with ticket counts and billing totals"
            },
            {
                "name": "Recent Jobs using GetJobDetails_FieldService View",
                "pattern": "SELECT TOP 10 * FROM GetJobDetails_FieldService ORDER BY ShiftDate DESC",
                "description": "Gets the 10 most recent jobs using the consolidated GetJobDetails_FieldService view"
            },
            {
                "name": "Jobs by Customer using GetJobDetails_FieldService View",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE CustomerName LIKE @CustomerName ORDER BY ShiftDate DESC",
                "description": "Finds all jobs for a specific customer using the GetJobDetails_FieldService view"
            },
            {
                "name": "Jobs by Operation Type using GetJobDetails_FieldService View",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Operation = @OperationType ORDER BY ShiftDate DESC",
                "description": "Finds all jobs for a specific operation type using the GetJobDetails_FieldService view"
            },
            {
                "name": "Jobs with High Total Amount using GetJobDetails_FieldService View",
                "pattern": "SELECT * FROM GetJobDetails_FieldService WHERE Total > @Amount ORDER BY Total DESC",
                "description": "Finds jobs with totals greater than a specified amount using the GetJobDetails_FieldService view"
            }
        ]
    
    description = "Common Query Patterns:\n\n"
    for pattern in patterns:
        description += f"- {pattern['name']}:\n"
        description += f"  ```sql\n  {pattern['pattern']}\n  ```\n"
        description += f"  {pattern['description']}\n\n"
    
    return description

@lru_cache(maxsize=32)
def get_invoice_report_description(db_name=None):
    """
    Get a comprehensive description of the invoice report database structure.
    This function is cached to prevent repeated processing for the same database.
    """
    schema_description = get_schema_description(db_name)
    relationships_description = get_relationships_description(db_name)
    table_descriptions = get_table_descriptions(db_name)
    column_samples_description = get_column_data_samples_description(db_name)
    join_paths = get_common_join_paths(db_name)
    query_patterns = get_common_query_patterns(db_name)
    
    # Combine all descriptions into a single comprehensive prompt
    full_description = "# Invoice Report Database Structure\n\n"
    
    # Add table descriptions
    full_description += "## Table Descriptions\n\n"
    for table, desc in table_descriptions.items():
        full_description += f"### {table}\n{desc}\n\n"
    
    # Add schema information
    full_description += "## Database Schema\n\n"
    full_description += schema_description
    
    # Add relationship information
    full_description += "## Table Relationships\n\n"
    full_description += relationships_description
    
    # Add join paths
    full_description += "## Common Join Paths\n\n"
    full_description += join_paths
    
    # Add query patterns
    full_description += "## Common Query Patterns\n\n"
    full_description += query_patterns
    
    # Add sample data
    full_description += "## Sample Data\n\n"
    full_description += column_samples_description
    
    return full_description

@lru_cache(maxsize=1)
def get_job_details_view_info(db_name=None):
    """
    Get information about the GetJobDetails_FieldService view including its schema and description.
    This function is cached to prevent repeated database calls.
    """
    # Get the column information for the view
    view_schema_query = """
    SELECT 
        c.COLUMN_NAME as column_name, 
        c.DATA_TYPE as data_type, 
        c.CHARACTER_MAXIMUM_LENGTH as max_length,
        c.NUMERIC_PRECISION as numeric_precision,
        c.NUMERIC_SCALE as numeric_scale,
        c.IS_NULLABLE as is_nullable
    FROM 
        INFORMATION_SCHEMA.COLUMNS c
    WHERE 
        c.TABLE_NAME = 'GetJobDetails_FieldService'
    ORDER BY 
        c.ORDINAL_POSITION;
    """
    
    try:
        view_schema_df = mssql_execute_query(view_schema_query, db_name=db_name)
        
        # Get sample data to understand the view better
        sample_query = "SELECT TOP 5 * FROM GetJobDetails_FieldService"
        sample_df = mssql_execute_query(sample_query, db_name=db_name)
        
        # --- Dynamically fetch distinct values for key categorical columns ---
        categorical_columns_to_fetch = [
            'ServiceType', 'JobStatus', 'OperationArea', 'State', 'County'
        ]
        distinct_values_map = {}
        for col in categorical_columns_to_fetch:
            try:
                # Only fetch if the column exists in the view
                if col in view_schema_df['column_name'].values:
                    # Query for distinct values, filtering out nulls and limiting results
                    distinct_query = f"SELECT DISTINCT TOP 50 {col} FROM GetJobDetails_FieldService WHERE {col} IS NOT NULL"
                    distinct_df = mssql_execute_query(distinct_query, db_name=db_name)
                    if not distinct_df.empty:
                        distinct_values_map[col] = distinct_df[col].tolist()
            except Exception as e:
                # Log or handle error if a specific distinct query fails
                print(f"Could not fetch distinct values for {col}: {e}")

        # Create a description of the view
        description = "GetJobDetails_FieldService View Schema:\n\n"
        
        for _, row in view_schema_df.iterrows():
            column_name = row['column_name']
            data_type_desc = row['data_type']
            if row['data_type'] in ['varchar', 'nvarchar', 'char', 'nchar'] and not pd.isna(row['max_length']):
                data_type_desc += f"({row['max_length'] if row['max_length'] != -1 else 'MAX'})"
            elif row['data_type'] in ['decimal', 'numeric'] and not pd.isna(row['numeric_precision']):
                data_type_desc += f"({row['numeric_precision']},{row['numeric_scale']})"
                
            description += f"- {column_name} ({data_type_desc})"
            if row['is_nullable'] == 'NO':
                description += " NOT NULL"
            
            # Append distinct values to the column description
            if column_name in distinct_values_map:
                example_values = ", ".join([f"'{v}'" for v in distinct_values_map[column_name][:10]])
                description += f" (e.g., {example_values})"
                if len(distinct_values_map[column_name]) > 10:
                    description += ", ..."

            description += "\n"
        
        # Add sample data description
        description += "\nSample Data from GetJobDetails_FieldService:\n\n"
        
        for column in sample_df.columns:
            values = sample_df[column].dropna().head(3).tolist()
            if values:
                description += f"- {column}: Examples: {', '.join(str(v) for v in values)}\n"
        
        # Use the dynamically fetched distinct values to build a more robust description
        view_description = """
GetJobDetails_FieldService View Description:
This view provides a consolidated, business-friendly representation of field service job details. 
It should be used as the primary source for all field service job-related queries.
"""
        # Append the list of distinct values to the main description for the AI to use
        if distinct_values_map:
            view_description += "\nKey Column Values:\n"
            for col, values in distinct_values_map.items():
                view_description += f"The '{col}' column contains values like: {', '.join([f'{v}' for v in values])}.\n"

        # Add a comprehensive description of what the view represents based on the database
        if db_name == "ETest_PRID" or db_name == "EUAT_PRID":
            # Add a more detailed description for the PRID databases
            view_description += f"""
This view in the {db_name} database provides a consolidated, business-friendly representation of field service job details, including:
- Customer and job location information
- Service, personnel, and ticket details
- Financial, status, and operational data
- Equipment, timing, and additional comments
"""
        
        description += view_description
        
        # Combine all information into a dictionary
        view_info = {
            "schema": description,
            "columns": view_schema_df['column_name'].tolist(),
            "sample_data": sample_df.to_dict(orient='records'),
            "distinct_values": distinct_values_map  # Pass this for more structured use later
        }
        
        return view_info
    except Exception as e:
        error_message = f"Error getting view info: {e}"
        return {
            "schema": "Error: Could not retrieve database schema information.",
            "columns": [],
            "sample_data": [],
            "distinct_values": {}
        }

@lru_cache(maxsize=1)
def get_cached_schema_info(db_name=None):
    """
    Get cached schema information for the specified database.
    """
    return get_job_details_view_info(db_name) 