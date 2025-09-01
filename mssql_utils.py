import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()

def get_db_engine(db_name=None):
    """
    Create a SQLAlchemy engine for MS SQL Server.
    Supports environment-specific database connections.
    """
    # Determine which database connection to use based on db_name
    # Default connection parameters
    db_user = os.getenv("MSSQL_USER")
    db_password = os.getenv("MSSQL_PASSWORD")
    db_host = os.getenv("MSSQL_HOST")
    db_port = os.getenv("MSSQL_PORT")
    
    # If db_name is specified, try to find environment-specific connection parameters
    if db_name:
        # Map database names to environment prefixes
        db_to_env = {
            "EV1_WEB_OPRS_DEMO_DEV": "DEV",
            "EV1_WEB_OPRS_DEMO_UAT": "UAT",
            "EV1_WEB_OPRS_DEMO_QA": "QA",
            "EV1_WEB_OPRS_DEMO_PROD": "DEMO",
            "ETest_PRID": "NEWDEMO",
            "EUAT_PRID": "PRID-UAT"  # Correctly map EUAT_PRID to the PRID-UAT prefix
        }
        
        # Get the environment prefix for the database
        env_prefix = db_to_env.get(db_name)
        
        if env_prefix:
            # Try to get environment-specific connection parameters
            env_db_user = os.getenv(f"{env_prefix}_MSSQL_USER")
            env_db_password = os.getenv(f"{env_prefix}_MSSQL_PASSWORD")
            env_db_host = os.getenv(f"{env_prefix}_MSSQL_HOST")
            env_db_port = os.getenv(f"{env_prefix}_MSSQL_PORT")
            
            # Use environment-specific parameters if they exist
            db_user = env_db_user if env_db_user else db_user
            db_password = env_db_password if env_db_password else db_password
            db_host = env_db_host if env_db_host else db_host
            db_port = env_db_port if env_db_port else db_port
    
    # Use default db_name if none is provided
    if not db_name:
        db_name = os.getenv("MSSQL_DB")
    
    if not all([db_user, db_password, db_host, db_name]):
        raise ValueError("Please set MSSQL_USER, MSSQL_PASSWORD, MSSQL_HOST, and MSSQL_DB environment variables.")
        
    # URL-encode the password to handle special characters
    encoded_password = quote_plus(db_password)

    # Append port to host if it exists
    host_spec = f"{db_host}:{db_port}" if db_port else db_host
        
    database_url = f"mssql+pymssql://{db_user}:{encoded_password}@{host_spec}/{db_name}"
    
    engine = create_engine(database_url)
    return engine

def execute_query(query, db_name=None):
    """
    Execute an SQL query and return results as a Pandas DataFrame
    """
    engine = get_db_engine(db_name)
    with engine.connect() as connection:
        try:
            df = pd.read_sql_query(query, connection)
            return df
        except Exception as e:
            raise e

def get_table_schema(db_name=None):
    """
    Get the schema information for all tables in the database
    """
    table_schema_query = """
    SELECT 
        t.TABLE_NAME as table_name, 
        c.COLUMN_NAME as column_name, 
        c.DATA_TYPE as data_type, 
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
    ORDER BY 
        t.TABLE_NAME, 
        c.ORDINAL_POSITION;
    """
    
    engine = get_db_engine(db_name)
    with engine.connect() as connection:
        try:
            df = pd.read_sql_query(table_schema_query, connection)
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
            column_desc = f"{row['column_name']} ({row['data_type']})"
            if row['constraint_type'] == 'PRIMARY KEY':
                column_desc += " PRIMARY KEY"
            elif row['constraint_type'] == 'FOREIGN KEY':
                column_desc += " FOREIGN KEY"
            elif row['constraint_type'] == 'UNIQUE':
                column_desc += " UNIQUE"
            
            if row['is_nullable'] == 'NO':
                column_desc += " NOT NULL"
                
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

def get_foreign_key_relationships(db_name=None):
    """
    Get the foreign key relationships between tables
    """
    foreign_key_query = """
    SELECT
        fk.TABLE_NAME as table_name, 
        fk.COLUMN_NAME as column_name, 
        pk.TABLE_NAME AS foreign_table_name,
        pk.COLUMN_NAME AS foreign_column_name 
    FROM 
        INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
    JOIN 
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
    WHERE
        fk.TABLE_CATALOG = DB_NAME() AND
        pk.TABLE_CATALOG = DB_NAME()
    """
    
    engine = get_db_engine(db_name)
    with engine.connect() as connection:
        try:
            df = pd.read_sql_query(foreign_key_query, connection)
            return df
        except Exception as e:
            raise e

def get_relationships_description(db_name=None):
    """
    Create a human-readable description of the relationships between tables
    """
    relationships_df = get_foreign_key_relationships(db_name)
    
    if relationships_df.empty:
        return "No foreign key relationships found."
    
    description = "Table Relationships:\n\n"
    for _, row in relationships_df.iterrows():
        description += f"- {row['table_name']}.{row['column_name']} references {row['foreign_table_name']}.{row['foreign_column_name']}\n"
    
    return description

def list_tables(db_name=None):
    """
    List all tables in the database
    """
    query = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_CATALOG = DB_NAME()
    ORDER BY TABLE_NAME
    """
    
    engine = get_db_engine(db_name)
    with engine.connect() as connection:
        try:
            df = pd.read_sql_query(query, connection)
            return df
        except Exception as e:
            raise e 