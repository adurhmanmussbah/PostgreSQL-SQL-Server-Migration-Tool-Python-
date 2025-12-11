# PostgreSQL-SQL-Server-Migration-Tool-Python-
A lightweight, extensible, production-ready ETL migration tool that automatically extracts schema and data from PostgreSQL and loads it into Microsoft SQL Server.



The tool reads and processes:

Schemas
Tables
Columns & Data Types
Primary Keys
Indexes (Unique & Non-Unique)
Foreign Keys
Views (exported for manual conversion)
Functions / Stored Procedures (exported for manual conversion)
Triggers (exported for manual conversion)


-----------------------------------------

All connection strings & settings live in an external file:
config.json

-------------------------------------------

All operations are logged to:
logs/migration.log

Including:
Connection status
Table creation
Row copy progress
Index creation
Foreign key creation

----------------------------------------------------

requirements:
pip install psycopg2-binary pyodbc 

--------------------------------------------------


ETL Flow
        ┌─────────────────┐
        │   PostgreSQL    │
        └───────┬────────┘
                │ Extract
                ▼
         Schema Metadata
     (tables, columns, pk, fk,
      views, triggers, indexes)
                │ Transform
                ▼
        Type Mapping Layer
                │ Load
                ▼
        ┌─────────────────┐
        │   SQL Server    │
        └─────────────────┘


        







