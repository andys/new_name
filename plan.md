# Database Anonymizer in Golang

## Overview

This document outlines the plan for building a database anonymizer in Golang. The anonymizer will support both MySQL and PostgreSQL databases, allowing data to be read from a source database and written to a destination database. The tool will utilize worker pools for parallel processing of tables, and will implement "upsert" logic based on the presence of an ID field, or truncate the destination table if no ID is present.

## Requirements

- Golang installed on your system.
- Access to MySQL and PostgreSQL databases.
- Necessary permissions to read from the source and write to the destination databases.

## Architecture

1. **Database Connection**: Establish connections to both the source and destination databases. Use Golang's `database/sql` package along with the appropriate drivers for MySQL and PostgreSQL.

2. **Schema Discovery**: Retrieve the list of tables and their columns from the source database. This will involve querying the information schema for MySQL and PostgreSQL.

3. **Worker Pools**:
   - **Reader Workers**: A pool of workers dedicated to reading data from the source database. Each worker will handle a specific table.
   - **Writer Workers**: A pool of workers responsible for writing data to the destination database. Each worker will handle a specific table.

4. **Data Anonymization**: Implement logic to anonymize data as it is read from the source. This could involve masking, hashing, or replacing sensitive data fields.

5. **Data Transfer**:
   - **Upsert Logic**: If the table has an ID field, implement upsert logic to update existing records or insert new ones.
   - **Truncate and Insert**: If no ID field is present, truncate the destination table before inserting new data.

## Implementation Steps

1. **Setup Project**: Initialize a new Golang project and set up the necessary dependencies.

2. **Directory Structure**:
   - Organize the project into the following packages:
     ```
     /cmd
       /anonymizer
         main.go
      /db
         connection.go
         schema.go
         transfer.go
      /worker
         reader.go
         writer.go
      /anonymizer
         anonymize.go
      /config
         config.go
      /utils
         logger.go
     ```

3. **Database Connection** (`/db/connection.go`):
   - Implement functions to establish connections to MySQL and PostgreSQL using `github.com/go-sql-driver/mysql` and `github.com/lib/pq`.

4. **Schema Discovery** (`/db/schema.go`):
   - Implement functions to query the information schema for table and column details.
   - Read a config file in YAML format that defines what fields to anonymize, in format `tablename: field1,field2,field3,...`

5a. **Read Worker Pool Implementation**:
   - Use https://github.com/alitto/pond
   - Create a worker pool for reading data from the source database.
   - Keep track of progress in a struct, and have a foreground thread write periodic updates

5b. **Write Worker Pool Implementation**:
   - Create a worker pool for writing data to the destination database.

6. **Data Anonymization** (`/anonymizer/anonymize.go`):
   - Use https://github.com/brianvoe/gofakeit to develop anonymization functions for different data types.

7. **Data Transfer Logic** (`/db/transfer.go`):
   - **Upsert Logic**:
     - Implement upsert logic using SQL `INSERT ... ON DUPLICATE KEY UPDATE` for MySQL and `INSERT ... ON CONFLICT` for PostgreSQL.
   - **Truncate Logic**:
     - Implement truncate logic for tables without an ID field.

8. **Configuration Management** (`/config/config.go`):
   - Implement functions to read and parse the YAML configuration file.

9. **Utilities** (`/utils/logger.go`):
   - Implement logging utilities to facilitate debugging and monitoring.

10. **Testing**:
    - Write unit tests for each component in their respective packages.
    - Perform integration testing with both MySQL and PostgreSQL databases.

11. **Deployment**:
    - Package the application for deployment.
    - Provide documentation for configuration and usage.
