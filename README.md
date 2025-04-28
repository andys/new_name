# Database Anonymizer

Database Anonymizer is a tool for copying data from a source database to a destination database, while anonymizing sensitive fields. Rather than removing data, it replaces sensitive values (such as names, emails, etc.) with realistic fake data, preserving the structure and usability of the database for development, testing, or analytics.

## Features

- **Supports MySQL and PostgreSQL**: Seamlessly works with both database types.
- **Configurable Anonymization**: Specify which fields to anonymize per table using a simple config file.
- **Parallel Processing**: Utilizes worker pools for fast, concurrent reading and writing of tables.
- **Upsert or Truncate Logic**: If a table has an ID field, records are upserted; otherwise, the destination table is truncated before insert.
- **Progress Reporting**: Periodically prints progress updates to the console.
- **Debug and Verbose Modes**: Optional flags for detailed error and SQL output.

## Usage

```
db-anonymizer --source <SOURCE_DB_URL> --dest <DEST_DB_URL> [--config <CONFIG_FILE>] [--debug] [--verbose]
```

### CLI Options

- `--source`, `-s` (required): Source database URL.  
  Example: `mysql://user:pass@host:port/dbname` or `postgres://user:pass@host:port/dbname`
- `--dest`, `-d` (required): Destination database URL.  
  Example: `mysql://user:pass@host:port/dbname` or `postgres://user:pass@host:port/dbname`
- `--config`, `-c`: Path to the anonymization config file.  
  Default: `new_name.conf`
- `--debug`: Enable debug mode with verbose error output.
- `--verbose`, `-v`: Enable verbose SQL output.

You can also set the following environment variables as alternatives to CLI flags:
- `SOURCE_DB_URL`
- `DEST_DB_URL`

## Configuration File

The configuration file specifies which fields in which tables should be anonymized.  
Format (YAML-like, but comma-separated):

```
table_name: field1,field2
table2: field1,field2,field3
```

**Example (`new_name.conf` or `new_name.sample.conf`):**
```
users: name,email
orders: address,phone
```

This means the `name` and `email` fields in the `users` table, and the `address` and `phone` fields in the `orders` table, will be anonymized.

## How It Works

1. **Connects** to both source and destination databases.
2. **Discovers schema** from the source, ensuring all tables exist in the destination.
3. **Truncates** destination tables that lack an ID field.
4. **Reads** data from the source using a pool of worker goroutines.
5. **Anonymizes** specified fields using realistic fake data.
6. **Writes** data to the destination using upsert logic (if ID field exists) or as new rows.
7. **Reports progress** throughout the process.

## Benefits

- **Safe Data Sharing**: Share production-like data without exposing sensitive information.
- **Easy Integration**: Simple CLI and config file make it easy to use in CI/CD or developer workflows.
- **Performance**: Parallel processing ensures fast operation even on large databases.

## Example

```
db-anonymizer --source "mysql://user:pass@localhost:3306/prod" --dest "mysql://user:pass@localhost:3306/dev" --config new_name.conf --verbose
```

