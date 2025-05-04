# Database Anonymizer

new_names is a tool for copying data from a source database to a destination database, while anonymizing sensitive
personal data such as name fields.

Rather than removing data, it replaces sensitive values (such as names, emails, etc.) with realistic fake data, preserving the structure and usability of the database for development, testing, or analytics.

## Features

- **Supports MySQL and PostgreSQL**: Seamlessly works with both database types.
- **Configurable Anonymization**: Specify which fields to anonymize per table using a simple YAML config file.
- **Parallel Processing**: Utilizes worker pools for fast, concurrent reading and writing of tables.
- **Upsert or Truncate Logic**: If a table has an ID field, records are upserted; otherwise, the destination table is truncated before insert.
- **Progress Reporting**: Periodically prints progress updates to the console.
- **Debug and Verbose Modes**: Optional flags for detailed error and SQL output.

## Usage

```
new_names --source <SOURCE_DB_URL> --dest <DEST_DB_URL> [--config <CONFIG_FILE>] [--debug] [--verbose] [--workers <N>]
```

### CLI Options

- `--source`, `-s` (required): Source database URL.  
  Example: `mysql://user:pass@host:port/dbname` or `postgres://user:pass@host:port/dbname`
- `--dest`, `-d` (required): Destination database URL.  
  Example: `mysql://user:pass@host:port/dbname` or `postgres://user:pass@host:port/dbname`
- `--config`, `-c`: Path to the anonymization config file.  
  Default: `new_names.conf`
- `--debug`: Enable debug mode with verbose error output.
- `--verbose`, `-v`: Enable verbose SQL output.
- `--workers`, `-w`: Number of workers for reader/writer pools.  
  Default: `4`

You can also set the following environment variables as alternatives to CLI flags:
- `SOURCE_DB_URL`
- `DEST_DB_URL`

## Configuration File

The configuration file specifies which fields in which tables should be anonymized, as well as tables to skip and optional sampling percentages.  
Format: YAML.

**Example (`new_names.conf` or `new_names.sample.conf`):**
```yaml
anonymize:
  users: email, name, phone
  orders: address
skip:
  - logs
  - audit
sample:
  events: 0.1
```

- The `anonymize` section lists tables and the fields to anonymize (comma-separated).
- The `skip` section lists tables to exclude from processing.
- The optional `sample` section allows you to specify a sampling percentage (e.g., `0.1` for 10%) for specific tables.

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
- **Performance**: Parallel processing ensures fast operation even on large databases. The number of parallel workers can be controlled with the `--workers` option.

## Example

```
new_names --source "mysql://user:pass@localhost:3306/prod" --dest "mysql://user:pass@localhost:3306/dev" --config new_names.conf --verbose
```
