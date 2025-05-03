package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/andys/new_name/config"
	"github.com/andys/new_name/db"
	"github.com/andys/new_name/worker"
	"github.com/urfave/cli/v2"
)

func main() {
	var cfg config.Config

	app := &cli.App{
		Name:  "new_name",
		Usage: "Anonymize database content from source to destination",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config",
				Aliases:     []string{"c"},
				Usage:       "Config file path",
				Value:       "new_name.conf",
				Destination: &cfg.ConfigFile,
			},
			&cli.StringFlag{
				Name:        "source",
				Aliases:     []string{"s"},
				Usage:       "Source database URL (e.g., mysql://user:pass@host:port/dbname or postgres://user:pass@host:port/dbname)",
				Required:    true,
				EnvVars:     []string{"SOURCE_DB_URL"},
				Destination: &cfg.SourceURL,
			},
			&cli.StringFlag{
				Name:        "dest",
				Aliases:     []string{"d"},
				Usage:       "Destination database URL (e.g., mysql://user:pass@host:port/dbname or postgres://user:pass@host:port/dbname)",
				Required:    true,
				EnvVars:     []string{"DEST_DB_URL"},
				Destination: &cfg.DestinationURL,
			},
			&cli.BoolFlag{
				Name:        "debug",
				Aliases:     []string{},
				Usage:       "Enable debug mode with verbose error output",
				Value:       false,
				Destination: &cfg.Debug,
			},
			&cli.BoolFlag{
				Name:        "verbose",
				Aliases:     []string{"v"},
				Usage:       "Enable verbose SQL output",
				Value:       false,
				Destination: &cfg.Verbose,
			},
			&cli.IntFlag{
				Name:        "workers",
				Aliases:     []string{"w"},
				Usage:       "Number of workers for reader/writer pools (default 4)",
				Value:       4, // Default value
				Destination: &cfg.WorkerCount,
			},
		},
		Action: func(c *cli.Context) error {

			// Load configuration
			err := config.LoadConfig(&cfg, cfg.ConfigFile)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Connect to source database
			sourceDB, err := db.Connect(cfg.SourceURL, &cfg, cfg.WorkerCount)
			if err != nil {
				return fmt.Errorf("failed to connect to source database: %w", err)
			}
			defer sourceDB.Close()

			// Connect to destination database
			destDB, err := db.Connect(cfg.DestinationURL, &cfg, cfg.WorkerCount)
			if err != nil {
				return fmt.Errorf("failed to connect to destination database: %w", err)
			}
			defer destDB.Close()

			fmt.Printf("Successfully connected to source (%s) and destination (%s) databases\n",
				sourceDB.Type, destDB.Type)

			// Get schema from source database
			schemas, err := sourceDB.GetSchema()
			if err != nil {
				return fmt.Errorf("failed to get schema from source database: %w", err)
			}

			// Print summary of tables and columns
			totalColumns := 0
			for _, table := range schemas {
				totalColumns += len(table.Columns)
			}
			fmt.Printf("\nFound %d tables with %d total columns\n", len(schemas), totalColumns)

			// Get schema from destination database
			destSchemas, err := destDB.GetSchema()
			if err != nil {
				return fmt.Errorf("failed to get schema from destination database: %w", err)
			}

			// Create map of destination table names for quick lookup
			destTables := make(map[string]bool)
			for _, schema := range destSchemas {
				destTables[schema.Name] = true
			}

			// Check that all source tables exist in destination
			for _, sourceSchema := range schemas {
				if !destTables[sourceSchema.Name] {
					return fmt.Errorf("table '%s' exists in source but not in destination database", sourceSchema.Name)
				}
			}

			// Truncate destination tables that have no ID field
			for _, sourceSchema := range schemas {
				if !sourceSchema.HasID {
					fmt.Printf("Truncating destination table '%s' (no ID field)...\n", sourceSchema.Name)
					query := fmt.Sprintf("TRUNCATE TABLE %s", sourceSchema.Name)
					if _, err := destDB.GetDB().Exec(query); err != nil {
						return fmt.Errorf("failed to truncate destination table '%s': %w", sourceSchema.Name, err)
					}
				}
			}

			// Create writer with 10 workers
			writer := worker.NewWriter(destDB, cfg.WorkerCount, &cfg)

			// Create reader with 10 workers
			reader := worker.NewReader(sourceDB, writer, cfg.WorkerCount, &cfg)

			// Start a goroutine to periodically print progress
			go func() {
				ticker := time.NewTicker(300 * time.Millisecond)
				defer ticker.Stop()

				for range ticker.C {
					progress := reader.GetProgress()
					processed := progress.ProcessedTables.Load()
					if processed >= progress.TotalTables {
						return
					}
					writerProgress := writer.GetProgress()
					fmt.Printf("\rProgress: %d/%d tables processed (Current: %s, Rows: %d, Errors: %d)                                  ",
						processed, progress.TotalTables, progress.CurrentTable,
						writerProgress.ProcessedRows.Load(),
						writerProgress.ErrorCount.Load())
				}
			}()

			// Process tables
			err = reader.ProcessTables(schemas)
			if err != nil {
				return fmt.Errorf("failed to process tables: %w", err)
			}

			// Wait for all writer tasks to finish
			writer.StopAndWait()

			// Enable foreign key checks on the destination database
			if err := destDB.EnableForeignKeyChecks(); err != nil {
				return fmt.Errorf("failed to enable foreign key checks: %w", err)
			}

			// Add final success message with newline
			fmt.Printf("\nAll %d tables processed successfully!\n", len(schemas))

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
