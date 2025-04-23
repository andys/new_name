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
		Name:  "db-anonymizer",
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
		},
		Action: func(c *cli.Context) error {

			// Load configuration
			err := config.LoadConfig(&cfg, cfg.ConfigFile)
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Connect to source database
			sourceDB, err := db.Connect(cfg.SourceURL)
			if err != nil {
				return fmt.Errorf("failed to connect to source database: %w", err)
			}
			defer sourceDB.Close()

			// Connect to destination database
			destDB, err := db.Connect(cfg.DestinationURL)
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

			// Create writer with 10 workers
			writer := worker.NewWriter(destDB, 10)

			// Create reader with 10 workers
			reader := worker.NewReader(sourceDB, writer, 10)

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
					fmt.Printf("\rProgress: %d/%d tables processed (Current: %s, Rows: %d)                                  ",
						processed, progress.TotalTables, progress.CurrentTable, writerProgress.ProcessedRows.Load())
				}
			}()

			// Process tables
			err = reader.ProcessTables(schemas)
			if err != nil {
				return fmt.Errorf("failed to process tables: %w", err)
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
