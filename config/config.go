package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Config holds the anonymization configuration
type Config struct {
	SourceURL       string
	DestinationURL  string
	ConfigFile      string
	Debug           bool
	Verbose         bool // Add this line
	AnonymizeFields map[string][]string
}

// LoadConfig reads and parses the configuration file
func LoadConfig(cfg *Config, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}
	defer file.Close()

	// Initialize the map if it doesn't exist
	if cfg.AnonymizeFields == nil {
		cfg.AnonymizeFields = make(map[string][]string)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue // Skip empty lines and comments
		}

		// Split on first colon
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid config line format (expected 'table: fields'): %s", line)
		}

		tableName := strings.TrimSpace(parts[0])
		if tableName == "" {
			return fmt.Errorf("empty table name in config line: %s", line)
		}

		// Split fields on commas and trim whitespace
		fields := strings.Split(parts[1], ",")
		fieldList := make([]string, 0, len(fields))
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field != "" {
				fieldList = append(fieldList, field)
			}
		}

		cfg.AnonymizeFields[tableName] = fieldList
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	return nil
}
