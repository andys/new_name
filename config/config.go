package config

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds the anonymization configuration
type Config struct {
	SourceURL       string
	DestinationURL  string
	ConfigFile      string
	Debug           bool
	Verbose         bool                // Add this line
	WorkerCount     int                 // Number of workers for reader/writer pools
	AnonymizeFields map[string][]string `yaml:"-"`
	SkipTables      []string            // List of tables to skip
	SampleTables    map[string]float64  // Table name to sample percentage
}

type yamlConfig struct {
	Anonymize map[string]string  `yaml:"anonymize"`
	Skip      []string           `yaml:"skip"`
	Sample    map[string]float64 `yaml:"sample"`
}

// LoadConfig reads and parses the configuration file
func LoadConfig(cfg *Config, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}
	defer file.Close()

	var ycfg yamlConfig
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&ycfg)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to parse yaml config: %w", err)
	}

	cfg.AnonymizeFields = make(map[string][]string)
	for table, csvFields := range ycfg.Anonymize {
		fields := strings.Split(csvFields, ",")
		fieldList := make([]string, 0, len(fields))
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field != "" {
				fieldList = append(fieldList, field)
			}
		}
		cfg.AnonymizeFields[table] = fieldList
	}

	cfg.SkipTables = ycfg.Skip

	if ycfg.Sample != nil {
		cfg.SampleTables = make(map[string]float64, len(ycfg.Sample))
		for table, pct := range ycfg.Sample {
			cfg.SampleTables[table] = pct
		}
	} else {
		cfg.SampleTables = make(map[string]float64)
	}
	return nil
}
