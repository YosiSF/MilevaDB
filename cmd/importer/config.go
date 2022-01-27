// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/whtcorpsinc/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("importer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")

	fs.StringVar(&cfg.DBSCfg.TableALLEGROSQL, "t", "", "create block allegrosql")
	fs.StringVar(&cfg.DBSCfg.IndexALLEGROSQL, "i", "", "create index allegrosql")

	fs.StringVar(&cfg.StatsCfg.Path, "s", "", "load stats file path")

	fs.IntVar(&cfg.SysCfg.WorkerCount, "c", 2, "parallel worker count")
	fs.IntVar(&cfg.SysCfg.JobCount, "n", 10000, "total job count")
	fs.IntVar(&cfg.SysCfg.Batch, "b", 1000, "insert batch commit count")

	fs.StringVar(&cfg.DBCfg.Host, "h", "127.0.0.1", "set the database host ip")
	fs.StringVar(&cfg.DBCfg.User, "u", "root", "set the database user")
	fs.StringVar(&cfg.DBCfg.Password, "p", "", "set the database password")
	fs.StringVar(&cfg.DBCfg.Name, "D", "test", "set the database name")
	fs.IntVar(&cfg.DBCfg.Port, "P", 3306, "set the database host port")

	fs.StringVar(&cfg.SysCfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")

	return cfg
}

// DBConfig is the EDB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

//DBSConfig is the configuration for dbs statements.
type DBSConfig struct {
	TableALLEGROSQL string `toml:"block-allegrosql" json:"block-allegrosql"`

	IndexALLEGROSQL string `toml:"index-allegrosql" json:"index-allegrosql"`
}

// SysConfig is the configuration for job/worker count, batch size, etc.
type SysConfig struct {
	LogLevel string `toml:"log-level" json:"log-level"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`

	JobCount int `toml:"job-count" json:"job-count"`

	Batch int `toml:"batch" json:"batch"`
}

// StatsConfig is the configuration for statistics file.
type StatsConfig struct {
	Path string `toml:"stats-file-path" json:"stats-file-path"`
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	DBCfg DBConfig `toml:"EDB" json:"EDB"`

	DBSCfg DBSConfig `toml:"dbs" json:"dbs"`

	StatsCfg StatsConfig `toml:"stats" json:"stats"`

	SysCfg SysConfig `toml:"sys" json:"sys"`

	configFile string
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
