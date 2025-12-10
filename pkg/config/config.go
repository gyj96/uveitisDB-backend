package config

import (
	"log"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type App struct {
	Name string `mapstructure:"name"`
	Env  string `mapstructure:"env"`
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type Auth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type Database struct {
	Path string `mapstructure:"path"`
}

type Logging struct {
	Level string `mapstructure:"level"`
	File  string `mapstructure:"file"`
}

type Config struct {
	App      App      `mapstructure:"app"`
	Auth     Auth     `mapstructure:"auth"`
	Database Database `mapstructure:"database"`
	Logging  Logging  `mapstructure:"logging"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	// Conservative defaults for a desktop app.
	v.SetDefault("app.host", "0.0.0.0")
	v.SetDefault("app.port", 8080)
	v.SetDefault("app.env", "development")
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.file", "./logs/app.log")

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	// Enable config hot-reload for development, but keep best-effort.
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		log.Printf("config file changed: %s", e.Name)
	})

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
