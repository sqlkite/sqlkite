package main

import (
	"flag"

	"src.goblgobl.com/utils/log"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/config"
	"src.sqlkite.com/sqlkite/http"
	"src.sqlkite.com/sqlkite/super"
)

func main() {
	configPath := flag.String("config", "config.json", "full path to config file")
	migrations := flag.Bool("migrations", false, "only run migrations and exit")
	flag.Parse()

	config, err := config.Configure(*configPath)
	if err != nil {
		log.Fatal("load_config").String("path", *configPath).Err(err).Log()
		return
	}

	if err := sqlkite.Init(config); err != nil {
		log.Fatal("sqlkite_init").Err(err).Log()
		return
	}

	if *migrations || config.Migrations == nil || *config.Migrations == true {
		if err := super.DB.EnsureMigrations(); err != nil {
			log.Fatal("sqlkite_migrations").Err(err).Log()
			return
		}
	} else {
		log.Info("migrations_skip").Log()
	}

	if *migrations {
		return
	}

	http.Listen()
}
