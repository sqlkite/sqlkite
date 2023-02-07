package config

import (
	"os"
	"strings"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/json"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/super"
)

type Config struct {
	InstanceId uint8             `json:"instance_id"`
	Migrations *bool             `json:"migrations"`
	RootPath   string            `json:"root_path"`
	HTTP       HTTP              `json:"http"`
	Log        log.Config        `json:"log"`
	Super      super.Config      `json:"super"`
	Buffer     *buffer.Config    `json:"buffer"`
	Validation validation.Config `json:"validation"`
}

type Buffer struct {
	Count uint32
}

type HTTP struct {
	Super          string             `json:"super"`
	Admin          string             `json:"admin"`
	Listen         string             `json:"listen"`
	Project        HTTPProject        `json:"project"`
	Authentication HTTPAuthentication `json:"auth"`
}

type HTTPProject struct {
	Type string `json:"type"`
}

type HTTPAuthentication struct {
	Type string `json:"type"`
}

func Configure(filePath string) (Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return Config{}, log.Err(codes.ERR_READ_CONFIG, err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return config, log.Err(codes.ERR_PARSE_CONFIG, err)
	}

	if err := log.Configure(config.Log); err != nil {
		return config, err
	}

	if config.RootPath == "" {
		return config, log.Errf(codes.ERR_CONFIG_ROOT_PATH_REQUIRED, "root_path is required")
	}

	if fi, err := os.Stat(config.RootPath); err != nil {
		return config, log.Err(codes.ERR_CONFIG_ROOT_PATH_INVALID, err)
	} else if !fi.IsDir() {
		return config, log.Errf(codes.ERR_CONFIG_ROOT_PATH_FILE, "root_path must be a directory")
	}

	if !strings.HasSuffix(config.RootPath, "/") {
		config.RootPath += "/"
	}

	if config.Buffer == nil {
		config.Buffer = &buffer.Config{
			Count: 1000,
			Min:   32768,   // 32K
			Max:   1048576, // 1MB
		}
	}

	if err := validation.Configure(config.Validation); err != nil {
		return config, err
	}

	if err := super.Configure(config.Super); err != nil {
		return config, err
	}

	return config, nil
}
