package config

import (
	"path"
	"testing"

	"src.goblgobl.com/tests/assert"
)

func init() {

}

func Test_Config_InvalidPath(t *testing.T) {
	_, err := Configure("invalid.json")
	assert.Equal(t, err.Error(), "code: 303001 - open invalid.json: no such file or directory")
}

func Test_Config_InvalidJson(t *testing.T) {
	_, err := Configure(testConfigPath("invalid_config.json"))
	assert.Equal(t, err.Error(), "code: 303002 - expected colon after object key")
}

func Test_Config_RootPath_Missing(t *testing.T) {
	_, err := Configure(testConfigPath("missing_root_path_config.json"))
	assert.Equal(t, err.Error(), "code: 303004 - root_path is required")
}

func Test_Config_RootPath_NonExisting(t *testing.T) {
	_, err := Configure(testConfigPath("nonexisting_root_path_config.json"))
	assert.Equal(t, err.Error(), "code: 303005 - stat i/hope/this/doesn't/exist: no such file or directory")
}

func Test_Config_RootPath_File(t *testing.T) {
	_, err := Configure(testConfigPath("file_root_path_config.json"))
	assert.Equal(t, err.Error(), "code: 303006 - root_path must be a directory")
}

func Test_Config_FileRootPath(t *testing.T) {
	_, err := Configure(testConfigPath("file_root_path_config.json"))
	assert.Equal(t, err.Error(), "code: 303006 - root_path must be a directory")
}

func Test_Config_Minimal(t *testing.T) {
	_, err := Configure(testConfigPath("minimal_config.json"))
	assert.Nil(t, err)
}

func Test_Config_Maximal(t *testing.T) {
	config, err := Configure(testConfigPath("maximal_config.json"))
	assert.Nil(t, err)
	assert.Equal(t, config.RootPath, "/tmp/")
}

func testConfigPath(file string) string {
	return path.Join("../tests/data/", file)
}
