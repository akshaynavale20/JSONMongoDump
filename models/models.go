package models

type ConfigDetails struct {
	DatabaseURL       string     `json:"dbURL"`
	DatabaseName      string     `json:"dbName"`
	CollectionURL      string     `json:"dbCollectionName"`
	InputFolderPath      string  `json:"inputFolderPath"`
	OutputFolderPath string `json:"outputFolderPath"`
	ErrorFolderPath	string `json:"errorFolderPath"`
}
type JSONFileData struct {
	Data     interface{}    `json`
}