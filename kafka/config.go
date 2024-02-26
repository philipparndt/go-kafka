package kafka

type TLSConf struct {
	CertFile string `json:"cert-file"`
	KeyFile  string `json:"key-file"`
	CAFile   string `json:"ca-file"`
}

type SchemaRegistryConf struct {
	URL      string `json:"url"`
	Scheme   string `json:"scheme"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type KafkaConf struct {
	Brokers []string `json:"brokers"`

	Connection struct {
		TLS TLSConf `json:"tls"`
	} `json:"connection"`

	SchemaRegistry SchemaRegistryConf `json:"schema-registry"`
}
