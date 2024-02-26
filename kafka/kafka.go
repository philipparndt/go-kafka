package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/philipparndt/go-logger"
	"github.com/riferrei/srclient"
	"log"
	"os"
)

func (cfg *KafkaConf) NewSchemaRegistryClient() (*srclient.SchemaRegistryClient, error) {
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(cfg.SchemaRegistry.MakeSchemaRegistryURL())
	schemaRegistryClient.SetCredentials(cfg.SchemaRegistry.Username, cfg.SchemaRegistry.Password)

	return schemaRegistryClient, nil
}

func (cfg *KafkaConf) NewConsumer(id string) (sarama.Consumer, error) {
	// Set up configuration
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	config.ClientID = id

	if tlsC, err := cfg.Connection.TLS.MakeTLSConfig(); tlsC != nil && err == nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsC
	}

	// Create a new consumer
	consumer, err := sarama.NewConsumer(cfg.Brokers, config)
	if err != nil {
		log.Fatal("Error creating consumer:", err)
		return nil, err
	}

	return consumer, nil
}

func (cfg *KafkaConf) NewKafka() (sarama.Consumer, *srclient.SchemaRegistryClient, error) {
	consumer, err := cfg.NewConsumer("kaftail-vehubtoken-consumer")
	if err != nil {
		logger.Error("Error creating consumer:", err)
		os.Exit(1)
		return nil, nil, err
	}

	schemaRegistryClient, err := cfg.NewSchemaRegistryClient()
	if err != nil {
		logger.Error("Error creating schema registry client:", err)
		os.Exit(1)
		return nil, nil, err
	}

	return consumer, schemaRegistryClient, nil
}

func (cfg *KafkaConf) StartConsume(topic string) (sarama.PartitionConsumer, *srclient.SchemaRegistryClient, error) {
	consumer, schemaRegistryClient, err := cfg.NewKafka()
	if err != nil {
		logger.Error("Error creating consumer:", err)
		os.Exit(1)
		return nil, nil, err
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("Error creating partition consumer:", err)
		return nil, nil, err
	}

	return partitionConsumer, schemaRegistryClient, nil
}

func (tlsConf *TLSConf) MakeTLSConfig() (*tls.Config, error) {
	if tlsConf.CertFile == "" || tlsConf.KeyFile == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(tlsConf.CertFile, tlsConf.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("error loading X509 certificate/key pair: %v", err)
	}

	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing certificate: %v", err)
	}

	config := tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		ClientAuth:   tls.NoClientCert,
	}

	if tlsConf.CAFile != "" {
		// Load CA cert
		caCert, err := os.ReadFile(tlsConf.CAFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.RootCAs = caCertPool
	}

	return &config, nil
}

func (conf *SchemaRegistryConf) MakeSchemaRegistryURL() string {
	logger.Info("Schema Registry Conf: ", conf)
	if conf.URL == "" {
		return ""
	}

	url := fmt.Sprintf("%s://%s", conf.Scheme, conf.URL)
	logger.Info("Schema Registry URL: ", url)
	return url
}
