package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
)

type AvroSchema struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Fields    []struct {
		Name string      `json:"name"`
		Type interface{} `json:"type"`
	} `json:"fields"`
}

func deserializeAvro(schema *srclient.Schema, cleanPayload []byte) ([]byte, error) {
	codec := schema.Codec()
	native, _, err := codec.NativeFromBinary(cleanPayload)
	if err != nil {
		return nil, fmt.Errorf("unable to deserailize avro: %w", err)
	}
	value, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to json: %w", err)
	}

	return value, nil
}

func DeserializePayload(srClient *srclient.SchemaRegistryClient, payload []byte) ([]byte, *srclient.Schema, error) {
	// first byte of the payload is 0
	if payload[0] != byte(0) {
		return nil, nil, fmt.Errorf("failed to deserialize payload: magic byte is not 0")
	}

	// next 4 bytes contain the schema id
	schemaID := binary.BigEndian.Uint32(payload[1:5])
	schema, err := srClient.GetSchema(int(schemaID))
	if err != nil {
		return nil, schema, err
	}

	var value []byte
	value, err = deserializeAvro(schema, payload[5:])
	if err != nil {
		return nil, schema, err
	}
	return value, schema, nil

	//schemaType := schema.SchemaType().String()
	//switch schemaType {
	//case srclient.Avro.String():
	//	var value []byte
	//	value, err = deserializeAvro(schema, payload[5:])
	//	if err != nil {
	//		return nil, schema, err
	//	}
	//	return value, schema, nil
	//default:
	//	return nil, schema, fmt.Errorf("unsupported schema type: %s", schemaType)
	//}
}

func SchemaName(schema *srclient.Schema) (string, error) {
	var avroSchema AvroSchema
	if err := json.Unmarshal([]byte(schema.Schema()), &avroSchema); err != nil {
		fmt.Println("Error unmarshalling Avro schema:", err)
		return "", err
	}

	// Accessing the namespace and name
	namespace := avroSchema.Namespace
	name := avroSchema.Name

	return fmt.Sprintf("%s.%s", namespace, name), nil
}
