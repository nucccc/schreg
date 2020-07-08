package schreg

import (
	"sync"

	"github.com/hamba/avro"
)

const (
	defaultRegistryUrl    = "http://localhost:8081"
	defaultDynamicSubject = "dynamic-subject"
)

/*	Struct aimed at representing the Client entitled to interact with the schema registry
	through http */
type SchemaRegistryClient struct {
	registry_url         string
	dynamic_subject      string
	schema_id_cache_lock sync.RWMutex
	schema_id_cache      map[[32]byte]int
	schema_cache_lock    sync.RWMutex
	schema_cache         map[int]avro.Schema
}

/*	Function aimed at providing a new client */
func NewSchemaRegistryClient(input_registry_url string, input_dynamic_subject string) *SchemaRegistryClient {
	new_client := new(SchemaRegistryClient)

	if input_registry_url != "" {
		new_client.registry_url = input_registry_url
	} else {
		new_client.registry_url = defaultRegistryUrl
	}

	if input_dynamic_subject != "" {
		new_client.dynamic_subject = input_dynamic_subject
	} else {
		new_client.dynamic_subject = defaultDynamicSubject
	}

	new_client.schema_cache = make(map[int]avro.Schema)
	new_client.schema_id_cache = make(map[[32]byte]int)
	return new_client
}

func (client *SchemaRegistryClient) GetSchemaID(schema avro.Schema) (int, error) {
	var result_id int
	var schema_in_cache bool
	client.schema_id_cache_lock.RLock()
	result_id, schema_in_cache = client.schema_id_cache[schema.Fingerprint()]
	client.schema_id_cache_lock.RUnlock()
	if schema_in_cache {
		return result_id, nil
	}
	//If the schema is not present in cache I shall post it

}

func (client *SchemaRegistryClient) PostSchema(schema avro.Schema, schema_registry_url string, subject string) (int, error) {

}
