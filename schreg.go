package schreg

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"github.com/hamba/avro"

	"github.com/Nuc94/schreg/compatibility_levels"
)

const (
	defaultRegistryUrl    string = "http://localhost:8081"
	defaultDynamicSubject string = "dynamic-subject"
	invalidId             int    = -1
	idResponseKey         string = "id"
)

/*
	CLIENT
*/

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

/*
	CLIENT METHODS
*/

/*	Method which return the id in the schema registry of a schema given a hamba avro Schema interface.
	It will first query the internal cache, and then if the schema is not present, it will send a Post request
	for the schema id via http to the schema registry. */
func (client *SchemaRegistryClient) GetSchemaID(schema avro.Schema) (int, error) {
	var result_id int
	var err error
	var schema_in_cache bool
	//at first we check if the id is present in cache
	client.schema_id_cache_lock.RLock()
	result_id, schema_in_cache = client.schema_id_cache[schema.Fingerprint()]
	client.schema_id_cache_lock.RUnlock()
	//if the schema was already cached I return it with no error
	if schema_in_cache {
		return result_id, nil
	}
	//If the schema is not present in cache I shall post it, using the address of the client and its default subject channel
	result_id, err = PostSchema(schema, client.registry_url, client.dynamic_subject)
	if err != nil {
		return result_id, err
	} else if !IsSchemaIdValid(result_id) {
		return result_id, errors.New("Id received is not valid")
	}
	//if a valid id was successfully received, I store it in the cache and return it as a result withour error
	client.schema_id_cache_lock.Lock()
	client.schema_id_cache[schema.Fingerprint()] = result_id
	client.schema_id_cache_lock.Unlock()
	return result_id, nil
}

/*
	FUNCTIONS
*/

/*	Just a function containing a basic logic to check if an id is valid or not */
func IsSchemaIdValid(schema_id int) bool {
	if schema_id <= 0 {
		return false
	}
	return true
}

/*
	SCHEMA REGISTRY INTERACTION FUNCTIONS
*/

func PostSubjectCompatibilityLevel(compatibility_level compatibility_levels.CompatibilityLevel, schema_registry_url string, subject string) (compatibility_levels.CompatibilityLevel, error) {
	var json_send, json_receive map[string]interface{}
	var message_body, response_body []byte
	var err error
	var ok bool
	var response_complev compatibility_levels.CompatibilityLevel
	var error_code int
	var error_message string
	json_send = make(map[string]interface{})
	json_receive = make(map[string]interface{})
	json_send["compatibility"] = compatibility_level
	message_body, err = json.Marshal(json_send)
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	resp, err := http.Post(schema_registry_url+"/config/"+subject, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(message_body))
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	resp.Body.Close()
	err = json.Unmarshal(response_body, &json_receive)
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	if response_complev, ok = json_receive["compatibility"].(compatibility_levels.CompatibilityLevel); ok {
		return response_complev, nil
	}
	if error_code, ok = json_receive["error_code"].(int); ok {
		error_message = json_receive["message"].(string)

		return compatibility_levels.InvalidCL, errors.New("Error: " + strconv.Itoa(error_code) + " message: " + error_message)
	}
	return compatibility_levels.InvalidCL, errors.New("Compatibility Level not present in response")

}

func PostSchema(schema avro.Schema, schema_registry_url string, subject string) (int, error) {
	var result_id int
	var message_body, response_body []byte
	var err error
	var json_send, json_receive map[string]interface{}
	var ok bool
	json_send = make(map[string]interface{})
	json_receive = make(map[string]interface{})

	json_send["schema"] = schema.String()
	message_body, err = json.Marshal(json_send)
	if err != nil {
		return invalidId, err
	}
	resp, err := http.Post(schema_registry_url+"/subjects/"+subject+"/versions", "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(message_body))
	if err != nil {
		return invalidId, err
	}
	resp.Body.Close()
	response_body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return invalidId, err
	}
	err = json.Unmarshal(response_body, &json_receive)
	if err != nil {
		return invalidId, err
	}
	if result_id, ok = json_receive[idResponseKey].(int); ok {
		return result_id, nil
	}
	return invalidId, errors.New("id key '" + idResponseKey + "' not found in http response")
}
