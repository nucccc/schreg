package schreg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/hamba/avro"

	//"go_connector/connector/sensorupdate"
	"github.com/nucccc/schreg/compatibility_levels"
)

const (
	DEFAULT_REGISTRY_URL string = "http://localhost:8081"
	DEFAULT_DUMP_SUBJECT string = "dumpsubject"
	DEFAULT_ENABLE_DUMP  bool   = false
	DEFAULT_ENABLE_CACHE bool   = true
	INVALID_ID           int    = -1
	ID_RESPONSE_KEY      string = "id"
)

type SchregConfigMap map[string]interface{}

var default_config_map *SchregConfigMap = &SchregConfigMap{
	"registry_url":        DEFAULT_REGISTRY_URL,
	"enable_dump_subject": true,
	"dump_subject":        DEFAULT_DUMP_SUBJECT,
	"enable_cache":        true,
}

func (conf_map *SchregConfigMap) fill_defaults() {
	var present bool
	if _, present = (*conf_map)["registry_url"]; !present {
		(*conf_map)["registry_url"] = DEFAULT_REGISTRY_URL
	}
	if _, present := (*conf_map)["enable_dump_subject"]; !present {
		(*conf_map)["enable_dump_subject"] = DEFAULT_ENABLE_DUMP
	}
	if _, present = (*conf_map)["dump_subject"]; !present {
		(*conf_map)["dump_subject"] = DEFAULT_DUMP_SUBJECT
	}
	if _, present = (*conf_map)["enable_cache"]; !present {
		(*conf_map)["enable_cache"] = DEFAULT_ENABLE_CACHE
	}
}

/*
	CLIENT
*/

/*	Struct aimed at representing the Client entitled to interact with the schema registry
	through http */
type SchRegClient struct {
	registry_url         string
	enable_dump_subject  bool
	dump_subject         string
	enable_cache         bool
	schema_cache_lock    sync.RWMutex
	schema_cache         map[int]avro.Schema
	schema_id_cache_lock sync.RWMutex
	schema_id_cache      map[[32]byte]int
}

func NewSchRegClient(config_map *SchregConfigMap) (schregcl *SchRegClient, err error) {
	config_map.fill_defaults()
	schregcl = &SchRegClient{
		registry_url:        (*config_map)["registry_url"].(string),
		enable_dump_subject: (*config_map)["enable_dump_subject"].(bool),
		enable_cache:        (*config_map)["enable_cache"].(bool),
	}
	if schregcl.enable_dump_subject {
		schregcl.dump_subject = (*config_map)["dump_subject"].(string)
		err = schregcl.initDumpSubject()
		if err != nil {
			return nil, err
		}
	}
	if schregcl.enable_cache {
		schregcl.schema_cache_lock = sync.RWMutex{}
		schregcl.schema_cache = make(map[int]avro.Schema)
		schregcl.schema_id_cache_lock = sync.RWMutex{}
		schregcl.schema_id_cache = make(map[[32]byte]int)
	}
	return schregcl, nil
}

/*
	CLIENT METHODS
*/

/*	sets the compatibility level to NONE on the dump subject */
func (schregcl *SchRegClient) initDumpSubject() error {
	_, err := PostSubjectCompatibilityLevel(
		compatibility_levels.NoneCL,
		schregcl.registry_url,
		schregcl.dump_subject,
	)
	if err != nil {
		return fmt.Errorf("unable set NONE compatibility level on dump subject: %s, error: %s", schregcl.dump_subject, err)
	}
	return nil
}

/*	Method which return the id in the schema registry of a schema given a hamba avro Schema interface.
	It will first query the internal cache, and then if the schema is not present, it will send a Post request
	for the schema id via http to the schema registry. */
func (client *SchRegClient) GetSchemaID(schema avro.Schema) (int, error) {
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
	log.Println("Maybe I'm here")
	//If the schema is not present in cache I shall post it, using the address of the client and its default subject channel
	result_id, err = PostSchema(schema, client.registry_url, client.dump_subject)
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

func (client *SchRegClient) GetSchemaByID(id int) (avro.Schema, error) {
	/*var result_id int
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
	log.Println("Maybe I'm here")
	//If the schema is not present in cache I shall post it, using the address of the client and its default subject channel
	result_id, err = PostSchema(schema, client.registry_url, client.dump_subject)
	if err != nil {
		return result_id, err
	} else if !IsSchemaIdValid(result_id) {
		return result_id, errors.New("Id received is not valid")
	}
	//if a valid id was successfully received, I store it in the cache and return it as a result withour error
	client.schema_id_cache_lock.Lock()
	client.schema_id_cache[schema.Fingerprint()] = result_id
	client.schema_id_cache_lock.Unlock()
	return result_id, nil*/
	var result_schema avro.Schema
	var err error
	var id_in_cache bool

	client.schema_cache_lock.RLock()
	result_schema, id_in_cache = client.schema_cache[id]
	client.schema_cache_lock.RUnlock()

	if id_in_cache {
		return result_schema, nil
	}

	result_schema, err = GetSchema(client.registry_url, id)

	if err != nil {
		return nil, err
	}

	client.schema_cache_lock.Lock()
	client.schema_cache[id] = result_schema
	client.schema_cache_lock.Unlock()

	return result_schema, nil
}

/*
	FUNCTIONS
*/

/*	Just a function containing a basic logic to check if an id is valid or not */
func IsSchemaIdValid(schema_id int) bool {
	if schema_id > 0 {
		return true
	}
	return false
}

/*
	SCHEMA REGISTRY INTERACTION FUNCTIONS
*/

/*	Function aimed at setting the compatibility level on a given subject.
	It received a predefined compatibility level, the url of a schema registry (to which it will attach the route towards the
	subject configuration) and the subject.
	The function will output the resulting compatibility level on the subject, together with eventual errors */
func PostSubjectCompatibilityLevel(compatibility_level compatibility_levels.CompatibilityLevel, schema_registry_url string, subject string) (compatibility_levels.CompatibilityLevel, error) {
	var json_send, json_receive map[string]interface{} //maps used to hold data being sent to and coming from the jsons on http
	var message_body, response_body []byte
	var err error
	var ok bool
	var response_complev compatibility_levels.CompatibilityLevel
	var response_complev_acq string
	var error_code int
	var error_code_acquiral float64
	var error_message string
	json_send = make(map[string]interface{})
	json_receive = make(map[string]interface{})
	//I set the message to be sent on http
	json_send["compatibility"] = string(compatibility_level)
	message_body, err = json.Marshal(json_send)
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	//I then send the put request theough an http client
	log.Println("Start client creation")
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, schema_registry_url+"/config/"+subject, bytes.NewBuffer(message_body))
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := client.Do(req)
	log.Println("Request done")
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	//I then read and umarshal the received response
	response_body, err = ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	err = json.Unmarshal(response_body, &json_receive)
	if err != nil {
		return compatibility_levels.InvalidCL, err
	}
	//once umarshaled, I shall ensure that a compatibility field is present in the response body
	if response_complev_acq, ok = json_receive["compatibility"].(string); ok {
		response_complev = compatibility_levels.CompatibilityLevel(response_complev_acq)
		return response_complev, nil
	}
	//in case no compatibility response was found, I shall at first decode an eventual error message, and eventually send the final error message
	if error_code_acquiral, ok = json_receive["error_code"].(float64); ok {
		error_code = int(error_code_acquiral)
		error_message = json_receive["message"].(string)

		return compatibility_levels.InvalidCL, errors.New("Error: " + strconv.Itoa(error_code) + " message: " + error_message)
	}
	return compatibility_levels.InvalidCL, errors.New("Compatibility Level not present in response")
}

func PostSchema(schema avro.Schema, schema_registry_url string, subject string) (int, error) {
	var result_id int
	var result_acquiral float64
	var message_body, response_body []byte
	var err error
	var json_send, json_receive map[string]interface{}
	var ok bool

	json_send = make(map[string]interface{})
	json_receive = make(map[string]interface{})

	json_send["schema"] = schema.String()
	message_body, err = json.Marshal(json_send)
	if err != nil {
		return INVALID_ID, err
	}
	log.Println("boh")
	resp, err := http.Post(schema_registry_url+"/subjects/"+subject+"/versions", "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(message_body))
	log.Println("got it")
	if err != nil {
		return INVALID_ID, err
	}
	defer resp.Body.Close()
	response_body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return INVALID_ID, err
	}
	err = json.Unmarshal(response_body, &json_receive)
	if err != nil {
		return INVALID_ID, err
	}
	if result_acquiral, ok = json_receive[ID_RESPONSE_KEY].(float64); ok {
		result_id = int(result_acquiral)
		return result_id, nil
	}
	fmt.Println(json_receive)
	return INVALID_ID, errors.New("id key '" + ID_RESPONSE_KEY + "' not found in http response")
}

func GetSchema(schema_registry_url string, id int) (avro.Schema, error) {
	var err error
	var error_message string
	var response *http.Response
	var contents []byte
	var decoded_mess map[string]interface{}
	var schema_str string
	var ok bool
	var schema_result avro.Schema

	response, err = http.Get(schema_registry_url + "/schemas/ids/" + strconv.Itoa(id))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	decoded_mess = make(map[string]interface{})
	err = json.Unmarshal(contents, &decoded_mess)
	if err != nil {
		return nil, err
	}
	if err_code, ok := decoded_mess["error_code"]; ok {
		error_message = "Error code: " + strconv.Itoa(err_code.(int))
		if err_mess, ok := decoded_mess["message"]; ok {
			error_message = error_message + "; message: " + err_mess.(string)
		}
		return nil, errors.New(error_message)
	}
	schema_str, ok = decoded_mess["schema"].(string)
	if !ok {
		return nil, errors.New("Error: schema not present in response")
	}
	schema_result, err = avro.Parse(schema_str)
	if err != nil {
		return nil, err
	}
	return schema_result, nil
}
