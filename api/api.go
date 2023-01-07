package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/hamba/avro"
	"github.com/nucccc/schreg/compatibility_levels"
)

const CONTENT_TYPE string = "Accept: application/vnd.schemaregistry.v1+json"

/*
	ERROR HANDLING FUNCTIONS

	since some errors are common i'd like to have some standardized code for them
*/

func errInReqBuild(err error) error {
	return fmt.Errorf("unable to build request, error: %s", err)
}

func errInReceivingResp(err error) error {
	return fmt.Errorf("unable to receive response, error: %s", err)
}

func errIn404ResNotFound() error {
	return fmt.Errorf("error due to resource not found")
}

func errInSchRegBackend() error {
	return fmt.Errorf("internal backend error at schema registry")
}

/*
	API FUNCTIONS
*/

func getSchemaByIDReq(registry_url string, id int) (req *http.Request, err error) {
	req_url := fmt.Sprintf("%s/schemas/ids/%d/schema", registry_url, id)
	req, err = http.NewRequest(http.MethodGet, req_url, nil)
	return
}

/*	the function to get a schema by Id */
func GetSchemaByID(registry_url string, id int, client *http.Client) (avro.Schema, error) {
	req, err := getSchemaByIDReq(registry_url, id)
	if err != nil {
		return nil, errInReqBuild(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errInReceivingResp(err)
	}
	if resp.StatusCode == 404 {
		return nil, errIn404ResNotFound()
	} else if resp.StatusCode == 500 {
		return nil, errInSchRegBackend()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body, error: %s", err)
	}
	schema, err := avro.Parse(string(body))
	if err != nil {
		return nil, fmt.Errorf("unable to parse schema body, error: %s", err)
	}
	return schema, nil
}

func getSubjectsReq(registry_url string) (req *http.Request, err error) {
	req_url := fmt.Sprintf("%s/subjects", registry_url)
	req, err = http.NewRequest(http.MethodGet, req_url, nil)
	return
}

func GetSubjects(registry_url string, client *http.Client) ([]string, error) {
	req, err := getSubjectsReq(registry_url)
	if err != nil {
		return nil, errInReqBuild(err)
	}
	resp, err := client.Do(req)
	if resp.StatusCode == 500 {
		return nil, errInSchRegBackend()
	}

	//TO COMPLETE

	return nil, nil
}

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

func getSchema(registry_url string, id int, client *http.Client) (avro.Schema, error) {
	req_url := fmt.Sprintf("%s/schemas/ids/%d/schema", registry_url, id)
	resp, err := http.Get(req_url)
	if err != nil {
		return nil, fmt.Errorf("error in obtaining response from server due to err: %s", err)
	}
	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("error due to resource not found, err: %s", err)
	} else if resp.StatusCode == 500 {
		return nil, fmt.Errorf("internal backend error at schema registry, err: %s", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	http

	return nil, nil
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
