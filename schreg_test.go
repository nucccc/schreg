package schreg

import (
	"fmt"
	"testing"

	"github.com/hamba/avro"
	"github.com/nucccc/schreg/compatibility_levels"
)

func TestDefault(t *testing.T) {
	fmt.Println(default_config_map)
	if default_registry_url, present := (*default_config_map)["registry_url"]; !present {
		t.Errorf("key %s not present in default_config_map", "registry_url")
	} else if default_registry_url_str, ok := default_registry_url.(string); !ok {
		t.Errorf("key %s of wrong type in default_config_map", "registry_url")
	} else if default_registry_url_str != DEFAULT_REGISTRY_URL {
		t.Errorf("key %s of wrong value in default_config_map, got: %s expected: %s", "registry_url", default_registry_url_str, DEFAULT_REGISTRY_URL)
	}
}

func TestConstructor(t *testing.T) {
	_, _ = NewSchRegClient(default_config_map)
}

func TestTestContainer(t *testing.T) {
	/*ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-schema-registry:5.4.1",
		ExposedPorts: []string{"8081/tcp"},
		//WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	schema_reg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := schema_reg.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err.Error())
		}
	}()*/
}

func TestPostSchema(t *testing.T) {
	//IN THIS TEST I'M GONNA ASSUME A DOCKER COMPOSE IS RUNNING

	str_schema, err := avro.Parse("string")
	if err != nil {
		t.Fatalf("not even able to parse an avro string type, got error %s", err)
	}
	f1_schema, err := avro.NewField("f1", str_schema, nil)
	if err != nil {
		t.Fatalf("not even able to build a field schema, got error %s", err)
	}
	f2_schema, err := avro.NewField("f2", str_schema, nil)
	if err != nil {
		t.Fatalf("not even able to build a field schema, got error %s", err)
	}
	rec_schema, err := avro.NewRecordSchema("rec_schema", "", []*avro.Field{f1_schema, f2_schema})

	_, err = PostSubjectCompatibilityLevel(compatibility_levels.NoneCL, DEFAULT_REGISTRY_URL, DEFAULT_DUMP_SUBJECT)
	if err != nil {
		t.Log(err)
		t.Error(nil)
	}

	id, err := PostSchema(rec_schema, DEFAULT_REGISTRY_URL, DEFAULT_DUMP_SUBJECT)
	t.Log(id)
	if err != nil {
		t.Log(err)
		t.Error(nil)
	}
}
