package schreg

import (
	"context"
	"fmt"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"
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
	ctx := context.Background()
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
	}()
}
