package schreg

import (
	"fmt"
	"testing"
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
