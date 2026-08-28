package v1alpha1

import (
	"strings"
	"testing"
	"time"

	parser "github.com/haproxytech/client-native/v6/config-parser"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestDefaultsConfigurationModelWithMaxconn(t *testing.T) {
	d := &DefaultsConfiguration{
		Mode:    "http",
		Maxconn: ptr.To(int64(2000)),
		Timeouts: map[string]metav1.Duration{
			"client":  {Duration: 5 * time.Second},
			"connect": {Duration: 5 * time.Second},
			"server":  {Duration: 10 * time.Second},
		},
	}

	model, err := d.Model()
	if err != nil {
		t.Fatalf("Model() returned error: %v", err)
	}

	if model.Maxconn == nil || *model.Maxconn != 2000 {
		t.Fatalf("expected maxconn 2000, got %#v", model.Maxconn)
	}
}

func TestDefaultsConfigurationAddToParserWithMaxconn(t *testing.T) {
	d := &DefaultsConfiguration{
		Mode:    "http",
		Maxconn: ptr.To(int64(2000)),
		Timeouts: map[string]metav1.Duration{
			"client":  {Duration: 5 * time.Second},
			"connect": {Duration: 5 * time.Second},
			"server":  {Duration: 10 * time.Second},
		},
	}

	p, err := parser.New()
	if err != nil {
		t.Fatalf("parser.New() returned error: %v", err)
	}

	if err := d.AddToParser(p); err != nil {
		t.Fatalf("AddToParser() returned error: %v", err)
	}

	if !strings.Contains(p.String(), "maxconn 2000") {
		t.Fatalf("expected generated config to contain maxconn 2000, got:\n%s", p.String())
	}
}
