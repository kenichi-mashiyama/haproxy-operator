package v1alpha1

import (
	"strings"
	"testing"

	parser "github.com/haproxytech/client-native/v6/config-parser"
	"github.com/haproxytech/client-native/v6/models"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestDefaultsConfigurationModelWithOptions(t *testing.T) {
	d := &DefaultsConfiguration{
		Mode:     "http",
		Timeouts: map[string]metav1.Duration{},
		Options: &DefaultsOptions{
			LogSeparateErrors: ptr.To(true),
			LogHealthChecks:   ptr.To(true),
			Dontlognull:       ptr.To(false),
			DontlogNormal:     ptr.To(false),
			HTTPLogCLF:        ptr.To(true),
			Redispatch:        ptr.To(true),
		},
	}

	model, err := d.Model()
	if err != nil {
		t.Fatalf("Model() returned error: %v", err)
	}

	if model.LogSeparateErrors != models.DefaultsBaseLogSeparateErrorsEnabled {
		t.Fatalf("unexpected log-separate-errors value: %s", model.LogSeparateErrors)
	}
	if model.LogHealthChecks != models.DefaultsBaseLogHealthChecksEnabled {
		t.Fatalf("unexpected log-health-checks value: %s", model.LogHealthChecks)
	}
	if model.Dontlognull != models.DefaultsBaseDontlognullDisabled {
		t.Fatalf("unexpected dontlognull value: %s", model.Dontlognull)
	}
	if model.DontlogNormal != models.DefaultsBaseDontlogNormalDisabled {
		t.Fatalf("unexpected dontlog-normal value: %s", model.DontlogNormal)
	}
	if !model.Httplog {
		t.Fatalf("expected httplog to be enabled")
	}
	if !model.Clflog {
		t.Fatalf("expected clflog to be enabled")
	}
	if model.Redispatch == nil || model.Redispatch.Enabled == nil || *model.Redispatch.Enabled != models.RedispatchEnabledEnabled {
		t.Fatalf("expected redispatch to be enabled")
	}
}

func TestDefaultsConfigurationAddToParserWithOptions(t *testing.T) {
	d := &DefaultsConfiguration{
		Mode:     "http",
		Timeouts: map[string]metav1.Duration{},
		Options: &DefaultsOptions{
			LogSeparateErrors: ptr.To(true),
			LogHealthChecks:   ptr.To(true),
			Dontlognull:       ptr.To(false),
			DontlogNormal:     ptr.To(false),
			HTTPLogCLF:        ptr.To(true),
		},
	}

	p, err := parser.New()
	if err != nil {
		t.Fatalf("parser.New() returned error: %v", err)
	}

	if err := d.AddToParser(p); err != nil {
		t.Fatalf("AddToParser() returned error: %v", err)
	}

	cfg := p.String()
	checks := []string{
		"option httplog",
		"option log-separate-errors",
		"option log-health-checks",
		"no option dontlognull",
		"no option dontlog-normal",
	}

	for _, check := range checks {
		if !strings.Contains(cfg, check) {
			t.Fatalf("expected generated config to contain %q, got:\n%s", check, cfg)
		}
	}
}

func TestDefaultsConfigurationAddToParserWithHTTPLogCLF(t *testing.T) {
	d := &DefaultsConfiguration{
		Mode:     "http",
		Timeouts: map[string]metav1.Duration{},
		Options: &DefaultsOptions{
			HTTPLogCLF: ptr.To(true),
		},
	}

	p, err := parser.New()
	if err != nil {
		t.Fatalf("parser.New() returned error: %v", err)
	}

	if err := d.AddToParser(p); err != nil {
		t.Fatalf("AddToParser() returned error: %v", err)
	}

	cfg := p.String()
	if !strings.Contains(cfg, "option httplog clf") {
		t.Fatalf("expected generated config to contain %q, got:\n%s", "option httplog clf", cfg)
	}
}
