package instance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestValidationTimeoutErrorLog reproduces a validation Job that never reaches
// a terminal state and logs the timeout returned by the validator.
func TestValidationTimeoutErrorLog(t *testing.T) {
	scheme := validationTestScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := validator.Validate(ctx, ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if err == nil {
		t.Fatal("expected validation timeout error")
	}

	t.Logf("Timeout error log: %v", err)
	if !strings.Contains(err.Error(), "haproxy config validation timed out") {
		t.Fatalf("expected timeout error, got %q", err.Error())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

// TestValidationKubernetesAPIErrorLog reproduces a Kubernetes API failure while
// looking up the validation Job and logs the error returned by the validator.
func TestValidationKubernetesAPIErrorLog(t *testing.T) {
	scheme := validationTestScheme(t)
	apiErr := apierrors.NewInternalError(errors.New("validation API unavailable"))
	cli := &validationErrorClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		getErr: apiErr,
	}
	validator := NewKubernetesConfigValidator(cli, scheme)
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
	}

	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if err == nil {
		t.Fatal("expected Kubernetes API error")
	}

	t.Logf("K8s API error log: %v", err)
	if !strings.Contains(err.Error(), "validation API unavailable") {
		t.Fatalf("expected Kubernetes API error, got %q", err.Error())
	}
}

type validationErrorClient struct {
	client.Client
	getErr error
}

func (c *validationErrorClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return c.getErr
}