package instance

import (
	"context"
	"strings"
	"testing"
	"time"

	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateReusesExistingRunningJob(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add k8s scheme: %v", err)
	}
	if err := proxyv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add proxy scheme: %v", err)
	}

	data := map[string][]byte{"haproxy.cfg": []byte("global\n")}
	hash := validationDataHash(data)
	jobName := validationResourceName("test-haproxy", hash)

	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"},
		Status: batchv1.JobStatus{
			Active: 1,
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := validator.Validate(ctx, ConfigValidationRequest{
		Instance: &proxyv1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"}},
		Data:     data,
	})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got: %v", err)
	}

	jobs := &batchv1.JobList{}
	if err := cli.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected a single reused validation job, got %d", len(jobs.Items))
	}

	secrets := &corev1.SecretList{}
	if err := cli.List(context.Background(), secrets, client.InNamespace("default")); err != nil {
		t.Fatalf("list secrets: %v", err)
	}
	if len(secrets.Items) != 0 {
		t.Fatalf("expected no new validation secret for reused running job, got %d", len(secrets.Items))
	}
}
