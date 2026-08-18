package instance

import (
	"context"
	"strings"
	"testing"

	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestValidationJobDiscardsHAProxyOutput ensures the short-lived Job never surfaces
// haproxy -c stdout/stderr (which may contain certificate paths, ACL values, backend
// addresses, ...) in the Pod log; only the exit code must be observable.
func TestValidationJobDiscardsHAProxyOutput(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add k8s scheme: %v", err)
	}
	if err := proxyv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add proxy scheme: %v", err)
	}

	data := map[string][]byte{"haproxy.cfg": []byte("global\n")}
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:3.4.2-trixie"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)

	ctx, cancel := context.WithTimeout(context.Background(), 20_000_000) // 20ms
	defer cancel()

	_ = validator.Validate(ctx, ConfigValidationRequest{Instance: instance, Data: data})

	jobs := &batchv1.JobList{}
	if err := cli.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected exactly one validation Job to be created, got %d", len(jobs.Items))
	}

	containers := jobs.Items[0].Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected exactly one container, got %d", len(containers))
	}

	container := containers[0]
	if len(container.Command) != 2 || container.Command[0] != "sh" || container.Command[1] != "-c" {
		t.Fatalf("expected container Command to be [\"sh\" \"-c\"], got %v", container.Command)
	}
	if len(container.Args) != 1 {
		t.Fatalf("expected a single shell argument, got %v", container.Args)
	}

	shellArg := container.Args[0]
	if !strings.Contains(shellArg, "haproxy -c -f") {
		t.Fatalf("expected shell argument to still run haproxy -c, got %q", shellArg)
	}
	if !strings.Contains(shellArg, ">/dev/null 2>&1") {
		t.Fatalf("expected shell argument to discard stdout/stderr, got %q", shellArg)
	}
}

// TestWaitForValidationJobDoesNotLeakConditionMessage ensures that a JobFailed condition
// message (which could echo container output) is never propagated as the validation error.
func TestWaitForValidationJobDoesNotLeakConditionMessage(t *testing.T) {
	scheme := validationTestScheme(t)
	jobName := "test-haproxy-validate-12345678"

	sensitiveMessage := "unable to load certificate from file '/usr/local/etc/haproxy/port443.pem.crt': no start line."
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Message: sensitiveMessage,
				},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)

	err := validator.waitForValidationJob(context.Background(), "default", jobName, "test-haproxy-secret")
	if err == nil {
		t.Fatal("expected failed validation Job error")
	}
	if strings.Contains(err.Error(), sensitiveMessage) {
		t.Fatalf("expected error to not leak condition.Message, got %q", err.Error())
	}
	if err.Error() != "haproxy config validation job failed" {
		t.Fatalf("expected fixed error message, got %q", err.Error())
	}
}

// TestValidationJobSpecSettings ensures the validation Job has ActiveDeadlineSeconds,
// TTLSecondsAfterFinished, and BackoffLimit properly configured.
func TestValidationJobSpecSettings(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add k8s scheme: %v", err)
	}
	if err := proxyv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add proxy scheme: %v", err)
	}

	data := map[string][]byte{"haproxy.cfg": []byte("global\n")}
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:3.4.2-trixie"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)

	ctx, cancel := context.WithTimeout(context.Background(), 20_000_000) // 20ms
	defer cancel()

	_ = validator.Validate(ctx, ConfigValidationRequest{Instance: instance, Data: data})

	jobs := &batchv1.JobList{}
	if err := cli.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("expected exactly one validation Job to be created, got %d", len(jobs.Items))
	}

	jobSpec := jobs.Items[0].Spec
	if jobSpec.ActiveDeadlineSeconds == nil || *jobSpec.ActiveDeadlineSeconds != 120 {
		t.Fatalf("expected ActiveDeadlineSeconds to be 120, got %v", jobSpec.ActiveDeadlineSeconds)
	}
	if jobSpec.TTLSecondsAfterFinished == nil || *jobSpec.TTLSecondsAfterFinished != 60 {
		t.Fatalf("expected TTLSecondsAfterFinished to be 60, got %v", jobSpec.TTLSecondsAfterFinished)
	}
	if jobSpec.BackoffLimit == nil || *jobSpec.BackoffLimit != 0 {
		t.Fatalf("expected BackoffLimit to be 0, got %v", jobSpec.BackoffLimit)
	}
}
