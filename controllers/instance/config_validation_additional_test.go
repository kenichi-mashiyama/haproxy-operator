package instance

import (
	"context"
	"testing"

	"github.com/go-logr/zapr"
	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func TestClearForcedValidationState(t *testing.T) {
	scheme := validationTestScheme(t)
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-haproxy",
			Namespace: "default",
			Annotations: map[string]string{
				forceRevalidationAnnotationKey: "true",
				validationHashAnnotationKey:    "cached-hash",
				validationStateAnnotationKey:   validationStateFailed,
				validationErrorAnnotationKey:   "validation failed",
			},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
	reconciler := &Reconciler{Client: cli, Scheme: scheme}

	if err := reconciler.clearForcedValidationState(context.Background(), instance); err != nil {
		t.Fatalf("clear forced validation state: %v", err)
	}

	stored := &proxyv1alpha1.Instance{}
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(instance), stored); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if _, found := stored.Annotations[forceRevalidationAnnotationKey]; found {
		t.Fatal("expected force-revalidation annotation to be consumed")
	}
	if state, sameHash := getValidationState(stored, "cached-hash"); sameHash || state != "" {
		t.Fatalf("expected cached validation state to be cleared, got state %q for matching hash %t", state, sameHash)
	}
	if _, found := stored.Annotations[validationErrorAnnotationKey]; found {
		t.Fatal("expected cached validation error to be cleared")
	}
}

func TestSetValidationStatePersistsTimeoutFailure(t *testing.T) {
	scheme := validationTestScheme(t)
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
	reconciler := &Reconciler{Client: cli, Scheme: scheme}
	validationErr := "haproxy config validation timed out: context deadline exceeded"

	if err := reconciler.setValidationState(context.Background(), instance, "timeout-hash", validationStateFailed, validationErr); err != nil {
		t.Fatalf("persist timeout validation state: %v", err)
	}

	stored := &proxyv1alpha1.Instance{}
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(instance), stored); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if state, sameHash := getValidationState(stored, "timeout-hash"); !sameHash || state != validationStateFailed {
		t.Fatalf("expected failed state for timeout hash, got state %q for matching hash %t", state, sameHash)
	}
	if stored.Annotations[validationErrorAnnotationKey] != validationErr {
		t.Fatalf("expected timeout error %q, got %q", validationErr, stored.Annotations[validationErrorAnnotationKey])
	}
}

func TestValidationJobPodName(t *testing.T) {
	scheme := validationTestScheme(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-haproxy-validate-pod",
			Namespace: "default",
			Labels:    map[string]string{"job-name": "test-haproxy-validate-12345678"},
		},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)

	if podName := validator.validationJobPodName(context.Background(), "default", "test-haproxy-validate-12345678"); podName != pod.Name {
		t.Fatalf("expected Pod name %q, got %q", pod.Name, podName)
	}
}

func TestValidationJobFailed(t *testing.T) {
	job := &batchv1.Job{Status: batchv1.JobStatus{Failed: 1}}
	if !validationJobFailed(job) {
		t.Fatal("expected failed Job to be identified")
	}
}

func TestWaitForValidationJobLogsSuccess(t *testing.T) {
	scheme := validationTestScheme(t)
	jobName := "test-haproxy-validate-12345678"
	podName := "test-haproxy-validate-12345678-abcde"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      podName,
		Namespace: "default",
		Labels:    map[string]string{"job-name": jobName},
	}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job, pod).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)
	logs, observed := observer.New(zapcore.InfoLevel)
	ctx := log.IntoContext(context.Background(), zapr.NewLogger(zap.New(logs)))

	if err := validator.waitForValidationJob(ctx, "default", jobName, "test-haproxy-secret"); err != nil {
		t.Fatalf("wait for successful validation Job: %v", err)
	}

	entries := observed.FilterMessage("HAProxy config validation job succeeded").All()
	if len(entries) != 1 {
		t.Fatalf("expected one success log entry, got %d", len(entries))
	}
	if entries[0].Level != zapcore.InfoLevel {
		t.Fatalf("expected success log level info, got %s", entries[0].Level)
	}
	assertValidationLogFields(t, entries[0].Context, jobName, podName)
}

func TestWaitForValidationJobLogsFailure(t *testing.T) {
	scheme := validationTestScheme(t)
	jobName := "test-haproxy-validate-12345678"
	podName := "test-haproxy-validate-12345678-abcde"
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"},
		Status:     batchv1.JobStatus{Failed: 1},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      podName,
		Namespace: "default",
		Labels:    map[string]string{"job-name": jobName},
	}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(job, pod).Build()
	validator := NewKubernetesConfigValidator(cli, scheme)
	logs, observed := observer.New(zapcore.DebugLevel)
	ctx := log.IntoContext(context.Background(), zapr.NewLogger(zap.New(logs)))

	if err := validator.waitForValidationJob(ctx, "default", jobName, "test-haproxy-secret"); err == nil {
		t.Fatal("expected failed validation Job error")
	}

	entries := observed.FilterMessage("HAProxy config validation job failed").All()
	if len(entries) != 1 {
		t.Fatalf("expected one failure log entry, got %d", len(entries))
	}
	if entries[0].Level != zapcore.ErrorLevel {
		t.Fatalf("expected failure log level error, got %s", entries[0].Level)
	}
	assertValidationLogFields(t, entries[0].Context, jobName, podName)
}

func assertValidationLogFields(t *testing.T, fields []zapcore.Field, jobName, podName string) {
	t.Helper()
	values := make(map[string]string, len(fields))
	for _, field := range fields {
		if field.Type == zapcore.StringType {
			values[field.Key] = field.String
		}
	}
	if values["job"] != jobName {
		t.Errorf("expected job field %q, got %q", jobName, values["job"])
	}
	if values["pod"] != podName {
		t.Errorf("expected pod field %q, got %q", podName, values["pod"])
	}
}

func validationTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add Kubernetes scheme: %v", err)
	}
	if err := proxyv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add proxy scheme: %v", err)
	}

	return scheme
}
