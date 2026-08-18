package instance

import (
	"context"
	"errors"
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

func TestValidateRetriesJobDeadlineExceeded(t *testing.T) {
	scheme := validationTestScheme(t)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &validationRetryTestClient{Client: baseClient, failAttempts: 1}
	validator := NewKubernetesConfigValidator(cli, scheme)
	validator.retryDelays = zeroRetryDelays(validator)

	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}
	if cli.createdJobs != 2 {
		t.Fatalf("expected two validation Jobs (initial plus one retry), got %d", cli.createdJobs)
	}
	if len(cli.createdJobNames) != 2 || cli.createdJobNames[0] == cli.createdJobNames[1] {
		t.Fatalf("expected a distinct Job name for each attempt, got %v", cli.createdJobNames)
	}
	for _, jobName := range cli.createdJobNames {
		if len(jobName) > 63 {
			t.Fatalf("validation Job name exceeds DNS label length: %q", jobName)
		}
	}
	jobs := &batchv1.JobList{}
	if err := cli.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("list validation Jobs: %v", err)
	}
	if len(jobs.Items) != 1 || jobs.Items[0].Name != cli.createdJobNames[1] {
		t.Fatalf("expected timed-out Jobs to be deleted and final Job retained, got %v Jobs", len(jobs.Items))
	}
}

func TestValidateRetriesOperatorWaitTimeout(t *testing.T) {
	scheme := validationTestScheme(t)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &validationRetryTestClient{Client: baseClient, pendingAttempts: 1}
	validator := NewKubernetesConfigValidator(cli, scheme)
	validator.retryDelays = zeroRetryDelays(validator)
	validator.validationTimeout = 10 * time.Millisecond

	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if err != nil {
		t.Fatalf("expected retry after Operator wait timeout to succeed, got: %v", err)
	}
	if cli.createdJobs != 2 {
		t.Fatalf("expected two validation Jobs (initial plus retry), got %d", cli.createdJobs)
	}
}

func TestValidateStopsAfterMaximumTimeoutRetries(t *testing.T) {
	scheme := validationTestScheme(t)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &validationRetryTestClient{Client: baseClient, pendingAttempts: 2}
	validator := NewKubernetesConfigValidator(cli, scheme)
	validator.retryDelays = zeroRetryDelays(validator)
	validator.validationTimeout = 10 * time.Millisecond

	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if !errors.Is(err, errConfigValidationTimeout) {
		t.Fatalf("expected final validation timeout after retry limit, got: %v", err)
	}
	if cli.createdJobs != 2 {
		t.Fatalf("expected initial Job plus one retry, got %d Jobs", cli.createdJobs)
	}
	jobs := &batchv1.JobList{}
	if err := cli.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("list validation Jobs after exhausted retries: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("expected timed-out Jobs to be removed after exhaustion, got %d", len(jobs.Items))
	}

	if err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	}); err != nil {
		t.Fatalf("expected a later validation cycle with the same data to start a fresh Job, got: %v", err)
	}
	if cli.createdJobs != 3 {
		t.Fatalf("expected a fresh Job on a later validation cycle, got %d total Jobs", cli.createdJobs)
	}
}

func TestValidateDoesNotRetrySecondTimeout(t *testing.T) {
	scheme := validationTestScheme(t)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &validationRetryTestClient{Client: baseClient, failAttempts: 5}
	validator := NewKubernetesConfigValidator(cli, scheme)
	validator.retryDelays = zeroRetryDelays(validator)

	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if !errors.Is(err, errConfigValidationTimeout) {
		t.Fatalf("expected validation timeout after the single retry, got: %v", err)
	}
	if cli.createdJobs != 2 {
		t.Fatalf("expected no second timeout retry (2 Jobs total), got %d Jobs", cli.createdJobs)
	}
}

func TestValidateDoesNotRetryNonTimeoutJobFailure(t *testing.T) {
	scheme := validationTestScheme(t)
	baseClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cli := &validationRetryTestClient{Client: baseClient, failAttempts: 1, failureReason: "BackoffLimitExceeded"}
	validator := NewKubernetesConfigValidator(cli, scheme)
	validator.retryDelays = zeroRetryDelays(validator)

	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	err := validator.Validate(context.Background(), ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
	if err == nil || strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected non-timeout validation failure, got: %v", err)
	}
	if cli.createdJobs != 1 {
		t.Fatalf("expected no retry for non-timeout Job failure, created %d Jobs", cli.createdJobs)
	}
}

// zeroRetryDelays keeps the production retry count while removing the wait.
func zeroRetryDelays(v *KubernetesConfigValidator) []time.Duration {
	return make([]time.Duration, len(v.retryDelays))
}

type validationRetryTestClient struct {
	client.Client
	createdJobs     int
	createdJobNames []string
	failAttempts    int
	pendingAttempts int
	failureReason   string
}

func (c *validationRetryTestClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if job, ok := obj.(*batchv1.Job); ok {
		c.createdJobs++
		c.createdJobNames = append(c.createdJobNames, job.Name)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *validationRetryTestClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	job, ok := obj.(*batchv1.Job)
	if !ok || c.createdJobs == 0 {
		return nil
	}
	if c.createdJobs <= c.pendingAttempts {
		job.Status = batchv1.JobStatus{Active: 1}
		return nil
	}
	if c.createdJobs <= c.failAttempts {
		reason := c.failureReason
		if reason == "" {
			reason = "DeadlineExceeded"
		}
		job.Status = batchv1.JobStatus{
			Failed: 1,
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  corev1.ConditionTrue,
				Reason:  reason,
				Message: "test failure",
			}},
		}
		return nil
	}
	job.Status = batchv1.JobStatus{Succeeded: 1}
	return nil
}
