package instance

import (
	"context"
	"fmt"
	"testing"

	configv1alpha1 "github.com/six-group/haproxy-operator/apis/config/v1alpha1"
	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type timeoutConfigValidator struct {
	calls int
}

func (v *timeoutConfigValidator) Validate(context.Context, ConfigValidationRequest) error {
	v.calls++
	return fmt.Errorf("%w: test", errConfigValidationTimeout)
}

// The listed Backend is generation 1; storedGeneration 2 simulates kubectl apply landing mid-reconcile.
func staleBackendReconcileFixture(t *testing.T, storedGeneration int64, annotations map[string]string) (*Reconciler, *proxyv1alpha1.Instance, *configv1alpha1.BackendList, *timeoutConfigValidator) {
	t.Helper()
	scheme := validationTestScheme(t)
	if err := configv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add config scheme: %v", err)
	}
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default", UID: "instance-uid", Generation: 1, Annotations: annotations},
	}
	stored := &configv1alpha1.Backend{
		ObjectMeta: metav1.ObjectMeta{Name: "backend0", Namespace: "default", UID: "backend-uid", Generation: storedGeneration},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, stored).WithStatusSubresource(instance, stored).Build()

	current := &configv1alpha1.Backend{}
	if err := cli.Get(context.Background(), client.ObjectKeyFromObject(stored), current); err != nil {
		t.Fatalf("get stored backend: %v", err)
	}
	if current.Generation != storedGeneration {
		t.Fatalf("test setup expects stored backend generation %d, got %d", storedGeneration, current.Generation)
	}
	stale := current.DeepCopy()
	stale.Generation = 1

	validator := &timeoutConfigValidator{}
	reconciler := &Reconciler{Client: cli, Scheme: scheme, ConfigValidator: validator}
	return reconciler, instance, &configv1alpha1.BackendList{Items: []configv1alpha1.Backend{*stale}}, validator
}

func TestReconcileConfigUsesListedGenerationsForTimeoutRevision(t *testing.T) {
	reconciler, instance, backends, validator := staleBackendReconcileFixture(t, 2, nil)
	listens := &configv1alpha1.ListenList{}
	frontends := &configv1alpha1.FrontendList{}
	resolvers := &configv1alpha1.ResolverList{}
	expected := validationSourceRevision(instance, listens, frontends, backends.DeepCopy(), resolvers)

	if _, err := reconciler.reconcileConfig(context.Background(), instance, listens, frontends, backends, resolvers); err == nil {
		t.Fatal("expected validation timeout error")
	}
	if validator.calls != 1 {
		t.Fatalf("expected one validation call, got %d", validator.calls)
	}

	storedInstance := &proxyv1alpha1.Instance{}
	if err := reconciler.Get(context.Background(), client.ObjectKeyFromObject(instance), storedInstance); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if storedInstance.Annotations[validationStateAnnotationKey] != validationStateTimedOut {
		t.Fatalf("expected timed-out state, got %q", storedInstance.Annotations[validationStateAnnotationKey])
	}
	if got := storedInstance.Annotations[validationSourceRevisionKey]; got != expected {
		t.Fatalf("expected timeout revision from the listed (generation 1) inputs used to build the config, got %q want %q", got, expected)
	}
}

func TestReconcileConfigSkipsTimedOutConfigWhenInputsRefreshDuringReconcile(t *testing.T) {
	listens := &configv1alpha1.ListenList{}
	frontends := &configv1alpha1.FrontendList{}
	resolvers := &configv1alpha1.ResolverList{}

	first, firstInstance, firstBackends, _ := staleBackendReconcileFixture(t, 1, nil)
	if _, err := first.reconcileConfig(context.Background(), firstInstance, listens, frontends, firstBackends, resolvers); err == nil {
		t.Fatal("expected validation timeout error")
	}
	timedOut := &proxyv1alpha1.Instance{}
	if err := first.Get(context.Background(), client.ObjectKeyFromObject(firstInstance), timedOut); err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if timedOut.Annotations[validationStateAnnotationKey] != validationStateTimedOut {
		t.Fatalf("expected timed-out state after first reconcile, got %q", timedOut.Annotations[validationStateAnnotationKey])
	}

	second, secondInstance, secondBackends, validator := staleBackendReconcileFixture(t, 2, timedOut.Annotations)
	if _, err := second.reconcileConfig(context.Background(), secondInstance, listens, frontends, secondBackends, resolvers); err == nil {
		t.Fatal("expected cached validation timeout error")
	}
	if validator.calls != 0 {
		t.Fatalf("expected the timed-out config built from unchanged listed inputs not to be validated again, got %d validation calls", validator.calls)
	}
}
