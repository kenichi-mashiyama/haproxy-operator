package instance

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	retryInfoLogMessage   = "HAProxy config validation timed out, retrying"
	giveUpErrorLogMessage = "HAProxy config validation timed out after retry, giving up"
)

func observedLogContext() (context.Context, *observer.ObservedLogs) {
	core, observed := observer.New(zapcore.DebugLevel)
	logger := crzap.New(crzap.RawZapOpts(zap.WrapCore(func(zapcore.Core) zapcore.Core { return core })))
	return log.IntoContext(context.Background(), logger), observed
}

func runTimeoutLogValidation(t *testing.T, cli *validationRetryTestClient, ctx context.Context) error {
	t.Helper()
	validator := NewKubernetesConfigValidator(cli, cli.Scheme())
	validator.retryDelays = zeroRetryDelays(validator)
	validator.validationTimeout = 10 * time.Millisecond
	instance := &proxyv1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-haproxy", Namespace: "default"},
		Spec:       proxyv1alpha1.InstanceSpec{Image: "haproxy:test"},
	}
	return validator.Validate(ctx, ConfigValidationRequest{
		Instance: instance,
		Data:     map[string][]byte{"haproxy.cfg": []byte("global\n")},
	})
}

func TestValidateLogsInfoBeforeTimeoutRetry(t *testing.T) {
	scheme := validationTestScheme(t)
	cli := &validationRetryTestClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), pendingAttempts: 1}
	ctx, observed := observedLogContext()

	if err := runTimeoutLogValidation(t, cli, ctx); err != nil {
		t.Fatalf("expected retry to succeed, got: %v", err)
	}

	retryLogs := observed.FilterMessage(retryInfoLogMessage).All()
	if len(retryLogs) != 1 {
		t.Fatalf("expected one retry info log, got %d", len(retryLogs))
	}
	if retryLogs[0].Level != zapcore.InfoLevel {
		t.Fatalf("expected retry log level info, got %s", retryLogs[0].Level)
	}
	fields := retryLogs[0].ContextMap()
	if fields["job"] != cli.createdJobNames[0] {
		t.Fatalf("expected retry log job %q, got %v", cli.createdJobNames[0], fields["job"])
	}
	if _, ok := fields["retryAfter"]; !ok {
		t.Fatalf("expected retry log to include retryAfter, got %v", fields)
	}
	if got := observed.FilterMessage(giveUpErrorLogMessage).Len(); got != 0 {
		t.Fatalf("expected no give-up error log when retry succeeds, got %d", got)
	}
}

func TestValidateLogsErrorWhenRetryTimesOut(t *testing.T) {
	scheme := validationTestScheme(t)
	cli := &validationRetryTestClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), pendingAttempts: 2}
	ctx, observed := observedLogContext()

	if err := runTimeoutLogValidation(t, cli, ctx); err == nil {
		t.Fatal("expected validation timeout after retry")
	}

	if got := observed.FilterMessage(retryInfoLogMessage).Len(); got != 1 {
		t.Fatalf("expected one retry info log, got %d", got)
	}
	giveUpLogs := observed.FilterMessage(giveUpErrorLogMessage).All()
	if len(giveUpLogs) != 1 {
		t.Fatalf("expected one give-up error log, got %d", len(giveUpLogs))
	}
	if giveUpLogs[0].Level != zapcore.ErrorLevel {
		t.Fatalf("expected give-up log level error, got %s", giveUpLogs[0].Level)
	}
	fields := giveUpLogs[0].ContextMap()
	if !strings.Contains(fmt.Sprint(fields["error"]), "haproxy config validation timed out") {
		t.Fatalf("expected give-up log to include timeout error, got %v", fields["error"])
	}
	if fields["job"] != cli.createdJobNames[1] || fmt.Sprint(fields["attempts"]) != "2" {
		t.Fatalf("expected give-up log to include retry job and attempts=2, got %v", fields)
	}
}

func TestValidateDoesNotLogTimeoutRetryForNonTimeoutFailure(t *testing.T) {
	scheme := validationTestScheme(t)
	cli := &validationRetryTestClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build(), failAttempts: 1, failureReason: "BackoffLimitExceeded"}
	ctx, observed := observedLogContext()

	if err := runTimeoutLogValidation(t, cli, ctx); err == nil {
		t.Fatal("expected non-timeout validation failure")
	}

	if got := observed.FilterMessage(retryInfoLogMessage).Len(); got != 0 {
		t.Fatalf("expected no retry info log for non-timeout failure, got %d", got)
	}
	if got := observed.FilterMessage(giveUpErrorLogMessage).Len(); got != 0 {
		t.Fatalf("expected no give-up error log for non-timeout failure, got %d", got)
	}
}
