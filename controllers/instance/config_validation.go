package instance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	haproxy "github.com/haproxytech/client-native/v6/configuration/options"
	proxyv1alpha1 "github.com/six-group/haproxy-operator/apis/proxy/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	configValidationTTLSeconds            = int32(60)
	configValidationActiveDeadlineSeconds = int64(120)
	configValidationTimeout               = 2 * time.Minute
	configValidationDeleteTimeout         = 30 * time.Second
)

var (
	errConfigValidationTimeout  = errors.New("haproxy config validation timed out")
	configValidationRetryDelays = []time.Duration{
		30 * time.Second,
	}
)

// ConfigValidationRequest describes the generated configuration payload that must be validated.
type ConfigValidationRequest struct {
	Instance *proxyv1alpha1.Instance
	Data     map[string][]byte
}

// ConfigValidator validates generated HAProxy configuration before it is stored in the runtime secret.
type ConfigValidator interface {
	Validate(ctx context.Context, req ConfigValidationRequest) error
}

// NonRetryableValidationError marks a validation error that should not trigger immediate controller retries.
type NonRetryableValidationError struct {
	cause error
}

func (e *NonRetryableValidationError) Error() string {
	if e == nil || e.cause == nil {
		return "config validation failed"
	}

	return e.cause.Error()
}

func (e *NonRetryableValidationError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.cause
}

func newNonRetryableValidationError(err error) error {
	if err == nil {
		return nil
	}

	return &NonRetryableValidationError{cause: err}
}

func isNonRetryableValidationError(err error) bool {
	var target *NonRetryableValidationError
	return errors.As(err, &target)
}

// NoopConfigValidator skips validation and always returns success.
type NoopConfigValidator struct{}

// Validate always succeeds.
func (NoopConfigValidator) Validate(_ context.Context, _ ConfigValidationRequest) error {
	return nil
}

// KubernetesConfigValidator validates generated configuration by creating a short-lived job in the cluster.
type KubernetesConfigValidator struct {
	client            client.Client
	scheme            *runtime.Scheme
	retryDelays       []time.Duration
	validationTimeout time.Duration
}

// NewKubernetesConfigValidator creates a validator that runs haproxy -c in a short-lived job.
func NewKubernetesConfigValidator(client client.Client, scheme *runtime.Scheme) *KubernetesConfigValidator {
	return &KubernetesConfigValidator{
		client:            client,
		scheme:            scheme,
		retryDelays:       configValidationRetryDelays,
		validationTimeout: configValidationTimeout,
	}
}

// Validate validates the generated configuration and returns an error if the haproxy -c check fails.
func (v *KubernetesConfigValidator) Validate(ctx context.Context, req ConfigValidationRequest) error {
	if req.Instance == nil {
		return fmt.Errorf("config validation requires instance")
	}

	hash := validationDataHash(req.Data)
	secretName := validationResourceName(req.Instance.Name+"-secret", hash)
	cycleID := ""
	for attempt := 0; ; attempt++ {
		jobName := validationResourceName(req.Instance.Name, hash)
		if attempt > 0 {
			if cycleID == "" {
				cycleNonce := make([]byte, 8)
				if _, err := rand.Read(cycleNonce); err != nil {
					return fmt.Errorf("generate validation attempt ID: %w", err)
				}
				cycleID = hex.EncodeToString(cycleNonce)
			}
			jobName = validationRetryJobName(req.Instance.Name, attempt, cycleID)
		}

		job := &batchv1.Job{}
		err := v.client.Get(ctx, client.ObjectKey{Name: jobName, Namespace: req.Instance.Namespace}, job)
		if err != nil && !k8serrors.IsNotFound(err) {
			return err
		}
		if k8serrors.IsNotFound(err) {
			if err := v.ensureValidationSecret(ctx, req.Instance, secretName, req.Data); err != nil {
				return err
			}
			if err := v.createValidationJob(ctx, req.Instance, jobName, secretName); err != nil && !k8serrors.IsAlreadyExists(err) {
				return err
			}
		}

		err = v.waitForValidationJob(ctx, req.Instance.Namespace, jobName, secretName)
		if !errors.Is(err, errConfigValidationTimeout) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}

		if deleteErr := v.deleteValidationJob(req.Instance.Namespace, jobName); deleteErr != nil {
			_ = v.deleteValidationSecret(secretName, req.Instance.Namespace)
			return errors.Join(err, deleteErr)
		}

		if attempt >= len(v.retryDelays) {
			log.FromContext(ctx).Error(err, "HAProxy config validation timed out after retry, giving up", "job", jobName, "attempts", attempt+1)
			_ = v.deleteValidationSecret(secretName, req.Instance.Namespace)
			return err
		}

		log.FromContext(ctx).Info("HAProxy config validation timed out, retrying", "job", jobName, "retryAfter", v.retryDelays[attempt])
		if err := waitForValidationRetry(ctx, v.retryDelays[attempt]); err != nil {
			_ = v.deleteValidationSecret(secretName, req.Instance.Namespace)
			return err
		}
	}
}

func (v *KubernetesConfigValidator) ensureValidationSecret(ctx context.Context, instance *proxyv1alpha1.Instance, secretName string, data map[string][]byte) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: instance.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, v.client, secret, func() error {
		if err := controllerutil.SetOwnerReference(instance, secret, v.scheme); err != nil {
			return err
		}
		secret.Data = data
		return nil
	})
	return err
}

func (v *KubernetesConfigValidator) createValidationJob(ctx context.Context, instance *proxyv1alpha1.Instance, jobName, secretName string) error {
	image := instance.Spec.Image
	if image == "" {
		image = "haproxy:latest"
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"proxy.haproxy.com/instance": instance.Name,
				"proxy.haproxy.com/validate": "true",
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   ptr.To(configValidationActiveDeadlineSeconds),
			TTLSecondsAfterFinished: ptr.To(configValidationTTLSeconds),
			BackoffLimit:            ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:            "haproxy-config-check",
					Image:           image,
					ImagePullPolicy: instance.Spec.ImagePullPolicy,
					// Discard haproxy -c stdout/stderr so config detail never reaches the pod log; only the exit code is kept.
					Command: []string{"sh", "-c"},
					Args:    []string{fmt.Sprintf("haproxy -c -f %s >/dev/null 2>&1", haproxy.DefaultConfigurationFile)},
					VolumeMounts: []corev1.VolumeMount{{
						Name:      "haproxy-config",
						MountPath: filepath.Dir(haproxy.DefaultConfigurationFile),
					}},
				}},
				Volumes: []corev1.Volume{{
					Name:         "haproxy-config",
					VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName}},
				}},
			}},
		},
	}

	if err := controllerutil.SetOwnerReference(instance, job, v.scheme); err != nil {
		return err
	}
	return v.client.Create(ctx, job)
}

func (v *KubernetesConfigValidator) deleteValidationJob(namespace, jobName string) error {
	deleteCtx, cancel := context.WithTimeout(context.Background(), configValidationDeleteTimeout)
	defer cancel()
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace}}
	if err := v.client.Delete(deleteCtx, job, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	return wait.PollUntilContextCancel(deleteCtx, 100*time.Millisecond, true, func(checkCtx context.Context) (bool, error) {
		current := &batchv1.Job{}
		err := v.client.Get(checkCtx, client.ObjectKey{Name: jobName, Namespace: namespace}, current)
		if k8serrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (v *KubernetesConfigValidator) deleteValidationSecret(secretName, namespace string) error {
	err := v.client.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

func waitForValidationRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (v *KubernetesConfigValidator) waitForValidationJob(ctx context.Context, namespace, jobName, secretName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, v.validationTimeout)
	defer cancel()

	pollErr := wait.PollUntilContextCancel(waitCtx, 500*time.Millisecond, true, func(checkCtx context.Context) (bool, error) {
		current := &batchv1.Job{}
		if err := v.client.Get(checkCtx, client.ObjectKey{Name: jobName, Namespace: namespace}, current); err != nil {
			if k8serrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		if validationJobFailed(current) {
			if podName := v.validationJobPodName(checkCtx, namespace, jobName); podName != "" {
				log.FromContext(checkCtx).Error(errors.New("haproxy config validation job failed"), "HAProxy config validation job failed", "job", jobName, "pod", podName)
			}
		}

		if validationJobDeadlineExceeded(current) {
			return false, fmt.Errorf("%w: %w", errConfigValidationTimeout, context.DeadlineExceeded)
		}

		if current.Status.Succeeded > 0 {
			log.FromContext(checkCtx).Info("HAProxy config validation job succeeded", "job", jobName, "pod", v.validationJobPodName(checkCtx, namespace, jobName))
			_ = v.client.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}})
			return true, nil
		}

		if current.Status.Failed > 0 && current.Status.Active == 0 {
			_ = v.client.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}})
			return false, errors.New("haproxy config validation job failed")
		}

		for _, condition := range current.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				log.FromContext(checkCtx).Info("HAProxy config validation job succeeded", "job", jobName, "pod", v.validationJobPodName(checkCtx, namespace, jobName))
				_ = v.client.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}})
				return true, nil
			}
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				_ = v.client.Delete(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace}})
				// condition.Message is intentionally not propagated: it must never carry container output.
				return false, errors.New("haproxy config validation job failed")
			}
		}

		return false, nil
	})
	if pollErr != nil {
		if waitCtx.Err() != nil {
			return fmt.Errorf("%w: %w", errConfigValidationTimeout, waitCtx.Err())
		}
		return pollErr
	}

	return nil
}

func validationJobDeadlineExceeded(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue && condition.Reason == "DeadlineExceeded" {
			return true
		}
	}
	return false
}

func validationJobFailed(job *batchv1.Job) bool {
	if job == nil {
		return false
	}

	if job.Status.Failed > 0 && job.Status.Active == 0 {
		return true
	}

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func (v *KubernetesConfigValidator) validationJobPodName(ctx context.Context, namespace, jobName string) string {
	pods := &corev1.PodList{}
	if err := v.client.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		return ""
	}

	if len(pods.Items) == 0 {
		return ""
	}

	return pods.Items[0].Name
}

func validationDataHash(data map[string][]byte) string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, key := range keys {
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(data[key])
		_, _ = h.Write([]byte{0})
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func validationResourceName(prefix, hash string) string {
	suffix := hash
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}

	base := fmt.Sprintf("%s-validate-%s", prefix, suffix)
	if len(base) <= 63 {
		return base
	}

	// Leave room for the suffix separator and hash.
	trimLen := 63 - len("-validate-") - len(suffix)
	if trimLen < 1 {
		trimLen = 1
	}

	trimmed := prefix
	if len(trimmed) > trimLen {
		trimmed = trimmed[:trimLen]
	}
	trimmed = strings.TrimSuffix(trimmed, "-")
	if trimmed == "" {
		trimmed = "h"
	}

	return fmt.Sprintf("%s-validate-%s", trimmed, suffix)
}

func validationRetryJobName(instanceName string, attempt int, cycleID string) string {
	suffix := fmt.Sprintf("retry-%d-%s", attempt, cycleID)
	maxPrefixLength := 63 - len("-validate-") - len(suffix)
	if maxPrefixLength < 1 {
		maxPrefixLength = 1
	}
	if len(instanceName) > maxPrefixLength {
		instanceName = instanceName[:maxPrefixLength]
	}
	instanceName = strings.TrimSuffix(instanceName, "-")
	if instanceName == "" {
		instanceName = "h"
	}
	return fmt.Sprintf("%s-validate-%s", instanceName, suffix)
}
