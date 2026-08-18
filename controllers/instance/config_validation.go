package instance

import (
	"context"
	"crypto/sha256"
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
	client client.Client
	scheme *runtime.Scheme
}

// NewKubernetesConfigValidator creates a validator that runs haproxy -c in a short-lived job.
func NewKubernetesConfigValidator(client client.Client, scheme *runtime.Scheme) *KubernetesConfigValidator {
	return &KubernetesConfigValidator{client: client, scheme: scheme}
}

// Validate validates the generated configuration and returns an error if the haproxy -c check fails.
func (v *KubernetesConfigValidator) Validate(ctx context.Context, req ConfigValidationRequest) error {
	if req.Instance == nil {
		return fmt.Errorf("config validation requires instance")
	}

	hash := validationDataHash(req.Data)
	jobName := validationResourceName(req.Instance.Name, hash)
	secretName := validationResourceName(req.Instance.Name+"-secret", hash)

	existingJob := &batchv1.Job{}
	err := v.client.Get(ctx, client.ObjectKey{Name: jobName, Namespace: req.Instance.Namespace}, existingJob)
	if err == nil {
		return v.waitForValidationJob(ctx, req.Instance.Namespace, jobName, secretName)
	}
	if err != nil && !k8serrors.IsNotFound(err) {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: req.Instance.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, v.client, secret, func() error {
		if err := controllerutil.SetOwnerReference(req.Instance, secret, v.scheme); err != nil {
			return err
		}

		secret.Data = req.Data
		return nil
	})
	if err != nil {
		return err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: req.Instance.Namespace,
			Labels: map[string]string{
				"proxy.haproxy.com/instance": req.Instance.Name,
				"proxy.haproxy.com/validate": "true",
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   ptr.To(configValidationActiveDeadlineSeconds),
			TTLSecondsAfterFinished: ptr.To(configValidationTTLSeconds),
			BackoffLimit:            ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "haproxy-config-check",
							Image:           req.Instance.Spec.Image,
							ImagePullPolicy: req.Instance.Spec.ImagePullPolicy,
							// Discard haproxy -c stdout/stderr so config detail never reaches the pod log; only the exit code is kept.
							Command: []string{"sh", "-c"},
							Args:    []string{fmt.Sprintf("haproxy -c -f %s >/dev/null 2>&1", haproxy.DefaultConfigurationFile)},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "haproxy-config",
									MountPath: filepath.Dir(haproxy.DefaultConfigurationFile),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "haproxy-config",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{SecretName: secretName},
							},
						},
					},
				},
			},
		},
	}

	if job.Spec.Template.Spec.Containers[0].Image == "" {
		job.Spec.Template.Spec.Containers[0].Image = "haproxy:latest"
	}

	if err := controllerutil.SetOwnerReference(req.Instance, job, v.scheme); err != nil {
		return err
	}

	if err := v.client.Create(ctx, job); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return err
		}
	}

	return v.waitForValidationJob(ctx, req.Instance.Namespace, jobName, secretName)
}

func (v *KubernetesConfigValidator) waitForValidationJob(ctx context.Context, namespace, jobName, secretName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, configValidationTimeout)
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
			return fmt.Errorf("haproxy config validation timed out: %w", waitCtx.Err())
		}
		return pollErr
	}

	return nil
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
