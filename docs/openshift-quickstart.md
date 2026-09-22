# OpenShift Quickstart

This project assumes OpenShell and Agent Sandbox are already installed by a
cluster administrator.

## Required Checks

```bash
oc whoami
oc get nodes
oc get crd | grep -E 'sandbox|agents'
helm list -A | grep -i openshell
```

Confirm that the OpenShell gateway, sandbox backend, OpenClaw image, Jaeger,
Prometheus/cAdvisor access, TLS Secret, image-pull Secret, and StorageClass are
available before running the benchmark.

The runner does not modify cluster monitoring by default. A cluster
administrator may explicitly opt in with `APPLY_MONITORING=1`; review
`deploy/openshift/base/50-monitoring.yaml` before doing so. The SCC under
`deploy/openshift/prerequisites/` is cluster-scoped and is intentionally not
included in the namespace-local base; apply it only through the cluster's
approved administrative process.

## Safety

- Use a dedicated namespace.
- Use an explicit node selector only when the cluster owner requires it.
- Do not modify node labels, taints, or cluster-wide workloads.
- Do not commit kubeconfigs, tokens, or private image references.
- Save the generated manifests and experiment metadata with the results.

## Cleanup

Review resources before deleting the benchmark namespace:

```bash
oc get all -n trace-replay
oc delete namespace trace-replay
```
