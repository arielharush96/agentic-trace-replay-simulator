# OpenShift Manifest Templates

The files under `../base` are templates. Before applying them:

1. Replace placeholder image references with approved OpenClaw and OpenShell
   images.
2. Create the required Secrets from `openclaw-secrets.example.yaml`.
3. Configure the OpenShell TLS Secret and image-pull Secret.
4. Configure the OpenShift monitoring/Thanos endpoint for your cluster.
5. Review SCC requirements with the cluster administrator.

The repository intentionally does not install OpenShell or cluster-scoped
prerequisites.
