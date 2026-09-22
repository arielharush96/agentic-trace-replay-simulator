# Security

Do not commit kubeconfigs, access tokens, private image credentials, internal
hostnames, or unredacted agent traces. Use the example configuration files and
inject secrets through OpenShift Secrets or environment variables.

Report security issues privately to the repository maintainers rather than
opening a public issue with credentials or private cluster data.

Before changing repository visibility, scan the complete Git history and every
remote branch for secrets and internal infrastructure identifiers. Removing a
file from the current tree does not remove its earlier Git objects; historical
cleanup requires an explicit history rewrite and coordinated force-push.
