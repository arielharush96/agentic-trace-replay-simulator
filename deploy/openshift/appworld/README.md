# AppWorld Service Template

Run the AppWorld service outside the OpenShell sandbox so its cost can be
measured separately from sandbox overhead.

The service image must contain:

- The AppWorld package.
- AppWorld installed/encrypted code.
- AppWorld data.
- `scripts/appworld_service.py`.
- A session-to-task mapping file.

Do not commit AppWorld data or task mappings containing sensitive state. Build a
private image or mount them through cluster-approved storage.

The sandbox should call the service only through an internal Service address,
and a NetworkPolicy should restrict access to the benchmark namespace.
