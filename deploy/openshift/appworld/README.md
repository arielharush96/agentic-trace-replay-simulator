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
and a NetworkPolicy should restrict access to the benchmark namespace. Create
the `appworld-api-auth` Secret out of band and provide the same value to the
sandbox as `APPWORLD_API_TOKEN`; tokens must never be placed in the corpus or
this repository. The adapter rejects unauthenticated requests and limits
request bodies to 1 MiB.

Example Secret creation (run only in the dedicated benchmark namespace, with a
value supplied interactively or by a secret manager):

```bash
oc -n trace-replay create secret generic appworld-api-auth --from-literal=token="$APPWORLD_AUTH_TOKEN"
```
