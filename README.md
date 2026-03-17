# imagepod-executor

Executor service for [ImagePod](https://github.com/lonelyowl13/imagepod). Manages RunPod-compatible worker containers via Docker (DooD), with per-endpoint idle and execution timeouts.

The serverless endpoints are working.
Pods are a work in progress. 

## Quick start

- Set `IMAGEPOD_API_URL` and `IMAGEPOD_API_KEY` (e.g. in `.env`).
- Run with Docker Compose or see `Dockerfile` / `docker-compose.yml` for deployment.

## Configuration

See env vars documented at the top of `executor.py`.

### FRP pod tunnels

Pods with `tunnels` configured in the backend will cause the executor to start
an `frpc` sidecar container per pod. The executor:

- Reads `IMAGEPOD_FRP_SERVER_ADDR` (required) as the FRP server address.
- Always uses FRP control port `7000`.
- Configures each tunnel as an HTTP proxy in frp: HTTP and WebSocket traffic to the tunnel's domain is forwarded via frps `vhost_http_port` to the pod container's port.
- Sends FRP login metadata `executor_id` and `executor_key` to the backend
  `app/api/frp.py` handler via the frps HTTP plugin.

Make sure:

- `IMAGEPOD_DOCKER_NETWORK` is set to the Docker network name shared with frps (e.g. `imagepod_internal`). All worker and frpc sidecar containers are attached to this network so they can reach the executor and frps.
- `IMAGEPOD_FRP_SERVER_ADDR` is the hostname of the frps instance **as resolvable from the frpc sidecar** (e.g. `frp` if the frps service is named `frp` on that network).
- The executor's `IMAGEPOD_API_KEY` matches the executor record created via
  `/executors/add` in the backend.

For now, pods are expected to serve only HTTP and WebSocket traffic through these tunnels. If you later need to expose arbitrary TCP services (e.g. databases, SSH), you can extend the configuration to use `tcpmux` or plain TCP proxies instead of HTTP-only proxies.

Basic verification:

- Start a pod with one or more tunnel ports configured.
- Observe that a pod container and an `frpc` sidecar container are created on
  the executor host.
- Access the generated tunnel domain(s) from the backend pod payload; traffic
  should be forwarded through frps to the pod ports.
