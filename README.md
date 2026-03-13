# imagepod-executor

Executor service for [ImagePod](https://github.com/lonelyowl13/imagepod). Manages RunPod-compatible worker containers via Docker (DooD), with per-endpoint idle and execution timeouts.

The serverless endpoints are working.
Pods are a work in progress. 

## Quick start

- Set `IMAGEPOD_API_URL` and `IMAGEPOD_API_KEY` (e.g. in `.env`).
- Run with Docker Compose or see `Dockerfile` / `docker-compose.yml` for deployment.

## Configuration

See env vars documented at the top of `executor.py`.
