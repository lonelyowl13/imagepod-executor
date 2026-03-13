"""
ImagePod Executor -- Docker-outside-of-Docker (DooD) executor for ImagePod.

Manages RunPod-compatible worker containers via the Docker socket. Supports
per-endpoint idle timeouts (stopping idle workers) and per-endpoint execution
timeouts (marking stuck jobs as TIMED_OUT).

Configuration (env vars):
  IMAGEPOD_API_URL        (required) API base URL reachable from executor AND workers
  IMAGEPOD_API_KEY        (required) Executor API key (from /executors/add)
  IMAGEPOD_POLL_TIMEOUT   Long-poll timeout in seconds (default: 20)
  IMAGEPOD_STATE_FILE     Optional path to JSON state file (default: ./imagepod-executor-state.json)

Hardware (GPU, VRAM, CPU, RAM, CUDA version) is always auto-detected via
nvidia-smi and /proc.  The executor container must be started with GPU
access (e.g. deploy.resources.reservations.devices in Compose).
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Any

import docker
import docker.errors
import docker.types
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

from state_store import StateStore

log = logging.getLogger("imagepod-executor")

DEFAULT_EXECUTION_TIMEOUT_MS = 600_000
DEFAULT_IDLE_TIMEOUT = 5

# Default base URL that workers use to reach the executor's proxy.
DEFAULT_EXECUTOR_PROXY_URL = "http://executor:8080"


@dataclass(slots=True)
class TrackedJob:
    endpoint_id: int
    first_seen: float


@dataclass(slots=True)
class EndpointConfig:
    execution_timeout_ms: int = DEFAULT_EXECUTION_TIMEOUT_MS
    idle_timeout: int = DEFAULT_IDLE_TIMEOUT


class ImagePodExecutor:
    def __init__(self) -> None:
        self._api_url: str = os.environ["IMAGEPOD_API_URL"].rstrip("/")
        self._api_key: str = os.environ["IMAGEPOD_API_KEY"]
        self._poll_timeout: float = float(os.environ.get("IMAGEPOD_POLL_TIMEOUT", "20"))
        # Workers always run on a dedicated Docker network so they can reach the
        # executor proxy and are isolated from the host network.
        self._docker_network: str = "imagepod"
        self._proxy_url: str = os.environ.get(
            "IMAGEPOD_EXECUTOR_PROXY_URL", DEFAULT_EXECUTOR_PROXY_URL
        ).rstrip("/")

        self._http = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self._api_key}"},
            timeout=httpx.Timeout(self._poll_timeout + 10, connect=10),
        )
        self._docker = docker.from_env()

        self._workers: dict[int, docker.models.containers.Container] = {}
        self._pod_containers: dict[int, docker.models.containers.Container] = {}
        self._active_jobs: dict[int, TrackedJob] = {}
        self._last_activity: dict[int, float] = {}
        self._endpoint_configs: dict[int, EndpointConfig] = {}
        self._endpoint_data: dict[int, dict] = {}
        self._pod_data: dict[int, dict] = {}

        self._store = StateStore()

        # Per-pod tokens: key = str(endpoint_id) or str(pod_id) for proxy auth.
        self._pod_tokens: dict[str, str] = {}

        # Basic timing / metrics data.
        self._pod_started_at: dict[str, float] = {}
        self._pod_ready_at: dict[str, float] = {}
        self._job_assigned_at: dict[int, float] = {}
        self._job_completed_at: dict[int, float] = {}
        self._last_job_done_at: dict[str, float] = {}

        self._shutting_down = False

    # ------------------------------------------------------------------ #
    # Hardware detection
    # ------------------------------------------------------------------ #

    @staticmethod
    def _detect_hardware() -> dict:
        specs: dict = {}
        gpu: str | None = None
        vram: int | None = None

        try:
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=name,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                parts = result.stdout.strip().split("\n")[0].split(", ")
                gpu = parts[0].strip()
                if len(parts) > 1:
                    vram = int(float(parts[1].strip()) * 1024 * 1024)
        except (FileNotFoundError, subprocess.TimeoutExpired, ValueError, IndexError):
            pass

        if gpu:
            specs["gpu"] = gpu
        if vram:
            specs["vram"] = vram

        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        specs["cpu"] = line.split(":", 1)[1].strip()
                        break
        except OSError:
            pass

        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        specs["ram"] = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass

        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                specs["cuda_version"] = result.stdout.strip().split("\n")[0].strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        specs["compute_type"] = "GPU" if gpu else "CPU"
        return specs

    # ------------------------------------------------------------------ #
    # API helpers
    # ------------------------------------------------------------------ #

    async def _register(self) -> None:
        specs = self._detect_hardware()
        resp = await self._http.post(f"{self._api_url}/executors/register", json=specs)
        resp.raise_for_status()
        log.info("Registered with specs: %s", specs)

    async def _patch_job(self, job_id: int, body: dict) -> bool:
        try:
            resp = await self._http.patch(
                f"{self._api_url}/executors/job/{job_id}",
                json=body,
                timeout=10,
            )
            return resp.status_code == 200
        except httpx.HTTPError as exc:
            log.error("PATCH job %d failed: %s", job_id, exc)
            return False

    async def _patch_endpoint_status(self, endpoint_id: int, status: str) -> None:
        try:
            resp = await self._http.patch(
                f"{self._api_url}/executors/endpoints/{endpoint_id}",
                json={"status": status},
                timeout=10,
            )
            if resp.status_code == 200:
                log.info("Endpoint %d -> %s", endpoint_id, status)
            else:
                log.warning(
                    "Failed to set endpoint %d to %s: %d %s",
                    endpoint_id, status, resp.status_code, resp.text,
                )
        except httpx.HTTPError as exc:
            log.error("PATCH endpoint %d failed: %s", endpoint_id, exc)

    async def _ack_notifications(self, notification_ids: list[int]) -> None:
        if not notification_ids:
            return
        try:
            resp = await self._http.post(
                f"{self._api_url}/executors/updates",
                json={"notification_ids": notification_ids},
                timeout=10,
            )
            resp.raise_for_status()
            log.debug("Acknowledged %d notification(s)", len(notification_ids))
        except httpx.HTTPError as exc:
            log.error("Failed to ack notifications %s: %s", notification_ids, exc)

    # ------------------------------------------------------------------ #
    # Docker container management
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_cmd(cmd: str | list[str] | None) -> list[str] | None:
        """Ensure command/entrypoint is in exec form (list)."""
        if cmd is None:
            return None
        if isinstance(cmd, str):
            return cmd.split()
        if isinstance(cmd, list):
            if not cmd:
                return None
            # Handle ['python -u /handler.py'] → ['python', '-u', '/handler.py']
            if len(cmd) == 1 and isinstance(cmd[0], str) and " " in cmd[0]:
                return cmd[0].split()
            return list(cmd)
        return None

    def _build_volume_mounts(self, entity: dict) -> list[docker.types.Mount]:
        """Build Docker named-volume mounts from endpoint or pod payload (volumes list)."""
        volumes = entity.get("volumes") or []
        mounts: list[docker.types.Mount] = []
        for vol in volumes:
            volume_id = vol.get("volume_id")
            name = vol.get("volume_name") or vol.get("name") or ""
            mount_path = vol.get("mount_path", "/runpod-volume")
            if volume_id is None:
                continue
            source = f"imagepod-volume-{name}-{volume_id}".strip("-")
            mounts.append(
                docker.types.Mount(target=mount_path, source=source, type="volume")
            )
            log.debug("Volume %s (%s): %s -> %s", volume_id, name, source, mount_path)
        return mounts

    def _build_runpod_env(self, entity: dict, pod_id_str: str, pod_token: str) -> dict[str, str]:
        """Build RunPod env for endpoint or pod. entity has template, env; pod_id_str is RUNPOD_POD_ID."""
        template = entity.get("template") or {}
        template_env = template.get("env") or {}
        entity_env = entity.get("env") or {}
        env: dict[str, str] = {}
        env.update({k: str(v) for k, v in template_env.items()})
        env.update({k: str(v) for k, v in entity_env.items()})
        base = self._proxy_url
        env.update(
            {
                "RUNPOD_POD_ID": pod_id_str,
                "RUNPOD_AI_API_KEY": f"Bearer {pod_token}",
                "RUNPOD_EXECUTOR_TOKEN": pod_token,
                "RUNPOD_WEBHOOK_GET_JOB": (
                    f"{base}/runpod/job-take/$ID"
                    f"?source=imagepod&pod_id={pod_id_str}&token={pod_token}"
                ),
                "RUNPOD_WEBHOOK_POST_OUTPUT": (
                    f"{base}/runpod/job-done/{pod_id_str}"
                    f"?job_id=$ID&token={pod_token}"
                ),
                "RUNPOD_WEBHOOK_POST_STREAM": (
                    f"{base}/runpod/job-stream/{pod_id_str}"
                    f"?job_id=$ID&token={pod_token}"
                ),
                "RUNPOD_WEBHOOK_PING": (
                    f"{base}/runpod/ping/{pod_id_str}"
                    f"?token={pod_token}"
                ),
            }
        )
        return env

    def _is_worker_running(self, endpoint_id: int) -> bool:
        container = self._workers.get(endpoint_id)
        if container is None:
            return False
        try:
            container.reload()
            return container.status == "running"
        except docker.errors.NotFound:
            self._workers.pop(endpoint_id, None)
            return False

    def _start_worker(self, endpoint: dict) -> bool:
        """Start worker container for endpoint. Returns True if running, False if deployment failed."""
        endpoint_id = endpoint["id"]
        pod_id = str(endpoint_id)

        if self._is_worker_running(endpoint_id):
            return True

        self._workers.pop(endpoint_id, None)

        # Generate a per-pod token and record pod start time for metrics.
        pod_token = os.urandom(32).hex()
        self._pod_tokens[pod_id] = pod_token
        self._pod_started_at[pod_id] = time.monotonic()

        template = endpoint.get("template") or {}
        image = template.get("image_name")
        if not image:
            log.warning("Endpoint %d: no template.image_name, cannot start worker", endpoint_id)
            return False

        entrypoint = self._normalize_cmd(template.get("docker_entrypoint"))
        command = self._normalize_cmd(template.get("docker_start_cmd"))

        env = self._build_runpod_env(endpoint, str(endpoint_id), pod_token)
        mounts = self._build_volume_mounts(endpoint)
        container_name = f"imagepod-worker-{endpoint_id}"

        try:
            stale = self._docker.containers.get(container_name)
            stale.remove(force=True)
        except docker.errors.NotFound:
            pass

        run_kw: dict = {
            "image": image,
            "command": command,
            "entrypoint": entrypoint,
            "detach": True,
            "auto_remove": True,
            "name": container_name,
            "environment": env,
            "mounts": mounts or None,
            "device_requests": [
                docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]]),
            ],
        }
        # Always attach workers to the dedicated Docker network so they can
        # reach the executor proxy and remain isolated from the host network.
        run_kw["network"] = self._docker_network
        try:
            container = self._docker.containers.run(**run_kw)
            self._workers[endpoint_id] = container
            vol_desc = ""
            if mounts:
                vol_desc = f", volumes={len(mounts)}"
            log.info("Started worker %s (image=%s%s)", container_name, image, vol_desc)
            return True
        except docker.errors.APIError as exc:
            log.error("Failed to start worker for endpoint %d: %s", endpoint_id, exc)
            return False

    def _stop_worker(self, endpoint_id: int) -> None:
        container = self._workers.pop(endpoint_id, None)
        if container is None:
            return
        name = container.name
        pod_id = str(endpoint_id)
        self._pod_tokens.pop(pod_id, None)
        self._pod_started_at.pop(pod_id, None)
        self._pod_ready_at.pop(pod_id, None)
        self._last_job_done_at.pop(pod_id, None)
        try:
            container.stop(timeout=10)
            log.info("Stopped worker %s (endpoint %d)", name, endpoint_id)
        except docker.errors.NotFound:
            log.debug("Worker for endpoint %d already removed", endpoint_id)
        except docker.errors.APIError as exc:
            log.warning("Error stopping worker for endpoint %d: %s", endpoint_id, exc)

    def _is_pod_container_running(self, pod_id: int) -> bool:
        container = self._pod_containers.get(pod_id)
        if container is None:
            return False
        try:
            container.reload()
            return container.status == "running"
        except docker.errors.NotFound:
            self._pod_containers.pop(pod_id, None)
            return False

    def _start_pod_container(self, pod: dict) -> bool:
        """Start container for pod. Returns True if running."""
        pod_id = pod["id"]
        pod_id_str = str(pod_id)
        if self._is_pod_container_running(pod_id):
            return True
        self._pod_containers.pop(pod_id, None)
        pod_token = os.urandom(32).hex()
        self._pod_tokens[pod_id_str] = pod_token
        self._pod_started_at[pod_id_str] = time.monotonic()
        template = pod.get("template") or {}
        image = template.get("image_name")
        if not image:
            log.warning("Pod %d: no template.image_name, cannot start", pod_id)
            return False
        entrypoint = self._normalize_cmd(template.get("docker_entrypoint"))
        command = self._normalize_cmd(template.get("docker_start_cmd"))
        env = self._build_runpod_env(pod, pod_id_str, pod_token)
        mounts = self._build_volume_mounts(pod)
        ports_cfg = pod.get("ports") or []
        ports: dict[str, int | None] | None = None
        if isinstance(ports_cfg, list) and ports_cfg:
            # Expose each TCP port on the host with an auto-assigned host port.
            ports = {}
            for p in ports_cfg:
                try:
                    port_int = int(p)
                except (TypeError, ValueError):
                    continue
                ports[f"{port_int}/tcp"] = None
        container_name = f"imagepod-pod-{pod_id}"
        try:
            stale = self._docker.containers.get(container_name)
            stale.remove(force=True)
        except docker.errors.NotFound:
            pass
        run_kw: dict = {
            "image": image,
            "command": command,
            "entrypoint": entrypoint,
            "detach": True,
            "auto_remove": True,
            "name": container_name,
            "environment": env,
            "mounts": mounts or None,
            "ports": ports,
            "device_requests": [
                docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]]),
            ],
            "network": self._docker_network,
        }
        try:
            container = self._docker.containers.run(**run_kw)
            self._pod_containers[pod_id] = container
            log.info("Started pod container %s (image=%s)", container_name, image)
            return True
        except docker.errors.APIError as exc:
            log.error("Failed to start pod %d: %s", pod_id, exc)
            self._pod_tokens.pop(pod_id_str, None)
            self._pod_started_at.pop(pod_id_str, None)
            return False

    def _stop_pod_container(self, pod_id: int) -> None:
        """Stop pod container; keep pod config for later start."""
        container = self._pod_containers.pop(pod_id, None)
        if container is None:
            return
        pod_id_str = str(pod_id)
        self._pod_tokens.pop(pod_id_str, None)
        self._pod_started_at.pop(pod_id_str, None)
        self._pod_ready_at.pop(pod_id_str, None)
        self._last_job_done_at.pop(pod_id_str, None)
        try:
            container.stop(timeout=10)
            log.info("Stopped pod container %s (pod %d)", container.name, pod_id)
        except docker.errors.NotFound:
            log.debug("Pod %d container already removed", pod_id)
        except docker.errors.APIError as exc:
            log.warning("Error stopping pod %d: %s", pod_id, exc)

    def _terminate_pod(self, pod_id: int) -> None:
        """Stop container and remove pod from config and persistence."""
        self._stop_pod_container(pod_id)
        self._pod_data.pop(pod_id, None)
        self._store.remove_pod(pod_id)

    # ------------------------------------------------------------------ #
    # Notification handlers
    # ------------------------------------------------------------------ #

    def _apply_endpoint_config(self, payload: dict) -> None:
        """Update in-memory endpoint config and persistence from payload."""
        eid = payload.get("id")
        if eid is None:
            return
        self._endpoint_data[eid] = payload
        self._endpoint_configs[eid] = EndpointConfig(
            execution_timeout_ms=payload.get("execution_timeout_ms", DEFAULT_EXECUTION_TIMEOUT_MS),
            idle_timeout=payload.get("idle_timeout", DEFAULT_IDLE_TIMEOUT),
        )
        self._store.set_endpoint(eid, payload)

    def _handle_job_changed(self, payload: dict) -> bool:
        job_id = payload.get("id")
        endpoint_id = payload.get("endpoint_id")
        status = (payload.get("status") or "").upper()
        if job_id is None or endpoint_id is None:
            return True
        now = time.monotonic()
        self._last_activity[endpoint_id] = now
        terminal = status in ("COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT")
        if terminal:
            self._active_jobs.pop(job_id, None)
            return True
        if status in ("IN_QUEUE", "RUNNING", ""):
            if job_id not in self._active_jobs:
                self._active_jobs[job_id] = TrackedJob(endpoint_id=endpoint_id, first_seen=now)
                log.info("Tracking job %d (endpoint %d)", job_id, endpoint_id)
            ep = self._endpoint_data.get(endpoint_id) or self._store.get_endpoint(endpoint_id)
            if ep and self._start_worker(ep):
                return True
            if ep:
                try:
                    asyncio.get_running_loop().create_task(
                        self._patch_endpoint_status(endpoint_id, "UNHEALTHY")
                    )
                except RuntimeError:
                    pass
            else:
                log.warning("Job %d references unknown endpoint %d", job_id, endpoint_id)
        return True

    def _handle_endpoint_changed(self, payload: dict) -> bool:
        self._apply_endpoint_config(payload)
        endpoint_id = payload.get("id")
        if endpoint_id is None:
            return True
        status = (payload.get("status") or "").upper()
        if status not in ("DEPLOYING", "READY"):
            return True
        if self._is_worker_running(endpoint_id):
            self._stop_worker(endpoint_id)
        if self._start_worker(payload):
            try:
                asyncio.get_running_loop().create_task(
                    self._patch_endpoint_status(endpoint_id, "READY")
                )
            except RuntimeError:
                pass
        else:
            try:
                asyncio.get_running_loop().create_task(
                    self._patch_endpoint_status(endpoint_id, "UNHEALTHY")
                )
            except RuntimeError:
                pass
        return True

    def _handle_endpoint_deleted(self, entity_id: int) -> bool:
        if entity_id is None:
            return True
        self._stop_worker(entity_id)
        self._endpoint_data.pop(entity_id, None)
        self._endpoint_configs.pop(entity_id, None)
        self._store.remove_endpoint(entity_id)
        return True

    def _handle_pod_status_changed(self, payload: dict) -> bool:
        pod_id = payload.get("id")
        if pod_id is None:
            return True
        self._pod_data[pod_id] = payload
        self._store.set_pod(pod_id, payload)
        status = (payload.get("status") or "").upper()
        if status in ("RUNNING", "STARTING", ""):
            if self._is_pod_container_running(pod_id):
                return True
            self._start_pod_container(payload)
        else:
            if status in ("STOPPED", "STOPPING", "EXITED"):
                self._stop_pod_container(pod_id)
        return True

    def _handle_pod_terminated(self, entity_id: int) -> bool:
        if entity_id is None:
            return True
        self._terminate_pod(entity_id)
        return True

    def _volume_refs_for_volume_id(self, volume_id: int) -> tuple[list[int], list[int]]:
        """Return (endpoint_ids, pod_ids) that reference this volume_id."""
        ep_ids: list[int] = []
        for eid, ep in self._endpoint_data.items():
            for vol in ep.get("volumes") or []:
                if vol.get("volume_id") == volume_id:
                    ep_ids.append(eid)
                    break
        pod_ids: list[int] = []
        for pid, pod in self._pod_data.items():
            for vol in pod.get("volumes") or []:
                if vol.get("volume_id") == volume_id:
                    pod_ids.append(pid)
                    break
        return (ep_ids, pod_ids)

    def _handle_volume_changed_or_deleted(
        self, entity_id: int, payload: dict | None, is_deleted: bool
    ) -> bool:
        volume_id = entity_id
        if volume_id is None and payload:
            volume_id = payload.get("id")
        if volume_id is None:
            return True
        ep_ids, pod_ids = self._volume_refs_for_volume_id(volume_id)
        if is_deleted:
            for eid in ep_ids:
                ep = self._endpoint_data.get(eid) or self._store.get_endpoint(eid)
                if ep:
                    ep = dict(ep)
                    ep["volumes"] = [v for v in (ep.get("volumes") or []) if v.get("volume_id") != volume_id]
                    self._apply_endpoint_config(ep)
                    if self._is_worker_running(eid):
                        self._stop_worker(eid)
                        self._start_worker(self._endpoint_data[eid])
            for pid in pod_ids:
                pod = self._pod_data.get(pid) or self._store.get_pod(pid)
                if pod:
                    pod = dict(pod)
                    pod["volumes"] = [v for v in (pod.get("volumes") or []) if v.get("volume_id") != volume_id]
                    self._pod_data[pid] = pod
                    self._store.set_pod(pid, pod)
                    if self._is_pod_container_running(pid):
                        self._stop_pod_container(pid)
                        self._start_pod_container(self._pod_data[pid])
        else:
            for eid in ep_ids:
                ep = self._endpoint_data.get(eid) or self._store.get_endpoint(eid)
                if ep and self._is_worker_running(eid):
                    self._stop_worker(eid)
                    self._start_worker(self._endpoint_data[eid])
            for pid in pod_ids:
                pod = self._pod_data.get(pid) or self._store.get_pod(pid)
                if pod and self._is_pod_container_running(pid):
                    self._stop_pod_container(pid)
                    self._start_pod_container(self._pod_data[pid])
        return True

    def _handle_volume_mounted(self, payload: dict) -> bool:
        endpoint_id = payload.get("endpoint_id")
        if endpoint_id is None:
            return True
        vol = payload.get("volume")
        mount_path = payload.get("mount_path") or "/runpod-volume"
        if not vol:
            return True
        ep = self._endpoint_data.get(endpoint_id) or self._store.get_endpoint(endpoint_id)
        if not ep:
            return True
        ep = dict(ep)
        volumes = list(ep.get("volumes") or [])
        vid = vol.get("id")
        name = vol.get("name") or ""
        volumes = [v for v in volumes if v.get("volume_id") != vid]
        volumes.append({"volume_id": vid, "name": name, "mount_path": mount_path})
        ep["volumes"] = volumes
        self._apply_endpoint_config(ep)
        if self._is_worker_running(endpoint_id):
            self._stop_worker(endpoint_id)
            self._start_worker(self._endpoint_data[endpoint_id])
        return True

    def _handle_volume_unmounted(self, payload: dict) -> bool:
        endpoint_id = payload.get("endpoint_id")
        volume_id = payload.get("volume_id")
        if endpoint_id is None:
            return True
        ep = self._endpoint_data.get(endpoint_id) or self._store.get_endpoint(endpoint_id)
        if not ep:
            return True
        ep = dict(ep)
        ep["volumes"] = [v for v in (ep.get("volumes") or []) if v.get("volume_id") != volume_id]
        self._apply_endpoint_config(ep)
        if self._is_worker_running(endpoint_id):
            self._stop_worker(endpoint_id)
            self._start_worker(self._endpoint_data[endpoint_id])
        return True

    def _dispatch_notification(self, n: dict) -> bool:
        """Process one notification. Return True to ack."""
        ntype = n.get("type") or ""
        entity_kind = n.get("entity_kind") or ""
        entity_id = n.get("entity_id")
        payload = n.get("payload") or {}
        try:
            if entity_kind == "JOB" and ntype == "JOB_CHANGED":
                return self._handle_job_changed(payload)
            if entity_kind == "ENDPOINT" and ntype == "ENDPOINT_CHANGED":
                return self._handle_endpoint_changed(payload)
            if entity_kind == "ENDPOINT" and ntype == "ENDPOINT_DELETED":
                return self._handle_endpoint_deleted(entity_id)
            if entity_kind == "POD" and ntype == "POD_STATUS_CHANGED":
                return self._handle_pod_status_changed(payload)
            if entity_kind == "POD" and ntype == "POD_TERMINATED":
                return self._handle_pod_terminated(entity_id)
            if entity_kind == "VOLUME" and ntype == "VOLUME_CHANGED":
                return self._handle_volume_changed_or_deleted(entity_id, payload, is_deleted=False)
            if entity_kind == "VOLUME" and ntype == "VOLUME_DELETED":
                return self._handle_volume_changed_or_deleted(entity_id, payload, is_deleted=True)
            if ntype == "VOLUME_MOUNTED":
                return self._handle_volume_mounted(payload)
            if ntype == "VOLUME_UNMOUNTED":
                return self._handle_volume_unmounted(payload)
        except Exception as exc:
            log.exception("Handler error for notification %s: %s", n.get("id"), exc)
        return True

    # ------------------------------------------------------------------ #
    # Core loops
    # ------------------------------------------------------------------ #

    async def _poll_loop(self) -> None:
        while not self._shutting_down:
            try:
                resp = await self._http.get(
                    f"{self._api_url}/executors/updates",
                    params={"timeout": self._poll_timeout},
                    timeout=httpx.Timeout(self._poll_timeout + 10, connect=10),
                )
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPError as exc:
                if self._shutting_down:
                    return
                log.error("Poll error: %s", exc)
                await asyncio.sleep(5)
                continue

            notifications = data.get("notifications") or []
            ack_ids: list[int] = []
            for n in notifications:
                nid = n.get("id")
                if isinstance(nid, int) and self._dispatch_notification(n):
                    ack_ids.append(nid)
            if ack_ids:
                await self._ack_notifications(ack_ids)

    async def _timeout_monitor(self) -> None:
        """Periodically check tracked jobs and mark those exceeding execution_timeout_ms."""
        while not self._shutting_down:
            await asyncio.sleep(5)
            now = time.monotonic()
            timed_out: list[int] = []

            for job_id, tracked in self._active_jobs.items():
                cfg = self._endpoint_configs.get(tracked.endpoint_id, EndpointConfig())
                elapsed_ms = (now - tracked.first_seen) * 1000
                if elapsed_ms > cfg.execution_timeout_ms:
                    timed_out.append(job_id)

            for job_id in timed_out:
                tracked = self._active_jobs.pop(job_id, None)
                if tracked is None:
                    continue
                log.warning(
                    "Job %d exceeded execution timeout (%d ms), marking TIMED_OUT",
                    job_id,
                    self._endpoint_configs.get(tracked.endpoint_id, EndpointConfig()).execution_timeout_ms,
                )
                await self._patch_job(job_id, {"status": "TIMED_OUT"})

    async def _idle_monitor(self) -> None:
        """Stop worker containers that have been idle longer than their endpoint's idle_timeout."""
        while not self._shutting_down:
            await asyncio.sleep(5)
            now = time.monotonic()
            to_stop: list[int] = []

            active_endpoint_ids = {t.endpoint_id for t in self._active_jobs.values()}

            for endpoint_id in list(self._workers):
                if endpoint_id in active_endpoint_ids:
                    continue
                last = self._last_activity.get(endpoint_id, now)
                cfg = self._endpoint_configs.get(endpoint_id, EndpointConfig())
                if now - last > cfg.idle_timeout:
                    to_stop.append(endpoint_id)

            for endpoint_id in to_stop:
                log.info("Endpoint %d idle, stopping worker", endpoint_id)
                self._stop_worker(endpoint_id)

    # ------------------------------------------------------------------ #
    # Metrics helpers
    # ------------------------------------------------------------------ #

    def _record_pod_ready(self, pod_id: str) -> None:
        if pod_id not in self._pod_started_at:
            return
        if pod_id in self._pod_ready_at:
            return
        now = time.monotonic()
        self._pod_ready_at[pod_id] = now
        startup_s = now - self._pod_started_at[pod_id]
        log.info("Pod %s ready (startup %.3fs)", pod_id, startup_s)

    def _record_job_assigned(self, pod_id: str, job_id: int) -> None:
        now = time.monotonic()
        self._job_assigned_at[job_id] = now
        log.info("Assigned job %d to pod %s", job_id, pod_id)

    def _record_job_completed(self, pod_id: str, job_id: int) -> None:
        now = time.monotonic()
        assigned = self._job_assigned_at.pop(job_id, None)
        self._job_completed_at[job_id] = now
        self._last_job_done_at[pod_id] = now
        if assigned is not None:
            runtime_s = now - assigned
            log.info("Job %d completed on pod %s (runtime %.3fs)", job_id, pod_id, runtime_s)
        else:
            log.info("Job %d completed on pod %s", job_id, pod_id)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def _shutdown(self) -> None:
        if self._shutting_down:
            return
        log.info("Shutting down ...")
        self._shutting_down = True
        for endpoint_id in list(self._workers):
            self._stop_worker(endpoint_id)
        for pod_id in list(self._pod_containers):
            self._stop_pod_container(pod_id)
        await self._http.aclose()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(self._shutdown()))

        self._store.load()
        for eid in self._store.all_endpoint_ids():
            ep = self._store.get_endpoint(eid)
            if ep:
                self._endpoint_data[eid] = ep
                self._endpoint_configs[eid] = EndpointConfig(
                    execution_timeout_ms=ep.get("execution_timeout_ms", DEFAULT_EXECUTION_TIMEOUT_MS),
                    idle_timeout=ep.get("idle_timeout", DEFAULT_IDLE_TIMEOUT),
                )
        for pid in self._store.all_pod_ids():
            pod = self._store.get_pod(pid)
            if pod:
                self._pod_data[pid] = pod

        await self._register()

        log.info(
            "Executor running  (api=%s  poll=%.0fs  network=%s)",
            self._api_url,
            self._poll_timeout,
            self._docker_network,
        )

        try:
            await asyncio.gather(
                self._poll_loop(),
                self._timeout_monitor(),
                self._idle_monitor(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()


app = FastAPI()

_EXECUTOR: ImagePodExecutor | None = None


def get_executor() -> ImagePodExecutor:
    if _EXECUTOR is None:
        raise RuntimeError("Executor has not been initialized")
    return _EXECUTOR


async def _forward_to_backend(
    request: Request,
    method: str,
    backend_path: str,
    *,
    pod_id: str,
) -> Response:
    executor = get_executor()

    # Validate pod token from query parameter.
    query = dict(request.query_params)
    token = query.get("token")
    expected = executor._pod_tokens.get(pod_id)

    if not expected or token != expected:
        raise HTTPException(status_code=403, detail="Invalid pod token")

    # Record metrics around job assignment / completion where applicable.
    if backend_path.startswith("/runpod/job-take/"):
        job_id_str = backend_path.rsplit("/", 1)[-1]
        try:
            job_id = int(job_id_str)
        except ValueError:
            job_id = -1
        executor._record_pod_ready(pod_id)
        if job_id != -1:
            executor._record_job_assigned(pod_id, job_id)
    elif backend_path.startswith("/runpod/job-done/"):
        job_id_str = query.get("job_id")
        if job_id_str is not None:
            try:
                job_id = int(job_id_str)
            except ValueError:
                job_id = -1
            if job_id != -1:
                executor._record_job_completed(pod_id, job_id)

    url = f"{executor._api_url}{backend_path}"

    body: bytes | None = None
    if method in ("POST", "PUT", "PATCH"):
        body = await request.body()

    try:
        # Do not forward the worker's Authorization header upstream; the executor's
        # httpx client already carries the correct bearer token for the backend.
        fwd_headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in ("host", "authorization")
        }

        resp = await executor._http.request(
            method,
            url,
            params=request.query_params,
            content=body,
            headers=fwd_headers,
        )
    except httpx.HTTPError as exc:
        log.error("Proxy error %s %s: %s", method, url, exc)
        raise HTTPException(status_code=502, detail="Upstream error") from exc

    content_type = resp.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            data: Any = resp.json()
        except ValueError:
            data = resp.text
        return JSONResponse(status_code=resp.status_code, content=data)

    return PlainTextResponse(
        status_code=resp.status_code,
        content=resp.content,
        media_type=content_type or None,
    )


@app.get("/runpod/job-take/{job_id}")
async def proxy_job_take(job_id: int, request: Request) -> Response:
    pod_id = request.query_params.get("pod_id")
    if not pod_id:
        raise HTTPException(status_code=400, detail="Missing pod_id")
    backend_path = f"/runpod/job-take/{job_id}"
    return await _forward_to_backend(request, "GET", backend_path, pod_id=pod_id)


@app.post("/runpod/job-done/{pod_id}")
async def proxy_job_done(pod_id: str, request: Request) -> Response:
    backend_path = f"/runpod/job-done/{pod_id}"
    return await _forward_to_backend(request, "POST", backend_path, pod_id=pod_id)


@app.post("/runpod/job-stream/{pod_id}")
async def proxy_job_stream(pod_id: str, request: Request) -> Response:
    backend_path = f"/runpod/job-stream/{pod_id}"
    return await _forward_to_backend(request, "POST", backend_path, pod_id=pod_id)


@app.post("/runpod/ping/{pod_id}")
async def proxy_ping(pod_id: str, request: Request) -> Response:
    backend_path = f"/runpod/ping/{pod_id}"
    return await _forward_to_backend(request, "POST", backend_path, pod_id=pod_id)


async def _main_async() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    missing = [v for v in ("IMAGEPOD_API_URL", "IMAGEPOD_API_KEY") if v not in os.environ]
    if missing:
        log.critical("Missing required env var(s): %s", ", ".join(missing))
        raise SystemExit(1)

    global _EXECUTOR
    _EXECUTOR = ImagePodExecutor()

    # Configure and run uvicorn server alongside the executor core loops.
    host = os.environ.get("IMAGEPOD_EXECUTOR_PROXY_HOST", "0.0.0.0")
    port = int(os.environ.get("IMAGEPOD_EXECUTOR_PROXY_PORT", "8080"))

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)

    await asyncio.gather(
        _EXECUTOR.run(),
        server.serve(),
    )


def main() -> None:
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
