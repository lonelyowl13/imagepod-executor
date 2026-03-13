"""
Minimal persistence for executor state (endpoints, pods) across restarts.

Schema: { "endpoints": { "<id>": <payload> }, "pods": { "<id>": <payload> } }
File path from IMAGEPOD_STATE_FILE or default ./imagepod-executor-state.json
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger("imagepod-executor")

DEFAULT_STATE_FILE = "imagepod-executor-state.json"


class StateStore:
    def __init__(self, path: str | None = None) -> None:
        self._path = Path(
            path or os.environ.get("IMAGEPOD_STATE_FILE", DEFAULT_STATE_FILE)
        )
        self._endpoints: dict[str, dict[str, Any]] = {}
        self._pods: dict[str, dict[str, Any]] = {}

    def load(self) -> None:
        """Load state from disk. No-op if file missing or invalid."""
        if not self._path.exists():
            log.debug("No state file at %s, starting empty", self._path)
            return
        try:
            with open(self._path) as f:
                data = json.load(f)
            self._endpoints = {
                k: v for k, v in (data.get("endpoints") or {}).items() if isinstance(v, dict)
            }
            self._pods = {
                k: v for k, v in (data.get("pods") or {}).items() if isinstance(v, dict)
            }
            log.info(
                "Loaded state: %d endpoints, %d pods",
                len(self._endpoints),
                len(self._pods),
            )
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Failed to load state from %s: %s", self._path, exc)

    def save(self) -> None:
        """Write current endpoints and pods to disk."""
        data = {"endpoints": self._endpoints, "pods": self._pods}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(data, f, indent=2)
        except OSError as exc:
            log.warning("Failed to save state to %s: %s", self._path, exc)

    def get_endpoint(self, endpoint_id: int) -> dict[str, Any] | None:
        return self._endpoints.get(str(endpoint_id))

    def set_endpoint(self, endpoint_id: int, payload: dict[str, Any]) -> None:
        self._endpoints[str(endpoint_id)] = payload
        self.save()

    def remove_endpoint(self, endpoint_id: int) -> None:
        self._endpoints.pop(str(endpoint_id), None)
        self.save()

    def get_pod(self, pod_id: int) -> dict[str, Any] | None:
        return self._pods.get(str(pod_id))

    def set_pod(self, pod_id: int, payload: dict[str, Any]) -> None:
        self._pods[str(pod_id)] = payload
        self.save()

    def remove_pod(self, pod_id: int) -> None:
        self._pods.pop(str(pod_id), None)
        self.save()

    def all_endpoint_ids(self) -> list[int]:
        return [int(k) for k in self._endpoints if k.isdigit()]

    def all_pod_ids(self) -> list[int]:
        return [int(k) for k in self._pods if k.isdigit()]
