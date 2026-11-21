"""
msc_lease.py — mongo-stream-cdc
Kubernetes leader election via coordination.k8s.io/v1 Lease API.

Usage:
    lease = KubernetesLease(name="msc-stream-leader", namespace="mongo-stream-cdc")
    with lease:
        # only leader enters here
        watch_all_collections(...)
"""

import logging
import os
import socket
import time
from datetime import datetime, timezone, timedelta

from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException

log = logging.getLogger("msc_lease")

# ── Defaults (override via env) ───────────────────────────────────
LEASE_DURATION_S  = int(os.environ.get("LEASE_DURATION_S",  "30"))   # lease TTL
RENEW_INTERVAL_S  = int(os.environ.get("RENEW_INTERVAL_S",  "10"))   # how often leader renews
RETRY_INTERVAL_S  = int(os.environ.get("RETRY_INTERVAL_S",  "5"))    # standby poll interval


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _expired(acquired_time_str: str, duration_s: int) -> bool:
    try:
        acquired = datetime.fromisoformat(acquired_time_str.replace("Z", "+00:00"))
        return (_now() - acquired).total_seconds() > duration_s
    except Exception:
        return True


class KubernetesLease:
    """
    Distributed leader election using K8s Lease object.

    - Leader holds the lease and renews it every RENEW_INTERVAL_S seconds.
    - Standby polls every RETRY_INTERVAL_S seconds.
    - If lease expires (leader died), standby acquires it and becomes leader.
    - Identity = POD_NAME env var (injected by K8s downward API).
    """

    def __init__(
        self,
        name: str,
        namespace: str,
        lease_duration_s: int = LEASE_DURATION_S,
        renew_interval_s: int = RENEW_INTERVAL_S,
        retry_interval_s: int = RETRY_INTERVAL_S,
    ):
        self.name             = name
        self.namespace        = namespace
        self.lease_duration_s = lease_duration_s
        self.renew_interval_s = renew_interval_s
        self.retry_interval_s = retry_interval_s
        self.identity         = os.environ.get("POD_NAME", socket.gethostname())
        self._coord           = None   # CoordinationV1Api — lazy init
        self._is_leader       = False
        self._renew_thread    = None

    # ── K8s client ────────────────────────────────────────────────

    def _api(self):
        if self._coord is None:
            try:
                k8s_config.load_incluster_config()   # inside pod
            except Exception:
                k8s_config.load_kube_config()        # local dev
            self._coord = client.CoordinationV1Api()
        return self._coord

    # ── Lease CRUD ────────────────────────────────────────────────

    def _get_lease(self):
        try:
            return self._api().read_namespaced_lease(self.name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _create_lease(self):
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=self.name,
                namespace=self.namespace,
            ),
            spec=client.V1LeaseSpec(
                holder_identity=self.identity,
                lease_duration_seconds=self.lease_duration_s,
                acquire_time=_now_str(),
                renew_time=_now_str(),
                lease_transitions=0,
            ),
        )
        try:
            self._api().create_namespaced_lease(self.namespace, body)
            log.info("[lease] Created and acquired by %s", self.identity)
            return True
        except ApiException as e:
            if e.status == 409:   # already exists (race condition)
                return False
            raise

    def _try_acquire(self) -> bool:
        """Try to acquire or steal an expired lease. Returns True if successful."""
        lease = self._get_lease()

        if lease is None:
            return self._create_lease()

        spec = lease.spec
        holder = spec.holder_identity or ""
        renew  = spec.renew_time or spec.acquire_time or ""

        # Already the leader (pod restarted mid-session)
        if holder == self.identity:
            log.info("[lease] Already holder — re-acquiring")
            return self._renew_lease(lease)

        # Lease held by someone else — check if expired
        if renew and not _expired(str(renew), self.lease_duration_s):
            log.debug("[lease] Held by %s, not expired — standing by", holder)
            return False

        # Expired — steal it
        log.info("[lease] Lease expired (held by %s) — stealing", holder)
        return self._steal_lease(lease)

    def _steal_lease(self, lease) -> bool:
        transitions = (lease.spec.lease_transitions or 0) + 1
        lease.spec.holder_identity       = self.identity
        lease.spec.acquire_time          = _now_str()
        lease.spec.renew_time            = _now_str()
        lease.spec.lease_duration_seconds = self.lease_duration_s
        lease.spec.lease_transitions     = transitions
        try:
            self._api().replace_namespaced_lease(self.name, self.namespace, lease)
            log.info("[lease] Acquired (transition #%d)", transitions)
            return True
        except ApiException as e:
            if e.status in (409, 422):   # lost the race
                return False
            raise

    def _renew_lease(self, lease=None) -> bool:
        if lease is None:
            lease = self._get_lease()
        if lease is None:
            return False
        lease.spec.renew_time = _now_str()
        try:
            self._api().replace_namespaced_lease(self.name, self.namespace, lease)
            log.debug("[lease] Renewed by %s", self.identity)
            return True
        except ApiException as e:
            if e.status in (409, 422):
                log.warning("[lease] Renew conflict — lost leadership")
                return False
            raise

    def _release_lease(self):
        lease = self._get_lease()
        if lease and lease.spec.holder_identity == self.identity:
            lease.spec.holder_identity = ""
            lease.spec.renew_time      = _now_str()
            try:
                self._api().replace_namespaced_lease(self.name, self.namespace, lease)
                log.info("[lease] Released by %s", self.identity)
            except Exception as e:
                log.warning("[lease] Release failed (ok on shutdown): %s", e)

    # ── Renew loop (runs in background thread) ────────────────────

    def _start_renew_loop(self):
        import threading

        def _loop():
            while self._is_leader:
                time.sleep(self.renew_interval_s)
                if not self._is_leader:
                    break
                ok = self._renew_lease()
                if not ok:
                    log.error("[lease] Lost leadership — stream will exit")
                    self._is_leader = False

        self._renew_thread = threading.Thread(target=_loop, daemon=True, name="lease-renew")
        self._renew_thread.start()

    # ── Public API ────────────────────────────────────────────────

    def acquire_blocking(self):
        """Block until this pod becomes the leader."""
        log.info("[lease] %s waiting to become leader...", self.identity)
        while True:
            try:
                if self._try_acquire():
                    self._is_leader = True
                    self._start_renew_loop()
                    log.info("[lease] %s is now LEADER", self.identity)
                    return
            except Exception as e:
                log.error("[lease] Error during acquire: %s — retrying", e)
            time.sleep(self.retry_interval_s)

    def is_leader(self) -> bool:
        return self._is_leader

    def release(self):
        self._is_leader = False
        self._release_lease()

    # ── Context manager ───────────────────────────────────────────

    def __enter__(self):
        self.acquire_blocking()
        return self

    def __exit__(self, *_):
        self.release()
