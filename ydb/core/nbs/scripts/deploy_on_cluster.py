#!/usr/bin/env python3
"""
Automated deployment script for YDB NBS cluster testing.
Pulls latest changes, builds, deploys, and runs tests on cluster.
Supports multi-host, multi-disk configuration.

Cron setup (run every 5 minutes):

    crontab -e

Add the line (adjust paths and log dir as needed):

    */5 * * * * /usr/bin/flock -n /tmp/deploy_cron.lock \
        python3 ~/deploy_on_cluster.py \
        --log-dir ~/deploy_logs \
        >> ~/deploy_cron.log 2>&1

Notes:
  * `flock -n` makes cron skip the tick if a previous run is still active.
    The script also enforces its own lock via fcntl and will exit 0 if
    another instance is already running.
  * A log file is created under --log-dir ONLY if the script actually
    deploys to the cluster and runs fio (not when it exits early because
    there are no relevant changes).
  * The whole run is capped at 3 hours; if it doesn't finish in time the
    script exits with a non-zero code.
"""

import subprocess
import sys
import os
import time
import re
import signal
import fcntl
import datetime
import socket
import shlex
import json
import threading
import yaml
from pathlib import Path
from typing import List, Tuple, Dict, Optional, Any


SCRIPT_TIMEOUT_SECONDS = 3 * 60 * 60  # 3 hours
LOCK_FILE_PATH = "/tmp/deploy_on_cluster.lock"


class DeploymentConfig:
    """Configuration for deployment"""
    REPO_PATH = Path.home() / "ydbwork" / "ydb"
    ARCADIA_PATH = Path.home() / "arcadia"
    MULTINODE_PATH = ARCADIA_PATH / "kikimr" / "tools" / "multinode_configure"
    
    WATCHED_DIRS = [
        REPO_PATH / "ydb" / "core" / "nbs",
        REPO_PATH / "ydb" / "core" / "blobstorage" / "ddisk",
    ]
    
    # Cluster configuration file (set via command line)
    CLUSTER_CONFIG_PATH: Optional[Path] = None
    
    # Will be populated from cluster config
    HOSTS: List[str] = []
    GRPC_SERVER = ""  # First host will be used
    GRPC_PORT = 2135
    
    # Disk configuration
    NUM_DISKS = 8  # Number of disks to create (disk1-diskN)
    DISK_PREFIX = "disk"
    DISK_POOL = "ddp1"
    BLOCKS_COUNT = 1048576  # Default: 4GB (4G * 262144 blocks/G)

    # Unified-Agent / log_config fields populated from cluster config YAML.
    # Mirrors LogConfig in AppConfig proto used by ydbd.
    UA_URI: str = ""               # log_config.uaclient_config.uri
    UA_LOG_NAME: str = "ydb"        # log_config.uaclient_config.log_name
    CLUSTER_NAME: str = ""           # log_config.cluster_name

    @classmethod
    def load_cluster_config(cls):
        """Load cluster configuration from YAML file"""
        if not cls.CLUSTER_CONFIG_PATH.exists():
            raise FileNotFoundError(
                f"Cluster config not found: {cls.CLUSTER_CONFIG_PATH}\n"
                f"Expected format:\n"
                f"hosts:\n"
                f"  - host1.example.com\n"
                f"  - host2.example.com\n"
                f"overridden_configs:\n"
                f"  log_config:\n"
                f"    cluster_name: nbs_load_cluster\n"
                f"    uaclient_config:\n"
                f"      uri: localhost:16400\n"
            )
        
        with open(cls.CLUSTER_CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        cls.HOSTS = config.get('hosts', [])
        if not cls.HOSTS:
            raise ValueError(f"No hosts found in {cls.CLUSTER_CONFIG_PATH}")
        
        cls.GRPC_SERVER = cls.HOSTS[0]  # Use first host as GRPC server

        # --- Unified Agent / log_config ---
        overridden_configs = config.get('overridden_configs') or {}
        log_config = overridden_configs.get('log_config') or {}
        cls.CLUSTER_NAME = log_config.get('cluster_name', "") or ""
        ua_cfg = log_config.get('uaclient_config') or {}
        cls.UA_URI = ua_cfg.get('uri', "") or ""
        cls.UA_LOG_NAME = ua_cfg.get('log_name', "ydb") or "ydb"

        ColorLogger.success(f"Loaded {len(cls.HOSTS)} hosts from cluster config")
        for i, host in enumerate(cls.HOSTS, 1):
            ColorLogger.info(f"  {i}. {host}")
        if cls.UA_URI:
            ColorLogger.info(
                f"Unified Agent: uri={cls.UA_URI} "
                f"cluster={cls.CLUSTER_NAME or '<empty>'} log_name={cls.UA_LOG_NAME}"
            )
        else:
            ColorLogger.warning(
                "No log_config.uaclient_config.uri in cluster config; "
                "UA annotations will be disabled."
            )
    
    @classmethod
    def get_disk_ids(cls) -> List[str]:
        """Get list of all disk IDs (disk1, disk2, ..., diskN)"""
        return [f"{cls.DISK_PREFIX}{i}" for i in range(1, cls.NUM_DISKS + 1)]


class ColorLogger:
    """Simple colored logging"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    
    @staticmethod
    def info(msg: str):
        print(f"{ColorLogger.OKBLUE}[INFO]{ColorLogger.ENDC} {msg}")
    
    @staticmethod
    def success(msg: str):
        print(f"{ColorLogger.OKGREEN}[SUCCESS]{ColorLogger.ENDC} {msg}")
    
    @staticmethod
    def warning(msg: str):
        print(f"{ColorLogger.WARNING}[WARNING]{ColorLogger.ENDC} {msg}")
    
    @staticmethod
    def error(msg: str):
        print(f"{ColorLogger.FAIL}[ERROR]{ColorLogger.ENDC} {msg}")
    
    @staticmethod
    def step(step_num: int, msg: str):
        print(f"\n{ColorLogger.BOLD}[STEP {step_num}]{ColorLogger.ENDC} {msg}")


class SingleInstanceLock:
    """File-lock based guard ensuring only one instance of the script runs.

    Uses fcntl.flock with LOCK_NB. The lock fd is kept open for the lifetime
    of the process; exit() releases it automatically.
    """

    def __init__(self, path: str = LOCK_FILE_PATH):
        self.path = path
        self._fd = None

    def acquire_or_exit(self):
        self._fd = open(self.path, "w")
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            ColorLogger.warning(
                f"Another instance is already running (lock: {self.path}). Exiting."
            )
            sys.exit(0)
        self._fd.write(f"pid={os.getpid()}\nstarted={datetime.datetime.now().isoformat()}\n")
        self._fd.flush()


class TeeStream:
    """File-like object that writes to two streams at once (stdout/stderr + log file)."""

    def __init__(self, primary, secondary):
        self._primary = primary
        self._secondary = secondary

    def write(self, data):
        self._primary.write(data)
        try:
            self._secondary.write(data)
            self._secondary.flush()
        except Exception:
            pass
        return len(data)

    def flush(self):
        self._primary.flush()
        try:
            self._secondary.flush()
        except Exception:
            pass

    def isatty(self):
        try:
            return self._primary.isatty()
        except Exception:
            return False


class DeploymentLogger:
    """Lazy log file opener. File is created only on first activation.

    This guarantees that no log file appears on disk when the script exits
    early (e.g. no relevant changes detected).
    """

    def __init__(self, log_dir: Optional[Path]):
        self.log_dir = Path(log_dir).expanduser() if log_dir else None
        self.log_path: Optional[Path] = None
        self._fh = None
        self._orig_stdout = None
        self._orig_stderr = None
        self.activated = False

    def activate(self, commit_sha: str, commit_subject: str) -> Optional[Path]:
        """Open the log file and start tee'ing stdout/stderr into it."""
        if self.activated:
            return self.log_path
        if self.log_dir is None:
            return None

        self.log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.log_path = self.log_dir / f"deploy_{ts}.log"
        self._fh = open(self.log_path, "w", buffering=1)

        header = (
            "=" * 80 + "\n"
            f"Deployment log  :  {self.log_path}\n"
            f"Start time      :  {datetime.datetime.now().isoformat()}\n"
            f"Host            :  {socket.gethostname()}\n"
            f"PID             :  {os.getpid()}\n"
            f"Running commit  :  {commit_sha}\n"
            f"Commit subject  :  {commit_subject}\n"
            + "=" * 80 + "\n"
        )
        self._fh.write(header)
        self._fh.flush()

        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr
        sys.stdout = TeeStream(self._orig_stdout, self._fh)
        sys.stderr = TeeStream(self._orig_stderr, self._fh)
        self.activated = True
        return self.log_path

    def close(self):
        if not self.activated:
            return
        try:
            sys.stdout = self._orig_stdout
            sys.stderr = self._orig_stderr
        finally:
            if self._fh is not None:
                self._fh.close()
                self._fh = None


class ScriptTimeout(Exception):
    pass


def _install_timeout(seconds: int = SCRIPT_TIMEOUT_SECONDS):
    """Install a hard wall-clock timeout that raises ScriptTimeout."""

    def _handler(signum, frame):
        raise ScriptTimeout(f"script exceeded {seconds}s timeout")

    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)


# --- fio output parsing -----------------------------------------------------

# Matches e.g. "90.00th=[  914]" inside the "clat percentiles (usec):" block.
_CLAT_P90_RE = re.compile(r"90\.00th=\[\s*(\d+)\s*\]")
_CLAT_HEADER_RE = re.compile(r"clat percentiles \((usec|msec|nsec)\)")


def parse_fio_clat_p90(output: str) -> Optional[Tuple[int, str]]:
    """Extract clat p90 latency from fio textual output.

    Returns (value, unit) where unit is one of 'nsec', 'usec', 'msec'.
    Returns None if not found.
    """
    unit = None
    lines = output.splitlines()
    for i, line in enumerate(lines):
        m_hdr = _CLAT_HEADER_RE.search(line)
        if m_hdr:
            unit = m_hdr.group(1)
        if unit is None:
            continue
        m = _CLAT_P90_RE.search(line)
        if m:
            return int(m.group(1)), unit
    return None


def format_latency(value_unit: Optional[Tuple[int, str]]) -> str:
    if value_unit is None:
        return "n/a"
    value, unit = value_unit
    return f"{value} {unit}"


# Matches either "IOPS=43.0k" or "IOPS=43000" anywhere in an fio summary line.
_IOPS_RE = re.compile(r"IOPS=([\d.]+)([kKmM]?)")


def parse_fio_iops(output: str) -> Optional[float]:
    """Return IOPS as a float (in ops/sec) extracted from fio stdout."""
    for line in output.splitlines():
        # Prefer the summary line (starts with "  read:" / "  write:" etc.)
        stripped = line.strip()
        if not (stripped.startswith("read:") or stripped.startswith("write:")
                or stripped.startswith("rw:") or "IOPS=" in stripped):
            continue
        m = _IOPS_RE.search(stripped)
        if not m:
            continue
        value = float(m.group(1))
        mult = {"": 1.0, "k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6}[m.group(2)]
        return value * mult
    return None


def format_iops(iops: Optional[float]) -> str:
    if iops is None:
        return "n/a"
    if iops >= 1_000_000:
        return f"{iops/1_000_000:.2f}M"
    if iops >= 1_000:
        return f"{iops/1_000:.1f}k"
    return f"{iops:.0f}"


# ---------------------------------------------------------------------------
# Unified Agent annotator (SSH -> remote helper -> UA native gRPC stream)
# ---------------------------------------------------------------------------
# Unified Agent is not reachable from the machine running this script; it
# listens on a `plugin: grpc` input on one of the cluster hosts. So we:
#
#   1. scp `ua_annotate.py` once to host 0 (done via deploy_ua_helper()).
#   2. For every annotation build the message text and the common labels
#      locally (so the `pid` / `host` / timestamp are taken from the
#      deploy-script machine).
#   3. SSH into host 0 and invoke
#        python3 ~/ua_annotate.py --uri <UA uri>
#                                 --message '<text>'
#                                 --meta k=v ...
#      The helper opens the native `UnifiedAgentService/Session` bidi gRPC
#      stream (proto: library/cpp/unified_agent_client/proto/unified_agent.proto),
#      sends Initialize + one DataBatch, waits for Ack, exits 0.
#
# Session meta mirrors what ydbd's UA backend puts into SessionParameters.Meta
# (see log_backend_build.cpp::CreateLogBackendFromUAClientConfig):
#     _pid, _log_name, node_type, database, cluster
# plus a few OTEL-ish labels (project, service, host, level, component) and
# any per-event `extra_labels`.
#
# The message payload is formatted the same way ydbd writes its own log
# lines:  "<iso_ts> :<COMPONENT> <LEVEL>: <text>"
# with COMPONENT = DEPLOY_SCRIPT for this script.

DEPLOY_COMPONENT = "DEPLOY_SCRIPT"

# Name + remote path of the helper script that we scp to host 0.
UA_HELPER_SCRIPT_NAME = "ua_annotate.py"
UA_HELPER_REMOTE_PATH = f"~/{UA_HELPER_SCRIPT_NAME}"


def deploy_ua_helper(host: str) -> bool:
    """scp `ua_annotate.py` next to the deploy script up to remote *host*.

    Returns True on success. Best-effort -- any failure just means UA
    annotations will be skipped for this run.
    """
    local_path = Path(__file__).resolve().parent / UA_HELPER_SCRIPT_NAME
    if not local_path.exists():
        ColorLogger.warning(f"UA helper not found: {local_path}")
        return False

    scp_cmd = (
        f"scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"-o LogLevel=ERROR "
        f"{shlex.quote(str(local_path))} {host}:{UA_HELPER_REMOTE_PATH}"
    )
    rc, _, stderr = run_command(scp_cmd, check=False)
    if rc != 0:
        ColorLogger.warning(
            f"Failed to scp UA helper to {host}: {stderr.strip()[:200]}"
        )
        return False

    # Best-effort chmod; not required for `python3 script.py` invocation.
    run_remote_command(
        f"chmod +x {UA_HELPER_REMOTE_PATH}", host=host, check=False
    )
    ColorLogger.success(f"Deployed UA helper to {host}:{UA_HELPER_REMOTE_PATH}")
    return True


class UnifiedAgentAnnotator:
    """Remote-Unified-Agent annotation client.

    Records are built locally (so `host`, `pid`, timestamp reflect the deploy
    host) and forwarded over SSH to a small helper living on `remote_host`,
    which in turn opens a TCP connection to UA.

    Any error along the way is swallowed with a warning -- annotations must
    never break the deploy flow.
    """

    def __init__(
        self,
        uri: str,
        cluster_name: str,
        remote_host: Optional[str],
        remote_helper_path: str = UA_HELPER_REMOTE_PATH,
        log_name: str = "ydb",
        tenant: str = "",
        node_type: str = "static",
        project: str = "kikimr",
        service: str = "ydb",
        ssh_timeout: float = 15.0,
    ):
        self.uri = uri
        self.cluster_name = cluster_name
        self.log_name = log_name
        self.tenant = tenant
        self.node_type = node_type
        self.project = project
        self.service = service

        self.remote_host = remote_host
        self.remote_helper_path = remote_helper_path
        self.ssh_timeout = ssh_timeout

        fqdn = socket.getfqdn()
        self.hostname_full = fqdn
        self.hostname_short = fqdn.split(".")[0] if fqdn else socket.gethostname()
        self.pid = os.getpid()

        self._lock = threading.Lock()
        self.enabled = bool(uri) and bool(remote_host)

    # -- record construction ---------------------------------------------

    def _build_message_and_meta(
        self,
        level: str,
        text: str,
        extra_labels: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, List[Tuple[str, str]]]:
        """Return (message_payload, session_meta) for the UA helper.

        The `message_payload` is the plain log-line text that UA will store
        as the DataBatch payload, formatted identically to how ydbd writes
        its own log lines: ``<iso_ts> :<COMPONENT> <LEVEL>: <text>``.

        The `session_meta` list contains the common OTEL-style labels that
        the ydbd UA backend puts into SessionParameters.Meta (see
        ``log_backend_build.cpp::CreateLogBackendFromUAClientConfig``) plus
        the top-level OTEL fields (project / service / host / level) and
        any per-event ``extra_labels``.
        """
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        iso_ts = (
            now_utc.strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{now_utc.microsecond:06d}Z"
        )
        message = f"{text}"

        # Session meta. Empty values are dropped to keep the CLI short.
        meta_map: Dict[str, str] = {
            # ydbd-style session meta (see C++ backend):
            "_pid":        str(self.pid),
            "_log_name":   self.log_name,
            "node_type":   self.node_type,
            "database":    self.tenant,
            "cluster":     self.cluster_name,
            # OTEL-ish common labels for the annotation:
            "project":     self.project,
            "service":     self.service,
            "host":        self.hostname_short,
            "hostname":    self.hostname_full,
            "level":       level,
            "component":   DEPLOY_COMPONENT,
        }
        if extra_labels:
            for k, v in extra_labels.items():
                if v is None:
                    continue
                meta_map[str(k)] = str(v)

        meta_items = [(k, v) for k, v in meta_map.items() if v != ""]
        return message, meta_items

    # -- public api -------------------------------------------------------

    def _ssh_target(self) -> str:
        return self.remote_host or ""

    def annotate(
        self,
        text: str,
        level: str = "INFO",
        extra_labels: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send a single annotation. Best-effort; never raises."""
        if not self.enabled:
            return
        try:
            message, meta_items = self._build_message_and_meta(level, text, extra_labels)

            # NOTE: do NOT shlex.quote `self.remote_helper_path` -- we want
            # the remote shell to expand `~` / `$HOME` in it.
            parts = [f"python3 {self.remote_helper_path}",
                     "--uri",     shlex.quote(self.uri),
                     "--timeout", str(int(self.ssh_timeout)),
                     "--message", shlex.quote(message)]
            for k, v in meta_items:
                parts += ["--meta", shlex.quote(f"{k}={v}")]
            remote_cmd = " ".join(parts)

            ssh_cmd = [
                "ssh",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "LogLevel=ERROR",
                "-o", "BatchMode=yes",
                "-o", f"ConnectTimeout={int(self.ssh_timeout)}",
                self._ssh_target(),
                remote_cmd,
            ]

            with self._lock:
                proc = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    timeout=self.ssh_timeout + 5,
                )
            if proc.returncode != 0:
                stderr = proc.stderr.decode("utf-8", errors="replace").strip()
                ColorLogger.warning(
                    f"UA annotate rc={proc.returncode}: {stderr[:200]}"
                )
            else:
                ColorLogger.info(f"[UA] {level}: {text}")
        except Exception as e:
            ColorLogger.warning(f"UA annotate failed ({e}); continuing without it")

    def close(self) -> None:
        # No persistent connection to clean up; SSH processes are per-call.
        return


# Short human descriptions used in UA annotations.
FIO_MODE_DESCRIPTIONS = {
    "warmup":        "warmup 1m sequential write",
    "4k_randwrite":  "4k random write",
    "4k_randread":   "4k random read",
    "4k_mixed":      "4k random mixed (50/50)",
    "1m_seqwrite":   "1m sequential write",
    "1m_seqread":    "1m sequential read",
    "1m_mixed":      "1m sequential mixed (50/50)",
}


def _which(binary: str) -> Optional[str]:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(path, binary)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def run_command(cmd: str, cwd: Path = None, check: bool = True, env: dict = None, interactive: bool = False) -> Tuple[int, str, str]:
    """Run shell command and return (returncode, stdout, stderr)
    
    Args:
        cmd: Command to run
        cwd: Working directory
        check: Exit on error if True
        env: Environment variables (inherits parent if None)
        interactive: If True, don't capture output - let command write directly to terminal
                     (needed for commands with progress bars, colored output, etc.)
    """
    ColorLogger.info(f"Running: {cmd}")
    if cwd:
        ColorLogger.info(f"  in: {cwd}")
    
    # Inherit parent environment if not specified
    if env is None:
        env = os.environ.copy()
    
    if interactive:
        # Don't capture output - let command write directly to terminal
        # This preserves TTY detection, progress bars, colors, etc.
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            executable="/bin/bash",
            env=env
        )
        
        if check and result.returncode != 0:
            ColorLogger.error(f"Command failed with code {result.returncode}")
            sys.exit(1)
        
        return result.returncode, '', ''
    else:
        # Capture output (original behavior)
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            executable="/bin/bash",
            env=env
        )
        
        if check and result.returncode != 0:
            ColorLogger.error(f"Command failed with code {result.returncode}")
            if result.stderr:
                print(result.stderr)
            sys.exit(1)
        
        return result.returncode, result.stdout, result.stderr


def run_remote_command(cmd: str, host: str = None, check: bool = True) -> Tuple[int, str, str]:
    """Run command on remote host via SSH"""
    if host is None:
        host = DeploymentConfig.HOSTS[0] if DeploymentConfig.HOSTS else "localhost"
    
    ssh_cmd = f'ssh {host} "{cmd}"'
    return run_command(ssh_cmd, check=check)


def run_on_all_hosts(cmd: str, description: str = "", parallel: bool = True) -> Dict[str, Tuple[int, str, str]]:
    """Run command on all hosts in cluster
    
    Returns:
        Dict mapping hostname to (returncode, stdout, stderr)
    """
    if description:
        ColorLogger.info(f"{description} on {len(DeploymentConfig.HOSTS)} hosts")
    
    results = {}
    
    if parallel:
        # Run in parallel using background processes
        import threading
        
        def run_on_host(host):
            results[host] = run_remote_command(cmd, host, check=False)
        
        threads = []
        for host in DeploymentConfig.HOSTS:
            thread = threading.Thread(target=run_on_host, args=(host,))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
    else:
        # Run sequentially
        for host in DeploymentConfig.HOSTS:
            results[host] = run_remote_command(cmd, host, check=False)
    
    return results


def git_pull() -> Tuple[str, str]:
    """Pull latest changes from git repo.

    Remembers the HEAD sha BEFORE the pull so that caller can diff across
    every newly-pulled commit (not just the last one).

    Returns:
        (old_head, new_head) tuple of full SHAs. They are equal if nothing
        was pulled.
    """
    ColorLogger.step(1, "Git pull repo")

    _, old_head, _ = run_command(
        "git rev-parse HEAD",
        cwd=DeploymentConfig.REPO_PATH,
    )
    old_head = old_head.strip()

    run_command("git pull", cwd=DeploymentConfig.REPO_PATH)

    _, new_head, _ = run_command(
        "git rev-parse HEAD",
        cwd=DeploymentConfig.REPO_PATH,
    )
    new_head = new_head.strip()

    if old_head == new_head:
        ColorLogger.warning("No new commits pulled")
    else:
        ColorLogger.success(f"Pulled commits: {old_head[:8]} -> {new_head[:8]}")

    return old_head, new_head


def get_commit_subject(sha: str) -> str:
    """Return the subject line of a commit."""
    _, stdout, _ = run_command(
        f"git log -1 --pretty=format:%s {sha}",
        cwd=DeploymentConfig.REPO_PATH,
        check=False,
    )
    return stdout.strip()


def check_changes_in_directories(old_commit: str, new_commit: str) -> bool:
    """Check if any watched dir changed across ALL commits between old..new.

    This diffs old_commit..new_commit so every commit pulled in the most
    recent `git pull` is covered, not just the very last one.
    """
    ColorLogger.step(2, "Checking for changes in watched directories")

    if old_commit == new_commit:
        ColorLogger.warning("HEAD did not move; nothing to check")
        return False

    any_changes = False
    for watch_dir in DeploymentConfig.WATCHED_DIRS:
        rel_path = watch_dir.relative_to(DeploymentConfig.REPO_PATH)

        rc, stdout, _ = run_command(
            f"git diff --name-only {old_commit}..{new_commit} -- {rel_path}",
            cwd=DeploymentConfig.REPO_PATH,
            check=False,
        )

        if stdout.strip():
            ColorLogger.success(f"Changes detected in {rel_path}")
            print(stdout)
            any_changes = True

    if not any_changes:
        ColorLogger.warning("No changes in watched directories")
    return any_changes


def build_tools():
    """Build required tools"""
    ColorLogger.step(4, "Building multinode_configure")
    run_command(
        "ya make ~/arcadia/kikimr/tools/multinode_configure/",
        cwd=DeploymentConfig.MULTINODE_PATH,
        interactive=True  # ya make has progress bars and colored output
    )
    
    ColorLogger.step(6, "Building dstool")
    run_command(
        f"ya make {DeploymentConfig.REPO_PATH}/ydb/apps/dstool",
        interactive=True  # ya make has progress bars and colored output
    )


def deploy_cluster():
    """Deploy cluster configuration"""
    ColorLogger.step(7, "Deploying cluster configuration")
    
    run_command(
        f"configure/configure fullcycle promote "
        f"--config_path {shlex.quote(str(DeploymentConfig.CLUSTER_CONFIG_PATH))} "
        f"--deploy_flags do_strip",
        cwd=DeploymentConfig.MULTINODE_PATH,
        interactive=True  # configure uses rich/interactive output
    )


def define_ddisk_pool():
    """Define DDisk pool in cluster"""
    ColorLogger.step(8, "Defining DDisk pool")
    
    cmd = (
        f"{DeploymentConfig.REPO_PATH}/ydb/apps/ydbd/ydbd "
        f"--server \"{DeploymentConfig.GRPC_SERVER}\" "
        f"admin bs config invoke "
        f"--proto 'Command {{ DefineDDiskPool {{ "
        f"BoxId: 1 Name: \"{DeploymentConfig.DISK_POOL}\" "
        f"Geometry {{ NumFailRealms: 1 NumFailDomainsPerFailRealm: 5 "
        f"NumVDisksPerFailDomain: 1 RealmLevelBegin: 10 RealmLevelEnd: 10 "
        f"DomainLevelBegin: 10 DomainLevelEnd: 40 }} "
        f"PDiskFilter {{ Property {{ Type: SSD }} }} "
        f"NumDDiskGroups: 16 }} }}'"
    )
    
    run_command(cmd, interactive=True)

    time.sleep(5)


def remove_all_sockets():
    """Remove all disk sockets on all hosts before creating disks"""
    ColorLogger.step(9, "Removing existing disk sockets on all hosts")
    
    disk_ids = DeploymentConfig.get_disk_ids()
    socket_pattern = " ".join([f"/tmp/{disk_id}.sock" for disk_id in disk_ids])
    
    cmd = f"sudo rm -f {socket_pattern}"
    
    results = run_on_all_hosts(cmd, "Removing sockets", parallel=True)
    
    # Check results
    failures = []
    for host, (rc, stdout, stderr) in results.items():
        if rc == 0:
            ColorLogger.success(f"  {host}: Sockets removed")
        else:
            ColorLogger.warning(f"  {host}: Failed to remove sockets (rc={rc})")
            failures.append(host)
    
    if failures:
        ColorLogger.warning(f"Failed to remove sockets on {len(failures)} hosts")
    else:
        ColorLogger.success(f"Sockets removed on all {len(DeploymentConfig.HOSTS)} hosts")


def create_partitions():
    """Create NBS partitions for all disks"""
    ColorLogger.step(10, f"Creating {DeploymentConfig.NUM_DISKS} NBS partitions")
    
    disk_ids = DeploymentConfig.get_disk_ids()
    
    for disk_id in disk_ids:
        ColorLogger.info(f"Creating partition: {disk_id}")
        
        cmd = (
            f"{DeploymentConfig.REPO_PATH}/ydb/apps/dstool/ydb-dstool "
            f"-d -e grpc://{DeploymentConfig.GRPC_SERVER}:{DeploymentConfig.GRPC_PORT} "
            f"nbs partition create "
            f"--pool {DeploymentConfig.DISK_POOL} "
            f"--type=ssd "
            f"--block-size 4096 "
            f"--blocks-count {DeploymentConfig.BLOCKS_COUNT} "
            f"--disk-id {disk_id}"
        )
        
        run_command(cmd, interactive=True)

        time.sleep(5)
    
    ColorLogger.success(f"Created {len(disk_ids)} partitions")


def get_disks_on_host(host: str) -> List[str]:
    """Get list of disk IDs that have sockets on this host"""
    disk_ids = DeploymentConfig.get_disk_ids()
    socket_paths = " ".join([f"/tmp/{disk_id}.sock" for disk_id in disk_ids])
    
    # Check which sockets exist
    cmd = f"ls {socket_paths} 2>/dev/null || true"
    rc, stdout, _ = run_remote_command(cmd, host, check=False)
    
    found_disks = []
    for line in stdout.strip().split('\n'):
        if line and '.sock' in line:
            # Extract disk ID from /tmp/diskX.sock
            disk_id = line.split('/')[-1].replace('.sock', '')
            found_disks.append(disk_id)
    
    return found_disks


def start_qemu_on_all_hosts():
    """Start QEMU instances on all hosts based on socket availability
    
    Each QEMU instance on same host uses different ports:
    - SSH port: 8679 + offset (for SSH into QEMU VM)
    - QMP port: 8678 + offset (for QEMU management)
    
    Returns:
        Dict: {host: [(disk_id, qemu_pid, ssh_port), ...]}
    """
    ColorLogger.step(11, "Starting QEMU instances on all hosts")
    
    # First, stop any existing QEMU processes on all hosts
    ColorLogger.info("Stopping existing QEMU processes on all hosts")
    stop_cmd = "sudo pkill -f 'qemu.*disk' || true"
    run_on_all_hosts(stop_cmd, "Stopping QEMU", parallel=True)
    time.sleep(2)
    
    # Port configuration
    ssh_port_base = 8679
    qmp_port_base = 8678
    
    # Start QEMU on each host for disks that have sockets
    all_qemu_info = {}
    
    for host in DeploymentConfig.HOSTS:
        disks_on_host = get_disks_on_host(host)
        
        if not disks_on_host:
            ColorLogger.warning(f"  {host}: No disk sockets found, skipping")
            continue
        
        ColorLogger.info(f"  {host}: Found {len(disks_on_host)} disks: {', '.join(disks_on_host)}")
        
        host_qemu_pids = []
        for i, disk_id in enumerate(disks_on_host):
            ssh_port = ssh_port_base + 2 * i
            qmp_port = qmp_port_base + 2 * i
            
            # Pass ports as command-line arguments (not env vars - sudo doesn't preserve them)
            cmd = (
                f"cd ~/multinode_home/nbsd/bin && "
                f"nohup sudo ./run_qemu.sh -d {disk_id} --background "
                f"--ssh-port {ssh_port} --qmp-port {qmp_port} "
                f"--log-file ~/qemu_{disk_id}.log 2>&1 & "
                f"echo \\$!"
            )
            
            rc, stdout, _ = run_remote_command(cmd, host, check=False)
            if rc == 0:
                qemu_pid = stdout.strip()
                host_qemu_pids.append((disk_id, qemu_pid, ssh_port))
                ColorLogger.success(f"    {host}/{disk_id}: Started QEMU (PID: {qemu_pid}, SSH: {ssh_port}, QMP: {qmp_port})")
            else:
                ColorLogger.error(f"    {host}/{disk_id}: Failed to start QEMU")
        
        all_qemu_info[host] = host_qemu_pids
    
    # Wait for all QEMU instances to boot
    total_qemu = sum(len(pids) for pids in all_qemu_info.values())
    ColorLogger.info(f"Waiting for {total_qemu} QEMU instances to boot (30 seconds)...")
    time.sleep(30)
    
    return all_qemu_info

#
# Run fio in parallel for all user disks in qemu instances
#
def run_fio_in_qemu_parallel(qemu_info: Dict[str, List[Tuple[str, str, int]]], 
                             test_name: str, rw: str, bs: str, runtime: int,
                             iodepth: int = 32, rwmixread: int = None, 
                             numjobs: int = 1, description: str = "",
                             verify: bool = False,
                             annotator: Optional[UnifiedAgentAnnotator] = None) -> Dict:
    """Run FIO test inside all QEMU instances in parallel
    
    Args:
        qemu_info: Dict mapping host to list of (disk_id, qemu_pid, ssh_port) tuples
        test_name: Name for the test
        rw: IO pattern (read, write, randread, randwrite, randrw, readwrite)
        bs: Block size (e.g., '4k', '1m'). When verify=True, this value is used as
            the --bssplit argument instead of --bs.
        runtime: Runtime in seconds
        iodepth: IO depth
        rwmixread: Percentage of reads in mixed workload
        numjobs: Number of parallel jobs
        description: Human-readable description
        verify: If True, enable fio data verification. The bs argument is passed via
            --bssplit and the standard verify flags are appended.
    """
    ColorLogger.info(f"Running FIO test in parallel: {description or test_name}")

    # Short mode description for UA annotations.
    mode_desc = FIO_MODE_DESCRIPTIONS.get(test_name, f"{bs} {rw}")

    if annotator is not None:
        annotator.annotate(
            f"Start fio run: {mode_desc}",
            level="INFO",
            extra_labels={
                "event": "fio_start",
                "fio_test": test_name,
                "fio_mode": mode_desc,
                "fio_rw": rw,
                "fio_bs": bs,
                "fio_runtime_s": str(runtime),
            },
        )

    qemu_user = "qemu"
    device = "/dev/vdb"
    
    # Build FIO command
    fio_cmd_parts = [
        "sudo fio",
        f"--name={test_name}",
        "--ioengine=libaio",
        f"--iodepth={iodepth}",
        f"--rw={rw}",
    ]

    if verify:
        fio_cmd_parts.append(f"--bssplit={bs}")
    else:
        fio_cmd_parts.append(f"--bs={bs}")

    fio_cmd_parts += [
        "--direct=1",
        f"--numjobs={numjobs}",
        "--group_reporting",
        f"--filename={device}",
        f"--runtime={runtime}",
        "--time_based=1",
    ]

    if rwmixread is not None:
        fio_cmd_parts.append(f"--rwmixread={rwmixread}")

    if verify:
        fio_cmd_parts += [
            "--verify_fatal=1",
            "--verify_dump=1",
            "--verify_async=2",
            "--do_verify=1",
            "--verify=sha1",
            "--verify_backlog=500",
        ]
    
    fio_cmd = " ".join(fio_cmd_parts)
    
    # Run FIO in parallel on all QEMU instances
    import threading
    results = {}
    
    def run_fio_on_qemu(host, disk_id, ssh_port):
        """Run FIO on single QEMU instance"""
        # SSH into QEMU VM (via forwarded port on remote host)
        ssh_cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f"-i ~/qemu_key "
            f"-p {ssh_port} {qemu_user}@localhost "
            f"'{fio_cmd}'"
        )
        
        rc, stdout, stderr = run_remote_command(ssh_cmd, host, check=False)
        results[f"{host}/{disk_id}"] = (rc, stdout, stderr)
    
    # Start all FIO tests in parallel
    threads = []
    for host in DeploymentConfig.HOSTS:
        if host not in qemu_info:
            continue
        
        for disk_id, _, ssh_port in qemu_info[host]:
            thread = threading.Thread(target=run_fio_on_qemu, args=(host, disk_id, ssh_port))
            thread.start()
            threads.append(thread)
            ColorLogger.info(f"  Started FIO on {host}/{disk_id} (port {ssh_port})")
    
    # Wait for all tests to complete
    for thread in threads:
        thread.join()
    
    # Analyze results
    successes = 0
    failures = 0
    per_instance = {}  # instance -> {"ok": bool, "p90": ..., "iops": ...}

    print("\n" + "="*80)
    print(f"FIO Results: {description or test_name}")
    print(f"Total instances: {len(results)}")
    print("="*80)

    for instance, (rc, stdout, stderr) in results.items():
        p90 = parse_fio_clat_p90(stdout) if rc == 0 else None
        iops = parse_fio_iops(stdout) if rc == 0 else None
        per_instance[instance] = {"ok": rc == 0, "p90": p90, "iops": iops}
        if rc == 0:
            successes += 1
            ColorLogger.success(
                f"  {instance}: PASS  clat p90 = {format_latency(p90)}  "
                f"iops = {format_iops(iops)}"
            )
            for line in stdout.split('\n'):
                if 'IOPS=' in line or 'BW=' in line or 'lat' in line:
                    print(f"    {instance}: {line.strip()}")
        else:
            failures += 1
            ColorLogger.error(f"  {instance}: FAIL")
            if stderr:
                print(f"    Error: {stderr[:200]}")

    # Aggregate p90 across instances (max; any slow disk matters).
    all_p90 = [info["p90"] for info in per_instance.values() if info["p90"] is not None]
    agg_p90 = None
    if all_p90:
        def to_usec(v):
            val, unit = v
            return {"nsec": val / 1000.0, "usec": float(val), "msec": val * 1000.0}[unit]
        agg_p90 = max(all_p90, key=to_usec)

    # Aggregate IOPS across instances (sum -- that's the cluster throughput).
    all_iops = [info["iops"] for info in per_instance.values() if info["iops"] is not None]
    total_iops: Optional[float] = sum(all_iops) if all_iops else None

    print("="*80)
    print(f"Summary: {successes} passed, {failures} failed  "
          f"| aggregate clat p90 = {format_latency(agg_p90)}"
          f"  | total iops = {format_iops(total_iops)}")
    print("="*80 + "\n")

    if annotator is not None:
        ann_text = (
            f"Finish fio run: {mode_desc}  "
            f"p90={format_latency(agg_p90)}  iops={format_iops(total_iops)}  "
            f"ok={successes}/{successes + failures}"
        )
        annotator.annotate(
            ann_text,
            level="INFO" if failures == 0 else "WARN",
            extra_labels={
                "event": "fio_finish",
                "fio_test": test_name,
                "fio_mode": mode_desc,
                "fio_rw": rw,
                "fio_bs": bs,
                "fio_successes": str(successes),
                "fio_failures": str(failures),
                "fio_p90_value": str(agg_p90[0]) if agg_p90 else "",
                "fio_p90_unit": agg_p90[1] if agg_p90 else "",
                "fio_iops_total": f"{total_iops:.0f}" if total_iops is not None else "",
            },
        )

    return {
        "test_name": test_name,
        "description": description or test_name,
        "mode_desc": mode_desc,
        "rw": rw,
        "bs": bs,
        "runtime": runtime,
        "successes": successes,
        "failures": failures,
        "per_instance": per_instance,
        "aggregate_p90": agg_p90,
        "total_iops": total_iops,
    }


def setup_fio_client_server(qemu_info: Dict[str, List[Tuple[str, str]]]):
    """Setup FIO client-server configuration for distributed testing
    
    Args:
        qemu_info: Dict mapping host to list of (disk_id, qemu_pid) tuples
    """
    ColorLogger.step(12, "Setting up FIO client-server configuration")
    
    qemu_ssh_port = 8679
    qemu_user = "qemu"
    fio_port_base = 8765
    
    # Start FIO servers on all QEMU VMs
    ColorLogger.info("Starting FIO servers on all QEMU instances")
    
    server_info = []  # List of (host, disk_id, port)
    port_offset = 0
    
    for host in DeploymentConfig.HOSTS:
        if host not in qemu_info or not qemu_info[host]:
            continue
        
        for disk_id, _ in qemu_info[host]:
            fio_port = fio_port_base + port_offset
            port_offset += 1
            
            # Start FIO server inside QEMU VM (via SSH through host)
            fio_server_cmd = (
                f"nohup sudo fio --server={fio_port} "
                f"> ~/fio_server_{disk_id}.log 2>&1 & "
                f"echo \\$!"
            )
            
            ssh_cmd = (
                f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                f"-i ~/qemu_key -p {qemu_ssh_port} {qemu_user}@localhost "
                f"'{fio_server_cmd}'"
            )
            
            rc, stdout, _ = run_remote_command(ssh_cmd, host, check=False)
            if rc == 0:
                server_pid = stdout.strip()
                server_info.append((host, disk_id, fio_port))
                ColorLogger.success(f"  {host}/{disk_id}: FIO server started (port {fio_port}, PID {server_pid})")
            else:
                ColorLogger.error(f"  {host}/{disk_id}: Failed to start FIO server")
    
    if not server_info:
        ColorLogger.error("No FIO servers started!")
        return None
    
    ColorLogger.info(f"Started {len(server_info)} FIO servers")
    time.sleep(3)  # Give servers time to start
    
    return server_info


def run_distributed_fio_test(server_info: List[Tuple[str, str, int]], 
                             test_name: str, rw: str, bs: str, runtime: int,
                             description: str = ""):
    """Run FIO test across all servers in parallel
    
    Args:
        server_info: List of (host, disk_id, port) tuples
        test_name: Name for the test
        rw: IO pattern
        bs: Block size
        runtime: Runtime in seconds
        description: Human-readable description
    """
    ColorLogger.info(f"Running distributed FIO test: {description or test_name}")
    
    qemu_ssh_port = 8679
    qemu_user = "qemu"
    device = "/dev/vdb"
    
    # Create FIO job file for client
    fio_jobs = []
    for i, (host, disk_id, port) in enumerate(server_info):
        job = f"""
[{disk_id}]
client_hostname={host}
remote_config=1
port={port}
rw={rw}
bs={bs}
ioengine=libaio
iodepth=32
direct=1
filename={device}
runtime={runtime}
time_based=1
"""
        fio_jobs.append(job)
    
    fio_config = "[global]\ngroup_reporting=1\n" + "\n".join(fio_jobs)
    
    # Write FIO config to temp file on first host
    first_host = DeploymentConfig.HOSTS[0]
    config_file = f"/tmp/fio_{test_name}.ini"
    
    # Upload config via here-doc
    upload_cmd = f"cat > {config_file} << 'EOF'\n{fio_config}\nEOF"
    run_remote_command(upload_cmd, first_host, check=False)
    
    # Run FIO client
    fio_client_cmd = f"sudo fio --client={config_file} {config_file}"
    
    ColorLogger.info(f"Running FIO client on {first_host} with {len(server_info)} servers")
    rc, stdout, stderr = run_remote_command(fio_client_cmd, first_host, check=False)
    
    if rc == 0:
        ColorLogger.success(f"Distributed FIO test '{test_name}' completed")
        print("\n" + "="*80)
        print(f"Distributed FIO Results: {description or test_name}")
        print(f"Servers: {len(server_info)}")
        print("="*80)
        print(stdout)
        print("="*80 + "\n")
        return True
    else:
        ColorLogger.error(f"Distributed FIO test '{test_name}' failed")
        if stderr:
            print(stderr)
        return False


def start_fio_long_term_background(qemu_info: Dict[str, List[Tuple[str, str, int]]]):
    """Start long-term FIO test in background on all QEMU instances (360 days, 4K randwrite)
    
    Args:
        qemu_info: Dict mapping host to list of (disk_id, qemu_pid, ssh_port) tuples
    """
    ColorLogger.step(13, "Starting long-term FIO tests (360 days, background)")
    
    qemu_user = "qemu"
    device = "/dev/vdb"
    runtime_days = 360
    runtime_seconds = runtime_days * 24 * 3600
    
    ColorLogger.info(f"Starting background FIO on all QEMU instances: 4K randwrite for {runtime_days} days")
    
    all_pids = []
    
    for host in DeploymentConfig.HOSTS:
        if host not in qemu_info or not qemu_info[host]:
            continue
        
        for disk_id, qemu_pid, ssh_port in qemu_info[host]:
            # FIO command for long-term test
            fio_cmd = (
                f"nohup sudo fio "
                f"--name=longterm_4k_randwrite "
                f"--ioengine=libaio "
                f"--iodepth=32 "
                f"--rw=randwrite "
                f"--bs=4k "
                f"--direct=1 "
                f"--numjobs=1 "
                f"--group_reporting "
                f"--filename={device} "
                f"--runtime={runtime_seconds} "
                f"--time_based=1 "
                f"> ~/fio_longterm_{disk_id}.log 2>&1 & "
                f"echo \\$!"
            )
            
            # SSH into QEMU VM and start background FIO
            ssh_cmd = (
                f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                f"-i ~/qemu_key "
                f"-p {ssh_port} {qemu_user}@localhost "
                f"'{fio_cmd}'"
            )
            
            rc, stdout, _ = run_remote_command(ssh_cmd, host, check=False)
            
            if rc == 0:
                fio_pid = stdout.strip()
                all_pids.append((host, disk_id, ssh_port, fio_pid))
                ColorLogger.success(f"  {host}/{disk_id}: Long-term FIO started (PID: {fio_pid}, SSH port: {ssh_port})")
            else:
                ColorLogger.error(f"  {host}/{disk_id}: Failed to start long-term FIO")
    
    if all_pids:
        ColorLogger.success(f"Started {len(all_pids)} long-term FIO instances")
        ColorLogger.info(f"Duration: {runtime_days} days ({runtime_seconds} seconds)")
        ColorLogger.info("Log files: ~/fio_longterm_diskX.log (inside each QEMU)")
        return True
    else:
        ColorLogger.error("Failed to start any long-term FIO tests")
        return False


def build_report(new_head: str, commit_subject: str,
                 fio_results: List[Dict], log_path: Optional[Path]) -> str:
    """Build a human-readable text report for email + log."""
    lines = []
    lines.append("YDB NBS Cluster Deployment Report")
    lines.append("=" * 80)
    lines.append(f"Host          : {socket.gethostname()}")
    lines.append(f"Finished      : {datetime.datetime.now().isoformat()}")
    lines.append(f"Commit        : {new_head}")
    lines.append(f"Commit subject: {commit_subject}")
    if log_path:
        lines.append(f"Log file      : {log_path}")
    lines.append("")
    lines.append("FIO results (clat p90 and total IOPS across all disks):")
    lines.append("-" * 88)
    lines.append(f"{'Test':<28} {'rw':<10} {'bs':<6} {'runtime':>8}  "
                 f"{'ok/total':>10}  {'p90 clat':>12}  {'total iops':>12}")
    lines.append("-" * 88)
    for r in fio_results:
        total = r["successes"] + r["failures"]
        lines.append(
            f"{r['description'][:28]:<28} {r['rw']:<10} {r['bs']:<6} "
            f"{r['runtime']:>8}  {r['successes']:>4}/{total:<4}   "
            f"{format_latency(r['aggregate_p90']):>12}  "
            f"{format_iops(r.get('total_iops')):>12}"
        )
    lines.append("-" * 88)
    lines.append("")
    lines.append("Per-instance p90 clat / iops:")
    for r in fio_results:
        lines.append(f"  [{r['description']}]")
        for instance, info in r["per_instance"].items():
            status = "OK  " if info["ok"] else "FAIL"
            lines.append(
                f"    {status} {instance:<40} "
                f"p90={format_latency(info['p90'])}  "
                f"iops={format_iops(info.get('iops'))}"
            )
        lines.append("")
    return "\n".join(lines)


def main():
    """Main deployment workflow"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Automated deployment script for YDB NBS cluster testing (multi-host)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Skip git pull and change detection, force deployment'
    )
    parser.add_argument(
        '--num-disks', '-n',
        type=int,
        default=8,
        help='Number of disks to create (default: 8)'
    )
    parser.add_argument(
        '--config-path',
        type=str,
        default='~/.mnc/load_cluster.yaml',
        help='Path to cluster configuration YAML file (default: ~/.mnc/load_cluster.yaml)'
    )
    parser.add_argument(
        '--size',
        type=str,
        default='4G',
        help='Disk size in format XG (e.g., 4G, 8G, 16G). Default: 4G'
    )
    parser.add_argument(
        '--log-dir',
        type=str,
        default=None,
        help='Directory where a timestamped deploy log will be written. '
             'A log file is created ONLY when the script actually deploys '
             'to the cluster and runs fio (not when it exits early).'
    )
    args = parser.parse_args()

    # --- Single-instance lock: exit 0 if another run is in progress. -------
    lock = SingleInstanceLock()
    lock.acquire_or_exit()

    # --- 3h wall-clock timeout. --------------------------------------------
    _install_timeout(SCRIPT_TIMEOUT_SECONDS)

    # Parse disk size and convert to blocks
    size_str = args.size.upper()
    if not size_str.endswith('G'):
        ColorLogger.error(f"Invalid size format: {args.size}. Must be in format XG (e.g., 4G, 8G)")
        sys.exit(1)
    try:
        size_gb = int(size_str[:-1])
        # Convert GB to blocks (4KB block size)
        # size_gb * 1024 * 1024 * 1024 / 4096 = size_gb * 262144
        DeploymentConfig.BLOCKS_COUNT = size_gb * 262144
    except ValueError:
        ColorLogger.error(f"Invalid size format: {args.size}. Must be in format XG (e.g., 4G, 8G)")
        sys.exit(1)

    DeploymentConfig.NUM_DISKS = args.num_disks
    DeploymentConfig.CLUSTER_CONFIG_PATH = Path(args.config_path).expanduser()

    deploy_logger = DeploymentLogger(Path(args.log_dir) if args.log_dir else None)

    print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}YDB NBS Cluster Deployment Script (Multi-Host){ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")

    fio_results: List[Dict] = []
    new_head = "unknown"
    commit_subject = ""
    annotator: Optional[UnifiedAgentAnnotator] = None

    try:
        ColorLogger.info("Loading cluster configuration")
        DeploymentConfig.load_cluster_config()
        ColorLogger.info(f"Number of disks to create: {DeploymentConfig.NUM_DISKS}")
        ColorLogger.info(f"Disk size: {args.size} ({DeploymentConfig.BLOCKS_COUNT} blocks)")
        print()

        # Construct UA annotator. UA is reached via SSH to host 0 + the
        # ua_annotate.py helper we scp to it. Disabled if there's no uri
        # in the cluster config or no hosts to reach.
        ua_remote_host = DeploymentConfig.HOSTS[0] if DeploymentConfig.HOSTS else None
        annotator = UnifiedAgentAnnotator(
            uri=DeploymentConfig.UA_URI,
            cluster_name=DeploymentConfig.CLUSTER_NAME,
            log_name=DeploymentConfig.UA_LOG_NAME,
            remote_host=ua_remote_host,
        )
        if annotator.enabled:
            if not deploy_ua_helper(ua_remote_host):
                ColorLogger.warning(
                    "Disabling UA annotations because ua_annotate.py could not "
                    "be copied to the remote host."
                )
                annotator.enabled = False

        # --- Step 1-3: git pull + change detection across ALL pulled commits
        if args.force:
            ColorLogger.warning("--force flag set, skipping git pull and change detection")
            _, new_head, _ = run_command(
                "git rev-parse HEAD", cwd=DeploymentConfig.REPO_PATH
            )
            new_head = new_head.strip()
        else:
            old_head, new_head = git_pull()
            if old_head == new_head:
                ColorLogger.warning("No new commits pulled. Exiting (no log written).")
                sys.exit(0)
            if not check_changes_in_directories(old_head, new_head):
                ColorLogger.warning(
                    "No changes in watched directories across pulled commits. "
                    "Exiting (no log written)."
                )
                ColorLogger.info("Use --force to deploy anyway")
                sys.exit(0)

        commit_subject = get_commit_subject(new_head)

        # From this point we are definitely deploying: open the log file.
        log_path = deploy_logger.activate(new_head, commit_subject)
        if log_path is not None:
            ColorLogger.info(f"Deployment log: {log_path}")
        else:
            ColorLogger.warning("--log-dir not provided; no deploy log will be written")

        ColorLogger.info(f"Running commit: {new_head}  ({commit_subject})")

        # Annotate: start of deploy (carries commit being deployed).
        annotator.annotate(
            f"Start deploy of commit {new_head[:12]} ({commit_subject})",
            level="INFO",
            extra_labels={
                "event": "deploy_start",
                "commit": new_head,
                "commit_short": new_head[:12],
                "commit_subject": commit_subject,
            },
        )

        # --- Step 4-8: Build and deploy on build VM ------------------------
        build_tools()
        deploy_cluster()
        define_ddisk_pool()

        remove_all_sockets()
        create_partitions()

        qemu_info = start_qemu_on_all_hosts()
        if not qemu_info:
            ColorLogger.error("No QEMU instances started!")
            sys.exit(1)

        ColorLogger.step(12, "Running FIO tests in all QEMU VMs")

        ColorLogger.info("Running warmup test...")
        warmup = run_fio_in_qemu_parallel(
            qemu_info, "warmup", "write", "1m", 60,
            description="Warmup: Sequential write 1MB blocks",
            annotator=annotator,
        )
        fio_results.append(warmup)

        tests = [
            ("4k_randwrite", "randwrite", "4k", 60, None, "4K random write", 32, False),
            ("4k_randread", "randread", "4k", 60, None, "4K random read", 32, False),
            ("4k_mixed", "randrw", "4k", 60, 50, "4K random mixed (50/50)", 32, False),
            ("1m_seqwrite", "write", "1m", 60, None, "1MB sequential write", 32, False),
            ("1m_seqread", "read", "1m", 60, None, "1MB sequential read", 32, False),
            ("1m_mixed", "readwrite", "1m", 60, 50, "1MB sequential mixed (50/50)", 32, False),
            ("mixed_bssplit_verify", "randwrite", "4k/20:8k/20:64k/50:1M/10", 60, None,
                "Mixed bssplit random write with verify (sha1)", 32, True),
#           ("4k_randwrite_iodepth64", "randwrite", "4k", 60, None, "4K random write iodepth 64", 64, False),
#           ("4k_randwrite_iodepth96", "randwrite", "4k", 60, None, "4K random write iodepth 96", 96, False),
        ]
        for test_name, rw, bs, runtime, rwmixread, desc, iodepth, verify in tests:
            r = run_fio_in_qemu_parallel(
                qemu_info, test_name, rw, bs, runtime,
                rwmixread=rwmixread, description=desc, iodepth=iodepth,
                verify=verify,
                annotator=annotator,
            )
            fio_results.append(r)
            time.sleep(2)

        start_fio_long_term_background(qemu_info)

        print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
        ColorLogger.success("Deployment and testing completed successfully!")
        print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")

        total_qemu = sum(len(pids) for pids in qemu_info.values())
        ColorLogger.info(f"QEMU instances running: {total_qemu}")
        for host, pids in qemu_info.items():
            if pids:
                disks = ", ".join([disk_id for disk_id, _, _ in pids])
                ColorLogger.info(f"  {host}: {disks}")
        ColorLogger.info("Long-term FIO tests running on all QEMU instances - check ~/fio_longterm_diskX.log")

        # --- Report ------------------------------------------------
        report = build_report(new_head, commit_subject, fio_results, deploy_logger.log_path)
        print("\n" + report)

        # Annotate: finish of deploy (carries commit + summary).
        if annotator is not None:
            annotator.annotate(
                f"Finish deploy of commit {new_head[:12]} OK",
                level="INFO",
                extra_labels={
                    "event": "deploy_finish",
                    "status": "ok",
                    "commit": new_head,
                    "commit_short": new_head[:12],
                    "fio_tests": str(len(fio_results)),
                },
            )

    except ScriptTimeout as e:
        ColorLogger.error(f"TIMEOUT: {e}")
        partial = build_report(new_head, commit_subject, fio_results, deploy_logger.log_path)
        print(partial)
        if annotator is not None:
            annotator.annotate(
                f"Finish deploy of commit {new_head[:12]} TIMEOUT: {e}",
                level="ERROR",
                extra_labels={
                    "event": "deploy_finish",
                    "status": "timeout",
                    "commit": new_head,
                    "commit_short": new_head[:12],
                },
            )
        sys.exit(2)
    except KeyboardInterrupt:
        ColorLogger.warning("\nDeployment interrupted by user")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as e:
        ColorLogger.error(f"Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        if annotator is not None:
            annotator.annotate(
                f"Finish deploy of commit {new_head[:12]} FAILED: {e}",
                level="ERROR",
                extra_labels={
                    "event": "deploy_finish",
                    "status": "failed",
                    "commit": new_head,
                    "commit_short": new_head[:12],
                    "error": str(e)[:500],
                },
            )
        sys.exit(1)
    finally:
        signal.alarm(0)
        if annotator is not None:
            annotator.close()
        deploy_logger.close()


if __name__ == "__main__":
    main()
