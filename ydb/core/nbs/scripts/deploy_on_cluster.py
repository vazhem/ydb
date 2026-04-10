#!/usr/bin/env python3
"""
Automated deployment script for YDB NBS cluster testing.
Pulls latest changes, builds, deploys, and runs tests on cluster.
Supports multi-host, multi-disk configuration.
"""

import subprocess
import sys
import os
import time
import yaml
from pathlib import Path
from typing import List, Tuple, Dict, Set


class DeploymentConfig:
    """Configuration for deployment"""
    REPO_PATH = Path.home() / "ydbwork" / "ydb"
    ARCADIA_PATH = Path.home() / "arcadia"
    MULTINODE_PATH = ARCADIA_PATH / "kikimr" / "tools" / "multinode_configure"
    
    WATCHED_DIRS = [
        REPO_PATH / "ydb" / "core" / "nbs",
        REPO_PATH / "ydb" / "core" / "blobstorage" / "ddisk",
    ]
    
    # Cluster configuration file
    CLUSTER_CONFIG_PATH = Path.home() / ".mnc" / "load_cluster.yaml"
    
    # Will be populated from cluster config
    HOSTS: List[str] = []
    GRPC_SERVER = ""  # First host will be used
    GRPC_PORT = 2135
    
    REMOTE_USER = os.environ.get("USER", "vazhenin-mv")
    
    # Disk configuration
    NUM_DISKS = 8  # Number of disks to create (disk1-diskN)
    DISK_PREFIX = "disk"
    DISK_POOL = "ddp1"
    
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
            )
        
        with open(cls.CLUSTER_CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        
        cls.HOSTS = config.get('hosts', [])
        if not cls.HOSTS:
            raise ValueError(f"No hosts found in {cls.CLUSTER_CONFIG_PATH}")
        
        cls.GRPC_SERVER = cls.HOSTS[0]  # Use first host as GRPC server
        ColorLogger.success(f"Loaded {len(cls.HOSTS)} hosts from cluster config")
        for i, host in enumerate(cls.HOSTS, 1):
            ColorLogger.info(f"  {i}. {host}")
    
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
    
    # Add user if not already in host string
    if '@' not in host:
        host = f"{DeploymentConfig.REMOTE_USER}@{host}"
    
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


def git_pull() -> bool:
    """Pull latest changes from git repo"""
    ColorLogger.step(1, "Git pull repo")
    
    _, old_head, _ = run_command(
        "git rev-parse HEAD",
        cwd=DeploymentConfig.REPO_PATH
    )
    old_head = old_head.strip()
    
    run_command("git pull", cwd=DeploymentConfig.REPO_PATH)
    
    _, new_head, _ = run_command(
        "git rev-parse HEAD",
        cwd=DeploymentConfig.REPO_PATH
    )
    new_head = new_head.strip()
    
    if old_head == new_head:
        ColorLogger.warning("No new commits pulled")
        return False
    
    ColorLogger.success(f"Pulled commits: {old_head[:8]} -> {new_head[:8]}")
    return True


def check_changes_in_directories(old_commit: str, new_commit: str) -> bool:
    """Check if any changes exist in watched directories"""
    ColorLogger.step(2, "Checking for changes in watched directories")
    
    for watch_dir in DeploymentConfig.WATCHED_DIRS:
        rel_path = watch_dir.relative_to(DeploymentConfig.REPO_PATH)
        
        rc, stdout, _ = run_command(
            f"git diff --name-only {old_commit}..{new_commit} -- {rel_path}",
            cwd=DeploymentConfig.REPO_PATH,
            check=False
        )
        
        if stdout.strip():
            ColorLogger.success(f"Changes detected in {rel_path}")
            print(stdout)
            return True
    
    ColorLogger.warning("No changes in watched directories")
    return False


def check_for_changes() -> bool:
    """Check if there are changes in watched directories since last commit"""
    ColorLogger.step(2, "Checking for changes in watched directories")
    
    # Get the last 2 commits to check changes between them
    rc, stdout, _ = run_command(
        "git log -2 --pretty=format:%H",
        cwd=DeploymentConfig.REPO_PATH
    )
    
    commits = stdout.strip().split('\n')
    if len(commits) < 2:
        ColorLogger.warning("Less than 2 commits, checking HEAD changes")
        new_commit = commits[0] if commits else "HEAD"
        old_commit = "HEAD~1"
    else:
        new_commit = commits[0]
        old_commit = commits[1]
    
    for watch_dir in DeploymentConfig.WATCHED_DIRS:
        rel_path = watch_dir.relative_to(DeploymentConfig.REPO_PATH)
        
        rc, stdout, _ = run_command(
            f"git diff --name-only {old_commit}..{new_commit} -- {rel_path}",
            cwd=DeploymentConfig.REPO_PATH,
            check=False
        )
        
        if stdout.strip():
            ColorLogger.success(f"Changes detected in {rel_path}")
            print(stdout)
            return True
    
    return False


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
        "configure/configure fullcycle promote "
        "--config_path ~/.mnc/load_cluster.yaml "
        "--deploy_flags do_strip",
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
        f"NumDDiskGroups: 32 }} }}'"
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
            f"--blocks-count 1048576 "
            f"--disk-id {disk_id}"
        )
        
        run_command(cmd, interactive=True)
    
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


def run_fio_in_qemu_parallel(qemu_info: Dict[str, List[Tuple[str, str, int]]], 
                             test_name: str, rw: str, bs: str, runtime: int,
                             iodepth: int = 32, rwmixread: int = None, 
                             numjobs: int = 1, description: str = "") -> bool:
    """Run FIO test inside all QEMU instances in parallel
    
    Args:
        qemu_info: Dict mapping host to list of (disk_id, qemu_pid, ssh_port) tuples
        test_name: Name for the test
        rw: IO pattern (read, write, randread, randwrite, randrw, readwrite)
        bs: Block size (e.g., '4k', '1m')
        runtime: Runtime in seconds
        iodepth: IO depth
        rwmixread: Percentage of reads in mixed workload
        numjobs: Number of parallel jobs
        description: Human-readable description
    """
    ColorLogger.info(f"Running FIO test in parallel: {description or test_name}")
    
    qemu_user = "qemu"
    device = "/dev/vdb"
    
    # Build FIO command
    fio_cmd_parts = [
        "sudo fio",
        f"--name={test_name}",
        "--ioengine=libaio",
        f"--iodepth={iodepth}",
        f"--rw={rw}",
        f"--bs={bs}",
        "--direct=1",
        f"--numjobs={numjobs}",
        "--group_reporting",
        f"--filename={device}",
        f"--runtime={runtime}",
        "--time_based=1"
    ]
    
    if rwmixread is not None:
        fio_cmd_parts.append(f"--rwmixread={rwmixread}")
    
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
    
    print("\n" + "="*80)
    print(f"FIO Results: {description or test_name}")
    print(f"Total instances: {len(results)}")
    print("="*80)
    
    for instance, (rc, stdout, stderr) in results.items():
        if rc == 0:
            successes += 1
            ColorLogger.success(f"  {instance}: PASS")
            # Optionally print summary from output
            for line in stdout.split('\n'):
                if 'IOPS=' in line or 'BW=' in line or 'lat' in line:
                    print(f"    {instance}: {line.strip()}")
        else:
            failures += 1
            ColorLogger.error(f"  {instance}: FAIL")
            if stderr:
                print(f"    Error: {stderr[:200]}")
    
    print("="*80)
    print(f"Summary: {successes} passed, {failures} failed")
    print("="*80 + "\n")
    
    return failures == 0


# Removed run_fio_warmup - using run_fio_in_qemu_parallel instead


# Removed run_fio_test_suite - using run_fio_in_qemu_parallel instead


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


def main():
    """Main deployment workflow"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Automated deployment script for YDB NBS cluster testing (multi-host)',
        formatter_class=argparse.RawDescriptionHelpFormatter
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
    args = parser.parse_args()
    
    # Set number of disks from args
    DeploymentConfig.NUM_DISKS = args.num_disks
    
    print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}YDB NBS Cluster Deployment Script (Multi-Host){ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")
    
    try:
        # Load cluster configuration
        ColorLogger.info("Loading cluster configuration")
        DeploymentConfig.load_cluster_config()
        ColorLogger.info(f"Number of disks to create: {DeploymentConfig.NUM_DISKS}")
        print()
        
        # Step 1-3: Git operations (skip if --force)
        if args.force:
            ColorLogger.warning("--force flag set, skipping git pull and change detection")
        else:
            if not git_pull():
                ColorLogger.warning("No new changes pulled, checking existing changes...")
            
            if not check_for_changes():
                ColorLogger.warning("No changes in watched directories. Exiting.")
                ColorLogger.info("Use --force to deploy anyway")
                sys.exit(0)
        
        # Step 4-8: Build and deploy on build VM
        build_tools()
        deploy_cluster()
        define_ddisk_pool()
        
        # Step 9: Remove old sockets on all hosts
        remove_all_sockets()
        
        # Step 10: Create partitions
        create_partitions()
        
        # Step 11: Start QEMU on all hosts
        qemu_info = start_qemu_on_all_hosts()
        
        if not qemu_info:
            ColorLogger.error("No QEMU instances started!")
            sys.exit(1)
        
        # Give QEMU some time to fully boot before running FIO
        ColorLogger.info("Waiting additional time for QEMU VMs to stabilize...")
        time.sleep(10)
        
        # Step 12: Run FIO tests in all QEMU VMs (in parallel, not client-server)
        ColorLogger.step(12, "Running FIO tests in all QEMU VMs")
        
        # Warmup
        ColorLogger.info("Running warmup test...")
        run_fio_in_qemu_parallel(
            qemu_info, "warmup", "write", "1m", 300,
            description="Warmup: Sequential write 1MB blocks"
        )
        
        # Performance test suite
        tests = [
            ("4k_randwrite", "randwrite", "4k", 60, None, "4K random write"),
            ("4k_randread", "randread", "4k", 60, None, "4K random read"),
            ("4k_mixed", "randrw", "4k", 60, 50, "4K random mixed (50/50)"),
            ("1m_seqwrite", "write", "1m", 60, None, "1MB sequential write"),
            ("1m_seqread", "read", "1m", 60, None, "1MB sequential read"),
            ("1m_mixed", "readwrite", "1m", 60, 50, "1MB sequential mixed (50/50)"),
        ]
        
        for test_name, rw, bs, runtime, rwmixread, desc in tests:
            run_fio_in_qemu_parallel(qemu_info, test_name, rw, bs, runtime, rwmixread=rwmixread, description=desc)
            time.sleep(2)
        
        # Step 13: Long-term background tests on all QEMU instances
        start_fio_long_term_background(qemu_info)
        
        print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
        ColorLogger.success("Deployment and testing completed successfully!")
        print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")
        
        # Summary
        total_qemu = sum(len(pids) for pids in qemu_info.values())
        ColorLogger.info(f"QEMU instances running: {total_qemu}")
        for host, pids in qemu_info.items():
            if pids:
                disks = ", ".join([disk_id for disk_id, _ in pids])
                ColorLogger.info(f"  {host}: {disks}")
        ColorLogger.info(f"Long-term FIO tests running on all QEMU instances - check ~/fio_longterm_diskX.log")
        
    except KeyboardInterrupt:
        ColorLogger.warning("\nDeployment interrupted by user")
        sys.exit(130)
    except Exception as e:
        ColorLogger.error(f"Deployment failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
