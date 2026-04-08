#!/usr/bin/env python3
"""
Automated deployment script for YDB NBS cluster testing.
Pulls latest changes, builds, deploys, and runs tests on cluster.
"""

import subprocess
import sys
import os
import time
from pathlib import Path
from typing import List, Tuple


class DeploymentConfig:
    """Configuration for deployment"""
    REPO_PATH = Path.home() / "ydbwork" / "ydb"
    ARCADIA_PATH = Path.home() / "arcadia"
    MULTINODE_PATH = ARCADIA_PATH / "kikimr" / "tools" / "multinode_configure"
    
    WATCHED_DIRS = [
        REPO_PATH / "ydb" / "core" / "nbs",
        REPO_PATH / "ydb" / "core" / "blobstorage" / "ddisk",
    ]
    
    REMOTE_HOST = "vla5-8297.search.yandex.net"
    REMOTE_USER = os.environ.get("USER", "vazhenin-mv")
    
    GRPC_SERVER = "vla5-8297.search.yandex.net"
    GRPC_PORT = 2135
    
    DISK_ID = "disk1"
    DISK_POOL = "ddp1"


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
        host = f"{DeploymentConfig.REMOTE_USER}@{DeploymentConfig.REMOTE_HOST}"
    
    ssh_cmd = f'ssh {host} "{cmd}"'
    return run_command(ssh_cmd, check=check)


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


def create_partition():
    """Create NBS partition"""
    ColorLogger.step(9, "Creating NBS partition")
    
    cmd = (
        f"{DeploymentConfig.REPO_PATH}/ydb/apps/dstool/ydb-dstool "
        f"-d -e grpc://{DeploymentConfig.GRPC_SERVER}:{DeploymentConfig.GRPC_PORT} "
        f"nbs partition create "
        f"--pool {DeploymentConfig.DISK_POOL} "
        f"--type=ssd "
        f"--block-size 4096 "
        f"--blocks-count 1048576 "
        f"--disk-id {DeploymentConfig.DISK_ID}"
    )
    
    run_command(cmd, interactive=True)


def start_qemu_on_remote():
    """Start QEMU on remote host in background"""
    ColorLogger.step(10, "Starting QEMU on remote host")
    
    # Stop any existing qemu processes for this disk
    ColorLogger.info("Stopping existing QEMU processes")
    run_remote_command(
        f"sudo pkill -f 'qemu.*{DeploymentConfig.DISK_ID}' || true",
        check=False
    )
    time.sleep(2)
    
    # Start qemu in background using nohup and redirect output
    cmd = (
        f"cd ~/multinode_home/nbsd/bin && "
        f"nohup sudo ./run_qemu.sh -d {DeploymentConfig.DISK_ID} "
        f"> ~/qemu_{DeploymentConfig.DISK_ID}.log 2>&1 & "
        f"echo \$!"
    )
    
    rc, stdout, _ = run_remote_command(cmd)
    qemu_pid = stdout.strip()
    ColorLogger.success(f"QEMU started with PID: {qemu_pid}")
    
    # Wait for QEMU to start and SSH to be available
    ColorLogger.info("Waiting for QEMU to boot (30 seconds)...")
    time.sleep(30)
    
    return qemu_pid


def run_fio_test(test_name: str, rw: str, bs: str, runtime: int, iodepth: int = 32, 
                 rwmixread: int = None, numjobs: int = 1, description: str = "") -> bool:
    """Run a single FIO test inside QEMU VM
    
    Args:
        test_name: Name for the test
        rw: IO pattern (read, write, randread, randwrite, randrw, readwrite)
        bs: Block size (e.g., '4k', '1m')
        runtime: Runtime in seconds
        iodepth: IO depth
        rwmixread: Percentage of reads in mixed workload (for randrw/readwrite)
        numjobs: Number of parallel jobs
        description: Human-readable description
    """
    qemu_ssh_port = 8679
    qemu_user = "qemu"
    device = "/dev/vdb"
    
    ColorLogger.info(f"Running FIO test: {description or test_name}")
    
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
    
    # SSH into QEMU VM (via forwarded port on remote host)
    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"-i ~/qemu_key "
        f"-p {qemu_ssh_port} {qemu_user}@localhost "
        f"'{fio_cmd}'"
    )
    
    rc, stdout, stderr = run_remote_command(ssh_cmd, check=False)
    
    if rc == 0:
        ColorLogger.success(f"FIO test '{test_name}' completed successfully")
        print("\n" + "="*80)
        print(f"FIO Results: {description or test_name}")
        print("="*80)
        print(stdout)
        print("="*80 + "\n")
        return True
    else:
        ColorLogger.error(f"FIO test '{test_name}' failed")
        if stderr:
            print(stderr)
        return False


def run_fio_warmup():
    """Run warmup: sequential write with 1MB blocks to fill disk"""
    ColorLogger.step(11, "Running FIO warmup (sequential write 1MB blocks)")
    
    return run_fio_test(
        test_name="warmup",
        rw="write",
        bs="1m",
        runtime=300,  # 5 minutes should be enough to warm up
        iodepth=32,
        numjobs=1,
        description="Warmup: Sequential write 1MB blocks (full disk)"
    )


def run_fio_test_suite():
    """Run comprehensive FIO test suite"""
    ColorLogger.step(12, "Running FIO test suite (1 minute tests)")
    
    tests = [
        # 4K tests
        {
            "test_name": "4k_randwrite",
            "rw": "randwrite",
            "bs": "4k",
            "runtime": 60,
            "description": "4K random write"
        },
        {
            "test_name": "4k_randread",
            "rw": "randread",
            "bs": "4k",
            "runtime": 60,
            "description": "4K random read"
        },
        {
            "test_name": "4k_mixed",
            "rw": "randrw",
            "bs": "4k",
            "runtime": 60,
            "rwmixread": 50,
            "description": "4K random mixed (50% read, 50% write)"
        },
        # 1MB tests
        {
            "test_name": "1m_seqwrite",
            "rw": "write",
            "bs": "1m",
            "runtime": 60,
            "description": "1MB sequential write"
        },
        {
            "test_name": "1m_seqread",
            "rw": "read",
            "bs": "1m",
            "runtime": 60,
            "description": "1MB sequential read"
        },
        {
            "test_name": "1m_mixed",
            "rw": "readwrite",
            "bs": "1m",
            "runtime": 60,
            "rwmixread": 50,
            "description": "1MB sequential mixed (50% read, 50% write)"
        },
    ]
    
    results = []
    for test in tests:
        success = run_fio_test(**test)
        results.append((test["test_name"], success))
        if not success:
            ColorLogger.warning(f"Test {test['test_name']} failed, continuing...")
        time.sleep(2)  # Brief pause between tests
    
    # Summary
    print("\n" + "="*80)
    print("Test Suite Summary:")
    print("="*80)
    for test_name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {status}: {test_name}")
    print("="*80 + "\n")
    
    return all(success for _, success in results)


def start_fio_long_term_background():
    """Start long-term FIO test in background (360 days, 4K randwrite)"""
    ColorLogger.step(13, "Starting long-term FIO test (360 days, background)")
    
    qemu_ssh_port = 8679
    qemu_user = "qemu"
    device = "/dev/vdb"
    runtime_days = 360
    runtime_seconds = runtime_days * 24 * 3600
    
    ColorLogger.info(f"Starting background FIO: 4K randwrite for {runtime_days} days")
    
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
        f"> ~/fio_longterm.log 2>&1 & "
        f"echo \\$!"
    )
    
    # SSH into QEMU VM and start background FIO
    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"-i ~/qemu_key "
        f"-p {qemu_ssh_port} {qemu_user}@localhost "
        f"'{fio_cmd}'"
    )
    
    rc, stdout, stderr = run_remote_command(ssh_cmd, check=False)
    
    if rc == 0:
        fio_pid = stdout.strip()
        ColorLogger.success(f"Long-term FIO started in background (PID: {fio_pid})")
        ColorLogger.info(f"Duration: {runtime_days} days ({runtime_seconds} seconds)")
        ColorLogger.info("Log file: ~/fio_longterm.log (inside QEMU)")
        ColorLogger.info(f"To view: ssh -i ~/qemu_key -p {qemu_ssh_port} {qemu_user}@localhost 'tail -f ~/fio_longterm.log'")
        ColorLogger.info(f"To stop: ssh -i ~/qemu_key -p {qemu_ssh_port} {qemu_user}@localhost 'kill {fio_pid}'")
        return True
    else:
        ColorLogger.error("Failed to start long-term FIO test")
        if stderr:
            print(stderr)
        return False


def main():
    """Main deployment workflow"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Automated deployment script for YDB NBS cluster testing',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Skip git pull and change detection, force deployment'
    )
    args = parser.parse_args()
    
    print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}YDB NBS Cluster Deployment Script{ColorLogger.ENDC}")
    print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")
    
    try:
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
        
        # Step 4-9: Build and deploy on build VM
        build_tools()
        deploy_cluster()
        define_ddisk_pool()
        create_partition()
        
        # Step 10: Start QEMU
        qemu_pid = start_qemu_on_remote()
        
        # Give QEMU some time to fully boot before running FIO
        ColorLogger.info("Waiting additional time for QEMU to stabilize...")
        time.sleep(10)
        
        # Step 11: Warmup
        if not run_fio_warmup():
            ColorLogger.warning("Warmup failed, continuing anyway...")
        
        # Step 12: Test suite
        if not run_fio_test_suite():
            ColorLogger.warning("Some tests failed, continuing anyway...")
        
        # Step 13: Long-term background test
        start_fio_long_term_background()
        
        print(f"\n{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}")
        ColorLogger.success("Deployment and testing completed successfully!")
        print(f"{ColorLogger.BOLD}{'='*80}{ColorLogger.ENDC}\n")
        
        ColorLogger.info(f"QEMU running on {DeploymentConfig.REMOTE_HOST} with PID: {qemu_pid}")
        ColorLogger.info(f"View QEMU logs: ssh {DeploymentConfig.REMOTE_HOST} 'tail -f ~/qemu_{DeploymentConfig.DISK_ID}.log'")
        ColorLogger.info(f"Long-term FIO running inside QEMU - check ~/fio_longterm.log")
        
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
