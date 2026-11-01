"""
Shopify Checker & Telegram Bot — Dynamic Hardware Tuner.
Dynamically detects CPU cores, RAM limits, Docker/Coolify containers,
and applies optimal concurrency, worker counts, memory pool sizing,
and Python GC thresholds for maximum performance.
"""
import os
import sys
import gc
import logging
from typing import Dict, Any

logger = logging.getLogger("system_tuner")

_PROFILE_CACHE = None

def detect_system_resources() -> Dict[str, Any]:
    """
    Dynamically detects effective CPU cores and RAM limits, accounting for
    Linux cgroups (Docker / Coolify containers) and bare-metal environments.
    """
    # 1. Detect CPU Cores
    cpu_count = None
    
    # Check Linux cgroups v2 for CPU quota
    if os.path.exists("/sys/fs/cgroup/cpu.max"):
        try:
            with open("/sys/fs/cgroup/cpu.max", "r") as f:
                parts = f.read().strip().split()
                if len(parts) >= 2 and parts[0] != "max":
                    quota, period = float(parts[0]), float(parts[1])
                    if period > 0:
                        cpu_count = max(1, int(round(quota / period)))
        except Exception:
            pass
            
    # Check Linux cgroups v1 for CPU quota
    if cpu_count is None and os.path.exists("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"):
        try:
            with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r") as f:
                quota = float(f.read().strip())
            with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r") as f:
                period = float(f.read().strip())
            if quota > 0 and period > 0:
                cpu_count = max(1, int(round(quota / period)))
        except Exception:
            pass

    # Check Linux CPU affinity
    if cpu_count is None and hasattr(os, "sched_getaffinity"):
        try:
            cpu_count = len(os.sched_getaffinity(0))
        except Exception:
            pass

    # Fallback to standard os.cpu_count
    if cpu_count is None:
        cpu_count = os.cpu_count() or 4

    # 2. Detect RAM (in GB)
    ram_gb = None
    
    # Check Linux cgroups v2 memory limit
    if os.path.exists("/sys/fs/cgroup/memory.max"):
        try:
            with open("/sys/fs/cgroup/memory.max", "r") as f:
                val = f.read().strip()
                if val != "max":
                    limit_bytes = float(val)
                    if 0 < limit_bytes < (100 * 1024**4):  # < 100 TB sanity check
                        ram_gb = round(limit_bytes / (1024**3), 2)
        except Exception:
            pass

    # Check Linux cgroups v1 memory limit
    if ram_gb is None and os.path.exists("/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r") as f:
                limit_bytes = float(f.read().strip())
                if 0 < limit_bytes < (100 * 1024**4):
                    ram_gb = round(limit_bytes / (1024**3), 2)
        except Exception:
            pass

    # Fallback to psutil
    available_gb = 0.0
    try:
        import psutil
        vmem = psutil.virtual_memory()
        if ram_gb is None:
            ram_gb = round(vmem.total / (1024**3), 2)
        available_gb = round(vmem.available / (1024**3), 2)
    except Exception:
        if ram_gb is None:
            ram_gb = 8.0
        available_gb = ram_gb * 0.5

    # 3. Detect Platform & Container (Coolify / Docker / Bare Metal)
    is_container = False
    container_platform = "bare_metal"
    
    if os.environ.get("COOLIFY_CONTAINER_NAME") or os.environ.get("COOLIFY_APP_ID") or os.environ.get("COOLIFY_BRANCH"):
        is_container = True
        container_platform = "coolify"
    elif os.path.exists("/.dockerenv"):
        is_container = True
        container_platform = "docker"
    elif os.path.exists("/proc/1/cgroup"):
        try:
            with open("/proc/1/cgroup", "r") as f:
                cg = f.read().lower()
                if "docker" in cg or "kubepods" in cg or "containerd" in cg:
                    is_container = True
                    container_platform = "docker"
        except Exception:
            pass

    return {
        "cpu_count": int(cpu_count),
        "ram_gb": float(ram_gb),
        "available_ram_gb": float(available_gb),
        "is_container": is_container,
        "container_platform": container_platform,
        "os": sys.platform,
    }


def get_hardware_profile() -> Dict[str, Any]:
    """
    Computes an optimal hardware profile based on detected CPU cores and RAM.
    Values are cached so detection runs only once per process.
    """
    global _PROFILE_CACHE
    if _PROFILE_CACHE is not None:
        return _PROFILE_CACHE

    res = detect_system_resources()
    cpu = res["cpu_count"]
    ram = res["ram_gb"]

    # 1. Granian / ASGI Web Server tuning: scale across available CPU cores up to 8
    granian_workers = min(8, max(1, cpu))
    granian_threads = min(8, max(1, cpu))

    # 2. AsyncIO ThreadPoolExecutor size (prevents thread starvation during concurrent DB/DNS lookups)
    thread_pool_workers = min(256, max(64, cpu * 16))

    # 3. Telegram Bot Connection Pool
    telegram_pool_size = 300 if cpu >= 8 else (200 if cpu >= 4 else 100)

    # 4. Shopify Checker Concurrency & Memory Pool Tuning
    # High-throughput concurrency: maximizes cards/sec without exhausting proxies.
    # Worker caps are now dynamically scaled to hardware profile.
    if cpu >= 12 and ram >= 20:
        mass_max_workers = 150
        prewarm_max_workers = 80
        checkout_max_workers = 60
        session_pool_max = 200
        gc_thresholds = (8000, 15, 15)
    elif cpu >= 8 and ram >= 14:
        mass_max_workers = 100
        prewarm_max_workers = 60
        checkout_max_workers = 50
        session_pool_max = 120
        gc_thresholds = (5000, 10, 10)
    elif cpu >= 4 and ram >= 6:
        mass_max_workers = 75
        prewarm_max_workers = 50
        checkout_max_workers = 40
        session_pool_max = 90
        gc_thresholds = (4000, 10, 10)
    elif cpu >= 2 and ram >= 3:
        mass_max_workers = 50
        prewarm_max_workers = 35
        checkout_max_workers = 30
        session_pool_max = 60
        gc_thresholds = (3000, 10, 10)
    else:
        mass_max_workers = 30
        prewarm_max_workers = 20
        checkout_max_workers = 15
        session_pool_max = 35
        gc_thresholds = (2000, 10, 10)

    _PROFILE_CACHE = {
        **res,
        "granian_workers": granian_workers,
        "granian_threads": granian_threads,
        "thread_pool_workers": thread_pool_workers,
        "telegram_pool_size": telegram_pool_size,
        "mass_max_workers": mass_max_workers,
        "prewarm_max_workers": prewarm_max_workers,
        "checkout_max_workers": checkout_max_workers,
        "session_pool_max": session_pool_max,
        "gc_thresholds": gc_thresholds,
    }
    return _PROFILE_CACHE


def apply_hardware_optimizations() -> Dict[str, Any]:
    """
    Applies runtime optimizations (GC tuning, memory limits) according to the
    detected hardware profile and returns the active profile.
    """
    profile = get_hardware_profile()
    
    # Apply Python Garbage Collector tuning
    try:
        gc.set_threshold(*profile["gc_thresholds"])
    except Exception as e:
        logger.debug(f"Failed to set GC threshold: {e}")

    platform_desc = profile["container_platform"].upper()
    if profile["is_container"]:
        platform_desc = f"Container ({platform_desc})"
    else:
        platform_desc = f"Host ({profile['os']})"

    logger.info(
        f"[TUNER] Hardware profile: {profile['cpu_count']} CPUs | {profile['ram_gb']}GB RAM | {platform_desc} | "
        f"Workers: Mass={profile['mass_max_workers']}, Prewarm={profile['prewarm_max_workers']}, Check={profile['checkout_max_workers']} | "
        f"GC={profile['gc_thresholds']}"
    )
    return profile


if __name__ == "__main__":
    prof = apply_hardware_optimizations()
    print("Detected System Profile:")
    for k, v in prof.items():
        print(f"  {k}: {v}")
