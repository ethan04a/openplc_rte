from dataclasses import dataclass, field
from enum import Enum, auto
import glob
import io
import os
import shutil
import subprocess
import threading
import zipfile
from pathlib import Path
from typing import Final

from webserver.runtimemanager import RuntimeManager
from webserver.logger import get_logger, LogParser
from webserver.plugin_config_model import PluginsConfiguration, PluginConfig

logger, _ = get_logger("runtime", use_buffer=True)

OPENPLC_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
LAST_UPLOADED_PROGRAM_ZIP: Final[Path] = OPENPLC_ROOT / "last_uploaded_program.zip"

MAX_FILE_SIZE: Final[int] = 10 * 1024 * 1024   # 10 MB per file
MAX_TOTAL_SIZE: Final[int] = 50 * 1024 * 1024  # 50 MB total
DISALLOWED_EXT = (".exe", ".dll", ".sh", ".bat", ".js", ".vbs", ".scr")

class BuildStatus(Enum):
    IDLE = auto()
    UNZIPPING = auto()
    COMPILING = auto()
    SUCCESS = auto()
    FAILED = auto()

@dataclass
class BuildProcess:
    status: BuildStatus = BuildStatus.IDLE
    logs: list[str] = field(default_factory=list)
    exit_code: int | None = None

    def log(self, msg: str):
        # logger.info(msg)
        self.logs.append(msg)

    def clear(self):
        self.status = BuildStatus.IDLE
        self.logs.clear()
        self.exit_code = None


build_state = BuildProcess()  # global-ish singleton for status


def analyze_zip(zip_path) -> tuple[bool, list]:
    """Analyze the ZIP file for safety before extraction."""
    build_state.status = BuildStatus.UNZIPPING

    if hasattr(zip_path, "seek"):
        zip_path.seek(0)

    if not zipfile.is_zipfile(zip_path):
        build_state.log("[ERROR] Not a valid PLC Program file.\n")
        return False, []

    with zipfile.ZipFile(zip_path, "r") as zf:
        total_size = 0
        safe = True
        valid_files = []

        for info in zf.infolist():
            filename = info.filename
            uncompressed_size = info.file_size
            compressed_size = info.compress_size
            ext = os.path.splitext(filename)[1].lower()

            # Check for path traversal or absolute paths
            if filename.startswith("/") or ".." in filename or ":" in filename:
                # logger.warning("Dangerous path: %s", filename)
                safe = False

            # Check uncompressed size
            if uncompressed_size > MAX_FILE_SIZE:
                logger.warning("File too large: %s (%d bytes)",
                                filename, uncompressed_size)
                safe = False

            # Check compression ratio (ZIP bomb detection)
            if compressed_size > 0 and uncompressed_size / compressed_size > 1000:
                # logger.warning("Suspicious compression ratio in %s",
                            #    filename)
                safe = False

            # Check disallowed extensions
            if ext in DISALLOWED_EXT:
                logger.warning("Disallowed extension: %s",
                                filename)
                safe = False

            total_size += uncompressed_size
            valid_files.append(info)

        # Check total size
        if total_size > MAX_TOTAL_SIZE:
            # logger.warning("Total uncompressed size too large: %d bytes", 
            #                total_size)
            safe = False

        if safe:
            logger.debug("ZIP file looks safe to extract (based on static checks).")
        else:
            logger.warning("ZIP file failed safety checks.")

        return safe, valid_files


def safe_extract(zip_path, dest_dir, valid_files):
    """Extract files safely to a target directory.
    - Skips macOS metadata (__MACOSX, .DS_Store)
    - Auto-strips a single common root folder if present
    """
    build_state.status = BuildStatus.UNZIPPING

    if hasattr(zip_path, "seek"):
        zip_path.seek(0)

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Detect roots (ignoring macOS junk)
        roots = set()
        for info in valid_files:
            if info.filename.startswith("__MACOSX/") or info.filename.endswith(".DS_Store"):
                continue
            parts = info.filename.split("/", 1)
            if parts and parts[0]:
                roots.add(parts[0])
        strip_root = len(roots) == 1

        for info in valid_files:
            filename = info.filename

            # Normalize path separators for cross-platform compatibility (Windows \ to Unix /)
            filename = filename.replace('\\', '/')

            # Skip macOS junk and directories
            if filename.startswith("__MACOSX/") or filename.endswith(".DS_Store") or filename.endswith("/"):
                continue

            # Optionally strip single root folder
            if strip_root:
                parts = filename.split("/", 1)
                if len(parts) == 2:
                    filename = parts[1]
                else:
                    filename = parts[0]

            out_path = os.path.join(dest_dir, filename)
            out_path = os.path.abspath(out_path)

            # Ensure extraction stays inside destination
            if not out_path.startswith(os.path.abspath(dest_dir)):
                # logger.warning("Skipping suspicious path: %s", filename)
                continue

            os.makedirs(os.path.dirname(out_path), exist_ok=True)

            with zf.open(info) as src, open(out_path, "wb") as dst:
                dst.write(src.read())

            logger.debug("Extracted: %s", out_path)


def update_plugin_configurations(generated_dir: str = "core/generated"):
    """
    Update plugin configurations based on available config files.
    
    Scans generated/conf/ for config files, copies them to plugin directories,
    and updates plugins.conf to enable/disable plugins accordingly.
    """
    plugins_conf_path = "plugins.conf"
    conf_dir = os.path.join(generated_dir, "conf")

    build_state.log(f"[DEBUG] update_plugin_configurations called with generated_dir='{generated_dir}'\n")
    build_state.log(f"[DEBUG] Looking for config files in: {conf_dir}\n")

    # Load current plugin configuration using the dataclass
    plugins_config = PluginsConfiguration.from_file(plugins_conf_path)
    build_state.log(f"[DEBUG] Loaded {len(plugins_config.plugins)} plugins from {plugins_conf_path}\n")
    
    # Log initial state
    for plugin in plugins_config.plugins:
        build_state.log(f"[DEBUG] Initial state - {plugin.name}: enabled={plugin.enabled}, config_path='{plugin.config_path}'\n")

    # Check if conf directory exists
    if not os.path.exists(conf_dir):
        build_state.log(f"[INFO] No conf directory found in {generated_dir}, disabling all plugins\n")
        # When there's no conf directory, disable all currently enabled plugins
        plugins_updated = 0
        update_messages = []
        for plugin in plugins_config.plugins:
            if plugin.enabled:
                plugin.enabled = False
                plugins_updated += 1
                update_messages.append(f"Disabled plugin '{plugin.name}' (no conf directory found)")
        
        # Log the updates
        build_state.log(f"[INFO] Found 0 config files (no conf directory): []\n")
        
        for message in update_messages:
            build_state.log(f"[INFO] {message}\n")
    else:
        # Process config files normally when conf directory exists
        # Use the utility method to update plugins based on available config files
        # Copy config files to plugin directories instead of referencing them directly
        plugins_updated, update_messages = plugins_config.update_plugins_from_config_dir(conf_dir, copy_to_plugin_dirs=True)
        
        # Log the updates
        config_files = glob.glob(os.path.join(conf_dir, "*.json"))
        available_configs = {os.path.splitext(os.path.basename(f))[0]: f for f in config_files}
        build_state.log(f"[INFO] Found {len(available_configs)} config files in {conf_dir}: {list(available_configs.keys())}\n")
        
        for message in update_messages:
            if "Copied config file" in message:
                build_state.log(f"[INFO] {message}\n")
            elif "Enabled plugin" in message or "Disabled plugin" in message:
                build_state.log(f"[INFO] {message}\n")
            else:
                build_state.log(f"[WARNING] {message}\n")

    # Save the updated configuration
    if plugins_config.to_file(plugins_conf_path):
        build_state.log(f"[INFO] Plugin configuration update complete. {plugins_updated} plugins updated.\n")
        
        # Log final state
        for plugin in plugins_config.plugins:
            build_state.log(f"[DEBUG] Final state - {plugin.name}: enabled={plugin.enabled}, config_path='{plugin.config_path}'\n")
        
        # Log configuration summary
        summary = plugins_config.get_config_summary()
        build_state.log(f"[INFO] Plugin summary: {summary['enabled']}/{summary['total']} enabled "
                       f"({summary['python']} Python, {summary['native']} Native)\n")
        
        # Validate configurations and log any issues
        issues = plugins_config.validate_plugins()
        if issues:
            build_state.log("[WARNING] Plugin validation issues found:\n")
            for issue in issues:
                build_state.log(f"[WARNING] {issue}\n")
    else:
        build_state.log("[ERROR] Failed to save updated plugin configuration\n")


def run_compile(runtime_manager: RuntimeManager, cwd: str = "core/generated"):
    """Run compile script synchronously (wait for completion) and update status/logs."""
    script_path: str = "./scripts/compile.sh"

    build_state.status = BuildStatus.COMPILING
    build_state.log(f"[INFO] Starting compilation\n")

    def stream_output(pipe, prefix):
        for line in iter(pipe.readline, ''):
            msg = f"{prefix}{line}"
            build_state.log(msg)
        pipe.close()

    def wait_and_finish(proc: subprocess.Popen, step_name: str):
        exit_code = proc.wait()
        build_state.exit_code = exit_code
        if exit_code == 0:
            build_state.status = BuildStatus.SUCCESS
            build_state.log(f"[INFO] {step_name} finished successfully\n")
        else:
            build_state.status = BuildStatus.FAILED
            build_state.log(f"[ERROR] {step_name} failed (exit={exit_code})\n")

    # --- Compile step ---
    compile_proc = subprocess.Popen(
        ["bash", script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    threading.Thread(target=stream_output, args=(compile_proc.stdout, ""), daemon=True).start()
    threading.Thread(target=stream_output, args=(compile_proc.stderr, "[ERROR] "), daemon=True).start()

    # Block until compile finishes
    wait_and_finish(compile_proc, "Build")

    # Stop PLC before cleanup
    runtime_manager.stop_plc()

    # --- Cleanup step ---
    cleanup_proc = subprocess.Popen(
        ["bash", "./scripts/compile-clean.sh"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    threading.Thread(target=stream_output, args=(cleanup_proc.stdout, ""), daemon=True).start()
    threading.Thread(target=stream_output, args=(cleanup_proc.stderr, "[ERROR] "), daemon=True).start()

    # Block until cleanup finishes
    wait_and_finish(cleanup_proc, "Cleanup")

    # Restart PLC only if everything succeeded
    if build_state.status == BuildStatus.SUCCESS:
        runtime_manager.reset_crash_tracking()
        runtime_manager.start_plc()
        try:
            from webserver.redundancy_program_sync import schedule_master_to_standby_sync

            schedule_master_to_standby_sync(runtime_manager)
        except Exception as e:
            logger.error("[热冗余] 无法排程备机程序同步: %s", e)
    else:
        build_state.log("[WARNING] PLC program has not been updated because the build failed\n")


def apply_program_zip_upload(runtime_manager: RuntimeManager, zip_bytes: bytes) -> dict:
    """
    Validate ZIP bytes, extract to core/generated, update plugins conf, start compile thread.
    Caller must clear build_state / gate on COMPILING as appropriate.
    """
    if len(zip_bytes) > MAX_FILE_SIZE:
        build_state.status = BuildStatus.FAILED
        return {
            "UploadFileFail": "File is too large",
            "CompilationStatus": build_state.status.name,
        }

    try:
        LAST_UPLOADED_PROGRAM_ZIP.parent.mkdir(parents=True, exist_ok=True)
        LAST_UPLOADED_PROGRAM_ZIP.write_bytes(zip_bytes)

        bio = io.BytesIO(zip_bytes)
        safe, valid_files = analyze_zip(bio)
        if not safe:
            build_state.status = BuildStatus.FAILED
            return {
                "UploadFileFail": "Uploaded ZIP file failed safety checks",
                "CompilationStatus": build_state.status.name,
            }

        extract_dir = "core/generated"
        if os.path.exists(extract_dir):
            shutil.rmtree(extract_dir)

        bio.seek(0)
        safe_extract(bio, extract_dir, valid_files)

        update_plugin_configurations(extract_dir)

        build_state.status = BuildStatus.COMPILING

        task_compile = threading.Thread(
            target=run_compile,
            args=(runtime_manager,),
            kwargs={"cwd": extract_dir},
            daemon=True,
        )
        task_compile.start()

        return {"UploadFileFail": "", "CompilationStatus": build_state.status.name}

    except (OSError, IOError) as e:
        build_state.status = BuildStatus.FAILED
        build_state.log(f"[ERROR] File system error: {e}")
        return {
            "UploadFileFail": f"File system error: {e}",
            "CompilationStatus": build_state.status.name,
        }
    except Exception as e:
        build_state.status = BuildStatus.FAILED
        build_state.log(f"[ERROR] Unexpected error: {e}")
        return {
            "UploadFileFail": f"Unexpected error: {e}",
            "CompilationStatus": build_state.status.name,
        }
