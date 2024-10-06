"""This module contains the different execution environments."""
from __future__ import annotations

import os
import shutil
import subprocess
from contextlib import contextmanager, ExitStack
from logging import Logger
from pathlib import Path
from subprocess import CalledProcessError, PIPE, STDOUT
from typing import TYPE_CHECKING

from fsstratify.errors import ConfigurationError, SimulationError
from fsstratify.filesystems import (
    SimulationVirtualFileSystem,
    set_simulation_mount_point,
)
from fsstratify.operations import Operation
from fsstratify.platforms import Platform, get_current_platform

if TYPE_CHECKING:
    from fsstratify.configuration import Configuration
from fsstratify.utils import get_logger, run_diskpart_script, format_mkfs_error
from fsstratify.volumes import (
    LinuxRawDiskImage,
    WindowsRawDiskImage,
)

forbidden_formatting_parameters = {
    "diskpart": ["FS="],
    "mkfs.fat": ["-C"],
    "mkfs.ntfs": ["--force"],
}


class ExecutionEnvironment:
    """Execution environment base class.

    This class defines the interface an execution environment has to implement.
    Moreover, it already implements some basic functionality which is common for almost
    all execution environments.
    """

    def __init__(self, config: Configuration):
        self._config: Configuration = config
        self._image = None
        self._context_stack: ExitStack = ExitStack()
        self._logger: Logger = get_logger(
            name="ExecutionEnvironment",
            loglevel=self._config["log_level"],
            logfile=self._config["simulation_log"],
        )

    def __del__(self):
        for h in list(self._logger.handlers):
            h.close()
            self._logger.removeHandler(h)

    def execute(self, operation: Operation):  # pragma: no cover
        """Execute the given operation.

        This method has to be implemented by the child classes of the
        ExecutionEnvironment.
        """
        raise NotImplementedError

    def get_simulation_vfs(self) -> SimulationVirtualFileSystem:
        """Return an instance of the simulation virtual file system."""
        return SimulationVirtualFileSystem(
            self._image,
            self._config["mount_point"],
            self._config["file_system"]["type"].lower(),
        )

    def flush_simulation_vfs(self) -> None:
        """Flush the simulation file system."""
        self._image.flush()

    def __enter__(self):  # pragma: no cover
        raise NotImplementedError

    def __exit__(self, exc_type, exc_val, exc_tb):  # pragma: no cover
        raise NotImplementedError


class WindowsEnvironment(ExecutionEnvironment):
    def execute(self, operation: Operation):
        operation.execute()
        self._image.flush()

    def _check_drive_letter(self):
        drive_letter = self._config["volume"]["win_drive_letter"]
        if not (len(drive_letter) == 1 and drive_letter.isalpha()):
            raise ConfigurationError("Invalid drive letter configutarion")
        if Path(f"{drive_letter}:\\").is_dir():
            raise ConfigurationError("Drive letter already active")

    @classmethod
    def _vdisk_select_cmd(cls, vol_path: Path):
        return f'SELECT VDISK FILE="{vol_path}"\n'

    @classmethod
    def _disk_select_cmd(cls, disk_id: int):
        return f"SELECT DISK={disk_id}\n"

    @classmethod
    def attach_vdisk(cls, vol_path: Path):
        run_diskpart_script(f'SELECT VDISK FILE="{vol_path}"\n' "ATTACH VDISK\n" "EXIT")

    @classmethod
    def format_and_mount(
        cls,
        dev_select_cmd,
        mnt_path: Path,
        drive_letter,
        size,
        fs_type,
        formatting_parameters,
    ):
        # mount must happen in same command as formatting
        run_diskpart_script(
            dev_select_cmd
            + f"CREATE PARTITION PRIMARY SIZE={int(size / (1024 * 1024))}\n"
            f"FORMAT FS={fs_type} {formatting_parameters}\n"
            f'ASSIGN MOUNT="{mnt_path}"\n'
            f"ASSIGN LETTER={drive_letter}\n"
            "EXIT"
        )

    @classmethod
    def only_mount(
        cls,
        dev_select_cmd,
        mnt_path: Path,
        drive_letter,
        size,
        fs_type,
        formatting_parameters,
        volume_number
    ):
        # mount must happen in same command as formatting
        run_diskpart_script(
            dev_select_cmd
            + f'SELECT VOLUME "{volume_number}"\n'
            f'ASSIGN MOUNT="{mnt_path}"\n'
            f"ASSIGN LETTER={drive_letter}\n"
            "EXIT"
        )

    @classmethod
    def unmount(cls, mnt_path: Path):
        run_diskpart_script(f'SELECT VOLUME="{mnt_path}"\n' "REMOVE ALL \n" "EXIT")

    @classmethod
    def detach_vdisk(cls, vol_path: Path):
        run_diskpart_script(
            f'SELECT VDISK FILE="{vol_path}" \n' "DETACH VDISK \n" "EXIT"
        )

    @contextmanager
    def _manage_format_and_mount(self):
        mnt_path = self._config["mount_point"]
        drive_letter = self._config["volume"]["win_drive_letter"]
        size = self._config["volume"]["size"]
        fs_type = self._config["file_system"]["type"]
        formatting_parameters = self._config["file_system"]["formatting_parameters"]
        volume_number = self._config["volume"]["volume_number"]
        self._logger.info(
            f"Formatting and mounting file system at byte offset {self._image.get_fs_offset()}"
        )
        if type(self._image) == WindowsRawDiskImage:
            select_cmd = self.__class__._vdisk_select_cmd(self._image.path)
        else:
            select_cmd = self.__class__._disk_select_cmd(
                self._config["volume"]["disk_num"]
            )
        if not self._config["volume"]["use_again"]:
            try:
                WindowsEnvironment.format_and_mount(
                    select_cmd, mnt_path, drive_letter, size, fs_type, formatting_parameters
                )
            except CalledProcessError as err:
                raise SimulationError(f"Unable to format and mount image. {err}") from err
        else:
            try:
                WindowsEnvironment.only_mount(
                    select_cmd, mnt_path, drive_letter, size, fs_type, formatting_parameters,
                    volume_number
                )
            except CalledProcessError as err:
                raise SimulationError(f"Unable to format and mount image. {err}") from err
        set_simulation_mount_point(self._config["mount_point"])
        yield
        self._logger.info("Unmounting file system")
        try:
            WindowsEnvironment.unmount(mnt_path)
        except CalledProcessError as err:
            raise SimulationError(f"Unable to unmount image. {err}") from err
        set_simulation_mount_point(None)

    @contextmanager
    def _manage_vdisk(self):
        vol_path = self._image.path
        self._logger.info("Attaching image file to system")
        try:
            WindowsEnvironment.attach_vdisk(vol_path)
        except CalledProcessError as err:
            raise SimulationError(f"Unable to attach image as vdisk. {err}") from err
        yield
        self._logger.info("Detaching image file from system")
        try:
            WindowsEnvironment.detach_vdisk(vol_path)
        except CalledProcessError as err:
            raise SimulationError(f"Unable to detach image from system. {err}") from err

    @contextmanager
    def _manage_mnt_dir(self):
        self._logger.info("Creating mount point")
        self._config["mount_point"].mkdir()
        yield
        self._logger.info("Removing mount point")
        shutil.rmtree(self._config["mount_point"])

    def __enter__(self):
        self._logger.info("Setting up the execution environment")
        volume_type = self._config["volume"]["type"]
        if volume_type not in [
            "file",
        ]:
            raise ConfigurationError(
                f'Unsupported volume type "{volume_type}" for the Windows execution environment.'
            )
        illegal_formatting_conf = set(
            self._config["file_system"]["formatting_parameters"].split()
        ) & set(forbidden_formatting_parameters["diskpart"])
        if len(illegal_formatting_conf) > 0:
            raise ConfigurationError(
                f"Following formatting parameters are used but not allowed for diskpart: {illegal_formatting_conf}"
            )
        self._check_drive_letter()  # check if drive letter available. This is done here and not earlier for the check to be closer to the actual drive letter usage

        if volume_type == "file":  # build context for file based volume
            self._image = self._context_stack.enter_context(
                WindowsRawDiskImage(self._config["volume"])
            )
            self._context_stack.enter_context(self._manage_vdisk())
            self._context_stack.enter_context(self._manage_mnt_dir())
            self._context_stack.enter_context(self._manage_format_and_mount())
            self._image.flush()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logger.info("Cleaning up the execution environment")
        self._context_stack.close()


class LinuxEnvironment(ExecutionEnvironment):
    """Linux execution environment."""

    def execute(self, operation: Operation) -> None:
        operation.execute()
        self._image.flush()

    def __enter__(self):
        self._logger.info("Setting up the execution environment")
        volume_type = self._config["volume"]["type"]
        if volume_type == "file":
            self._image = self._context_stack.enter_context(
                LinuxRawDiskImage(self._config["volume"])
            )
        else:
            raise ConfigurationError(
                f'Unsupported volume type "{volume_type}" for the Linux execution'
                " environment."
            )
        if not self._config["volume"]["use_again"]:
            self._format_file_system(self._image.path)
        self._image.flush()
        self._context_stack.enter_context(self._create_mount_point())
        self._context_stack.enter_context(self._mount_file_system(self._image))
        self._image.mount_point = self._config["mount_point"]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logger.info("Cleaning up the execution environment")
        self._context_stack.close()

    def _format_file_system(self, path: Path):
        self._logger.info("Formatting file system")
        mkfs_command = self._get_mkfs_command(path)
        try:
            res = subprocess.run(
                mkfs_command, stdout=PIPE, stderr=STDOUT, text=True, check=True
            )
            self._logger.info("%s output:\n%s", mkfs_command[0], res.stdout.strip())
        except CalledProcessError as err:
            mkfs_err = format_mkfs_error(mkfs_command[0], err)
            err_msg = "\n".join(
                ("Unable to format file system. Error message:", mkfs_err)
            )
            raise SimulationError(err_msg) from err

    @contextmanager
    def _create_mount_point(self):
        self._logger.info("Creating mount point")
        self._config["mount_point"].mkdir()
        yield
        self._logger.info("Removing mount point")
        shutil.rmtree(self._config["mount_point"])

    @contextmanager
    def _mount_file_system(self, volume):
        self._logger.info("Mounting file system")
        filesystem = self._config["file_system"]["type"]
        # mount_options = "loop,dirsync"
        mount_options = "loop,nodiscard"
        if filesystem in ("fat12", "fat16", "fat32", "ntfs"):
            mount_options = f"{mount_options},uid={os.getuid()}"
        try:
            subprocess.run(
                (
                    "mount",
                    "-o",
                    mount_options,
                    volume.path,
                    self._config["mount_point"],
                ),
                capture_output=True,
                text=True,
                check=True,
            )
            subprocess.run(
                ["chown", "-R", f"{os.getuid()}", self._config["mount_point"]],
                check=True,
            )
        except CalledProcessError as err:
            raise SimulationError(f"Unable to mount image. {err}") from err
        set_simulation_mount_point(self._config["mount_point"])
        yield
        self._logger.info("Unmounting file system")
        set_simulation_mount_point(None)
        try:
            subprocess.run(
                [
                    "umount",
                    "--force",
                    "--detach-loop",
                    self._config["mount_point"],
                ],
                check=True,
            )
        except CalledProcessError as err:
            raise SimulationError(f"Unable to unmount image. {err}") from err

    def _get_mkfs_command(self, path: Path):
        filesystem = self._config["file_system"]["type"]
        try:
            command = {
                "ext2": [
                    "mkfs.ext2",
                ],
                "ext3": [
                    "mkfs.ext3",
                ],
                "ext4": [
                    "mkfs.ext4",
                ],
                "fat12": [
                    "mkfs.fat",
                    "-F",
                    "12",
                ],
                "fat16": [
                    "mkfs.fat",
                    "-F",
                    "16",
                ],
                "fat32": [
                    "mkfs.fat",
                    "-F",
                    "32",
                ],
                "ntfs": ["mkfs.ntfs", "--force"],
            }[filesystem]
            forbidden_params = (
                forbidden_formatting_parameters[command[0]]
                if command[0] in forbidden_formatting_parameters.keys()
                else []
            )
            illegal_formatting_conf = set(
                self._config["file_system"]["formatting_parameters"].split()
            ) & set(forbidden_params)
            if len(illegal_formatting_conf) > 0:
                raise ConfigurationError(
                    f"Following formatting parameters are used but not allowed for {command[0]}: {illegal_formatting_conf}"
                )
            command.extend(self._config["file_system"]["formatting_parameters"].split())
            command.append(path)
            return command
        except KeyError:
            msg = (
                f'File system "{filesystem}" is currently not supported by the Linux'
                " execution environment"
            )
            raise ConfigurationError(msg) from None


def get_execution_environment(config: Configuration) -> ExecutionEnvironment:
    """Return an execution environment depending on the platform the simulation runs on.

    The function gets the platform the simulation is started on and creates and returns
    an instance of the corresponding execution environment.
    """
    system = get_current_platform()
    if system == Platform.LINUX:
        return LinuxEnvironment(config)
    if system == Platform.WINDOWS:
        return WindowsEnvironment(config)
    raise SimulationError(f'Unsupported platform "{system}"')
