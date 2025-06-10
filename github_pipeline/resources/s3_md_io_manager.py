import datetime
from typing import Any

from dagster import InputContext, OutputContext, UPathIOManager
from upath import UPath

from .io_manager_factories import S3UPathIOManagerFactory


class MdObjectIOManager(UPathIOManager):
    """Custom Dagster IOManager for writing/reading text files to/from S3 using UPath."""

    extension: str | None = '.md'

    def load_from_path(self, context: InputContext, path: UPath) -> str:
        """Load text content from S3 path.

        Args:
            context (InputContext): The context object from the asset function,
                which consumes the loaded asset as input.
            path (UPath): The path from which to load the text file on S3.

        Returns:
            str: The text content read from the file at the given S3 path.

        Raises:
            FileNotFoundError: If the file does not exist at the specified path.
            Exception: If there is an error reading the file from S3.
        """
        try:
            with path.open('r', encoding='utf-8') as file:
                return file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f'Could not find file at path: {path}')
        except Exception as e:
            raise Exception(f'Error reading file at path {path}: {e}')

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Save text content to S3 path.

        Args:
            context (OutputContext): The context object from the asset function.
            obj (Any): The text to save (must be a string).
            path (UPath): The path to save the file at.

        Raises:
            ValueError: If the object is not a string.
            Exception: If there is an error writing the file to S3.
        """
        try:
            # Save the main file
            with path.open('w', encoding='utf-8') as file:
                file.write(obj)

            # Save a timestamped backup if versioning is enabled
            if context.op_def.tags.get('activate_versioning', False):
                backup_path = (
                    path.parent /
                    f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")}_{path.name}'
                )
                with backup_path.open('w', encoding='utf-8') as file:
                    file.write(obj)
        except Exception as e:
            raise Exception(f'Error writing file to path {path}: {e}')

class S3MdIOManager(S3UPathIOManagerFactory):
    """IO manager for handling Markdown files on S3."""
    def io_manager_class(self):
        """Return the Markdown file IO manager class."""
        return MdObjectIOManager
