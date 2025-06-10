import datetime
import json
from typing import Any

from dagster import InputContext, OutputContext, UPathIOManager
from upath import UPath

from .io_manager_factories import S3UPathIOManagerFactory


class JsonObjectIOManager(UPathIOManager):
    """IO manager for handling JSON files on S3.

    This class manages the reading and writing of JSON files to and from S3 storage.
    It is intended to be used directly as an IO manager for JSON assets.

    Attributes:
        extension (str): The file extension used for JSON files (default: ".json").
    """
    extension: str | None = ".json"

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        """Load a JSON-serializable object from S3 path.

        Args:
            context: Input context from Dagster.
            path: Path to the JSON file on S3.

        Returns:
            Any: Deserialized JSON object.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is not valid JSON.
        """
        try:
            with path.open('r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Could not find file {path}') from e
        except json.JSONDecodeError as e:
            raise ValueError(f'File {path} does not contain valid JSON: {e}') from e

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        """Save a JSON-serializable object to S3 path.

        Args:
            context: Output context from Dagster.
            obj: Object to save (must be JSON-serializable).
            path: Path to save the JSON file on S3.

        Raises:
            TypeError: If the object is not JSON-serializable.
        """
        try:
            json_str = json.dumps(obj, ensure_ascii=False, indent=2)
        except TypeError as e:
            raise TypeError(f'Object is not JSON-serializable: {e}') from e

        # Save the main file
        with path.open('w', encoding='utf-8') as f:
            f.write(json_str)

        # Save a timestamped backup if versioning is enabled
        if context.op_def.tags.get('activate_versioning', False):
            backup_path = path.parent / f'{datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")}_{path.name}'
            with backup_path.open('w', encoding='utf-8') as f:
                f.write(json_str)

class S3JsonIOManager(S3UPathIOManagerFactory):
    """IO manager for handling Json files on S3."""
    def io_manager_class(self):
        """Return the Json file IO manager class."""
        return JsonObjectIOManager
