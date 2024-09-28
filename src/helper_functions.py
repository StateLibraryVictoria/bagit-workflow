import os
import json
import uuid
import logging

headers = json.loads(os.getenv("REQUIRED_HEADERS"))
logger = logging.getLogger(__name__)


class TriggerFile:
    def __init__(self, filename):
        self
        self.filename = filename
        self.name, self.status = os.path.splitext(filename)
        if not self.status == ".ok":
            raise ValueError("Only processes .ok files!")
        self.metadata = self.load_metadata()
        self.required_headers = headers

    def load_metadata(self):
        with open(self.filename) as f:
            metadata = json.loads(f.read())
        return metadata

    def get_metadata(self):
        return self.metadata

    def get_directory(self):
        return self.name

    def validate(self):
        logger.info(f"Verifying transfer: {self.name}")
        errors = []
        if not self._exists():
            errors.append("Folder does not exist.")
        elif not self._has_data():
            errors.append("Folder is empty.")
        if not self._valid_headers():
            errors.append("Headers do not match config.")
        if not self._check_metadata():
            errors.append("Error parsing metadata. Are your values filled in?")

        if len(errors) == 0:
            logger.info(f"Transfer verified: {self.name}")
            return True
        else:
            logger.info(
                f"Issues identified while processing {self.filename}: {' '.join(errors)}"
            )
            self._set_status(".error")
            try:
                with open(self.filename, "w") as f:
                    for error in errors:
                        f.write(error + "\n")
                    f.write(self._build_default_metadata())
            except PermissionError as e:
                logger.error(f"Error writing to file: {e}")
            return False

    def _valid_headers(self):
        if self.metadata is None:
            return False
        headers = list(self.metadata.keys())
        headers.sort()
        self.required_headers.sort()
        if headers != self.required_headers:
            logger.warning(
                f"Headers don't match config with headers: {', '.join(headers)}; required: {', '.join(self.required_headers)}"
            )
            return False
        else:
            return True

    def _set_status(self, new_status):
        new_name = f"{self.name}{new_status}"
        os.rename(self.filename, new_name)
        logger.info(f"Renaming file to .error file: {new_name}")
        self.status = new_status
        self.filename = new_name

    def _exists(self):
        return os.path.exists(self.name)

    def _has_data(self):
        return len(os.listdir(self.name)) > 0

    def _check_metadata(self):
        if self.metadata == None:
            logger.error("Metadata could not be parsed from file.")
            return False
        for key in self.required_headers:
            result = self.metadata.get(key)
            if result == None or result == "" or result == "Input this value.":
                logger.info(f"Metadata tag {key} was not parsed.")
                return False
        return True

    def _build_default_metadata(self):
        data = {}
        for key in self.required_headers:
            if self.metadata is not None and self.metadata.get(key) is not None:
                data.update({key: self.metadata.get(key)})
            else:
                data.update({key: "Input this value."})
        return json.dumps(data)
