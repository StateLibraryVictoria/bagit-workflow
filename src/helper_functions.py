import os
import json
import uuid
import bagit
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
        bag_info = os.path.join(self.name, "bag-info.txt")
        if os.path.isfile(bag_info):
            logging.info(
                "Existing bag identified. Will use bag metadata. Delete bag-info.txt to run a bag with supplied metadata."
            )
            bag = bagit.Bag(self.name)
            metadata = bag.info
        else:
            with open(self.filename) as f:
                try:
                    metadata = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Failed to parse metadata from file {self.filename}: {e}"
                    )
                    metadata = None
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
        valid_headers = True
        for header in self.required_headers:
            if header not in headers:
                valid_headers = False
                logger.warning(f"Missing mandatory header: {header}")
        return valid_headers

    def _set_status(self, new_status):
        new_name = f"{self.name}{new_status}"
        os.rename(self.filename, new_name)
        logger.info(f"Renaming file to {new_status} file: {new_name}")
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


class MetadataChecker:
    def __init__(self):
        self.required_headers = headers

    def validate(self, metadata):
        if self._valid_headers(metadata):
            return self._has_uuid(metadata)
        else:
            return None

    def _has_uuid(self, metadata):
        parsed_uuid = []
        mids = metadata.get("External-Identifier")
        ids = []
        if type(ids) == str:
            ids = list(ids)
        if mids is not None:
            if type(mids) != list:
                ids.append(mids)
            else:
                ids = mids
            for id in ids:
                try:
                    test_uuid = uuid.UUID(id)
                    parsed_uuid.append(test_uuid)
                    logger.debug(f"ID {id} is a uuid")
                except Exception as e:
                    logger.debug(f"Failed to parse uuid from {id}")
        if len(parsed_uuid) == 0:
            transfer_id = str(uuid.uuid4())
            if ids is not None:
                ids.append(transfer_id)
            else:
                ids = [transfer_id]
            metadata.update({"External-Identifier": ids})
            logger.info(f"Adding UUID to metadata: {', '.join(ids)}")
        return metadata

    def _valid_headers(self, metadata):
        if metadata is None:
            return False
        headers = list(metadata.keys())
        valid_headers = True
        for header in self.required_headers:
            if header not in headers:
                valid_headers = False
                logger.warning(f"Missing mandatory header: {header}")
        return valid_headers
