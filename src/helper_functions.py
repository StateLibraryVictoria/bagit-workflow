import os
import re
import json
import uuid
import bagit
import subprocess
import logging
from pathlib import Path
from abc import ABC, abstractmethod

headers = json.loads(os.getenv("REQUIRED_HEADERS"))
logger = logging.getLogger(__name__)

# metadata tags
PRIMARY_ID = "External-Identifier"
UUID_ID = "Internal-Sender-Identifier"
CONTACT = "Contact-Name"
EXTERNAL_DESCRIPTION = "External-Description"

class TriggerFile:
    def __init__(self, filename, id_parser):
        self.filename = filename
        self.id_parser = id_parser
        self.name, self.status = os.path.splitext(filename)
        if not self.status == ".ok":
            raise ValueError("Only processes .ok files!")
        self.transfer_type = TransferType(NewTransfer())
        self.metadata = self.load_metadata()

    def load_metadata(self):
        bag_info = os.path.join(self.name, "bag-info.txt")
        if os.path.isfile(bag_info):
            logging.info(
                "Existing bag identified. Will use bag metadata. Delete bag-info.txt to run a bag with supplied metadata."
            )
            self.transfer_type.transfer = BagTransfer()
        metadata = self.transfer_type.build_metadata(path=self.name, id_parser=self.id_parser)
        return metadata

    def get_metadata(self):
        return self.metadata

    def get_directory(self):
        return self.name

    def set_error(self, error):
        self._set_status(".error")
        try:
            with open(self.filename, "w") as f:
                f.write(error + "\n")
                f.write(self._build_default_metadata())
        except PermissionError as e:
            logger.error(f"Error writing to file: {e}")

    def validate(self):
        logger.info(f"Verifying transfer: {self.name}")
        errors = []
        if not os.path.exists(self.name):
            errors.append("Folder does not exist.")
        elif not len(os.listdir(self.name)) > 0:
            errors.append("Folder is empty.")
        if not self._check_metadata():
            errors.append("Error parsing metadata.")
        if len(errors) == 0:
            logger.info(f"Transfer verified: {self.name}")
            return True
        else:
            logger.error(
                f"Issues identified while processing {self.filename}: {' '.join(errors)}"
            )
            self._set_status(".error")
            try:
                with open(self.filename, "a") as f:
                    for error in errors:
                        f.write(error + "\n")
                    # get logfilename
                    f.write("See logfile for more information.")
            except PermissionError as e:
                logger.error(f"Error writing to file: {e}")
            return False

    def _set_status(self, new_status):
        new_name = f"{self.name}{new_status}"
        os.rename(self.filename, new_name)
        logger.info(f"Renaming file to {new_status} file: {new_name}")
        self.status = new_status
        self.filename = new_name

    def _check_metadata(self):
        if self.metadata == None:
            logger.error("Metadata could not be parsed from file.")
            return False
        else:
            return self._check_ids()
    
    def _check_ids(self):
        collection_id = self.metadata.get(PRIMARY_ID)
        logger.warning(self.metadata)
        transfer_id = self.metadata.get(UUID_ID)
        has_uuid = self._has_uuid(transfer_id)
        has_collection_id = self._has_collection_id(collection_id)
        if has_collection_id and not has_uuid:
            self._add_uuid()
            logger.warning("Adding uuid to item.")
        return has_collection_id

    def _has_uuid(self, ids: list):
        parsed_uuid = []
        if ids is not None and ids !=0:
            for id in ids:
                try:
                    test_uuid = uuid.UUID(id)
                    parsed_uuid.append(test_uuid)
                    logger.debug(f"ID {id} is a uuid")
                except Exception as e:
                    logger.debug(f"Failed to parse uuid from {id}")
        return len(parsed_uuid) != 0
    
    def _add_uuid(self):
        transfer_id = str(uuid.uuid4())
        self.metadata.update({UUID_ID: transfer_id})

    def _has_collection_id(self, ids: list):
        if ids is None or len(ids) == 0:
            # try to parse from folder title
            ids = self.id_parser.get_ids(self.name, normalise=True)
            self.metadata.update({PRIMARY_ID:ids})
        if ids is not None and len(ids) != 0:
            for id in ids:
                if self.id_parser.validate_id(id) == True:
                    return True
        if ids is None or len(ids) == 0:
            logger.error("No ids present.")
        else:
            logger.warning(f"No collection id could be parsed from identifiers: {', '.join(ids)}")
        return False
    
    def make_bag(self) -> bagit.Bag:
        self.set_in_process()
        bag = self.transfer_type.make_bag(self.name, self.metadata)
        return bag

    def set_in_process(self) -> None:
        self._set_status(".processing")


class IdParser:
    """Parser for pulling identifiers from strings based on config.

    Keyword arguments:
        valid_pattern -- regex pattern for validating identifiers
        pattern_list -- list of regex with identifier patterns
    """

    def __init__(self, valid_pattern, pattern_list):
        self
        self.valid_pattern = valid_pattern
        self.pattern_list = pattern_list

    def validate_id(self, id):
        """Returns true if submitted data matches valid ID pattern"""
        if id is None:
            return None
        return re.fullmatch(self.valid_pattern, id) is not None

    def normalise_id(self, id):
        """Normalise ids to facilitate easy searching"""
        # check if id is a MS number.
        ms_test = re.match(r"(MS)(\d+)", id)
        sc_test = re.match(r"(SC)\D?(\d+)", id)
        if ms_test is not None:
            norm_id = "-".join(ms_test.groups())
        elif sc_test is not None:
            norm_id = "".join(sc_test.groups())
        else:
            norm_id = re.sub("[_\.]|\s", "-", id)
        if norm_id != id:
            logger.info(f"Normalised id {id} to {norm_id}")
        return norm_id

    def get_ids(self, string, normalise=False):
        """Finds identifiers in strings based on supplied identifier pattern regex.
        Returns ids as a list which is compatible with Python BagIt. Optional to normalise with
        """

        matches = re.findall(self.pattern_list, string)
        if len(matches) < 1:
            ids = None
        else:
            ids = []
            for match in matches:
                id = "".join((match))
                if normalise:
                    id = self.normalise_id(id)
                if self.validate_id(id):
                    ids.append(id)
        return ids

class Transfer(ABC):

    @abstractmethod
    def build_metadata(self, path: str, id_parser: IdParser):
        pass

    @abstractmethod
    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        pass


class BagTransfer(Transfer):
    def build_metadata(self, path: str, id_parser: IdParser):
        bag = bagit.Bag(path)
        metadata = bag.info
        return metadata
    
    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        bag = bagit.Bag(path)
        for key in metadata.keys():
            bag.info[key] = metadata.get(key)
        bag.save()
        return bag


class NewTransfer(Transfer):
    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        bag = bagit.make_bag(path, bag_info=metadata)
        return bag

    def build_metadata(self, path: str, id_parser: IdParser):
        root, filename = os.path.split(path)
        owner = self._get_dir_owner(path)
        identifier = id_parser.get_ids(path)
        metadata = {
                PRIMARY_ID: identifier,
                CONTACT: owner,
                EXTERNAL_DESCRIPTION: filename,
            }
        return metadata

    def _get_dir_owner(self, path):
        root, folder = os.path.split(path)
        try:
            filepath = Path(root)
            return filepath.owner()
        except KeyError as e:
            logger.warning(f"Unable to parse using Path.owner, trying subprocess...")
            try:
                owner_data = subprocess.run(
                    ["dir", root, "/q"],
                    shell=True,
                    capture_output=True,
                    universal_newlines=True,
                )
            except Exception as e:
                logger.error(
                    f"Command failed: dir /q failed for directory: {folder} with exception {e}"
                )
                return None
            regex = f"STAFF.*{folder}"
            row = re.findall(regex, owner_data.stdout)
            try:
                owner = re.findall(f"STAFF\\\\([A-Za-z]+)\s", row[0])
                owner = owner[0]
            except IndexError:
                logger.error(
                    f"Error identifying owner from directory data for directory: {folder}"
                )
                owner = None
        return owner


class TransferType:
    def __init__(self, transfer: Transfer) -> None:
        self._transfer = transfer

    @property
    def transfer(self) -> Transfer:
        return self._transfer

    @transfer.setter
    def transfer(self, transfer: Transfer) -> None:
        self._transfer = transfer

    def build_metadata(self, path: str, id_parser: IdParser):
        metadata = self._transfer.build_metadata(path, id_parser)
        return metadata
    
    def make_bag(self, path: str, metadata: dict):
        bag = self._transfer.make_bag(path, metadata)
        return bag