import os
import re
import uuid
import bagit
import time
import platform
import shutil
import hashlib
import subprocess
import logging
from pathlib import Path
from abc import ABC, abstractmethod
from src.shared_constants import *

logger = logging.getLogger(__name__)


class TriggerFile:
    """A class for managing BagIt transfer status using files in a directory.

    Each TriggerFile object contains filepath information, a status, metadata,
    and helper objects to support different transfer types.

    Keyword argument:
    filename -- path to the trigger file. Must have extension ".ok" to create.
    id_parser -- a configured IdParser object which is used to parse identifiers from filepaths.
    """

    def __init__(self, filename, id_parser):
        self.filename = filename
        self.id_parser = id_parser
        self.name, self.status = os.path.splitext(filename)
        if not self.status == ".ok":
            raise ValueError("Only processes .ok files!")
        self.transfer_type = TransferType(NewTransfer())
        self.metadata = self.load_metadata()

    def load_metadata(self) -> dict:
        """Creates a dictionary of metadata tags and values.

        If directory contains bag-info.txt, will set the transfer type to BagTransfer,
        otherwise processes as a NewTransfer.
        """
        bag_info = os.path.join(self.name, "bag-info.txt")

        if not os.path.exists(self.name):
            return None

        if os.path.isfile(bag_info):
            logging.info(
                "Existing bag identified. Will use bag metadata. Delete bag-info.txt to run a bag with supplied metadata."
            )
            self.transfer_type.transfer = BagTransfer()
        metadata = self.transfer_type.build_metadata(
            path=self.name, id_parser=self.id_parser
        )
        return metadata

    def get_metadata(self) -> dict:
        """Returns the stored metadata dictionary."""
        return self.metadata

    def get_directory(self) -> str:
        """Returns the path to the directory being triggered for processing."""
        return self.name

    def set_error(self, error: str) -> None:
        """Set an error that will be written to the original trigger file.

        Keyword arguments:
        error -- Text to be written to error file.
        """
        self._set_status(".error")
        try:
            with open(self.filename, "a") as f:
                f.write(error + "\n")
        except PermissionError as e:
            logger.error(f"Error writing to file: {e}")

    def validate(self) -> bool:
        """Returns True if path exists, the folder has data, and file has minimum metadata.

        If any condition fails, the TriggerFile status is updated to .error and errors are written
        to .error file.
        """
        logger.info(f"Verifying transfer: {self.name}")
        errors = []
        if not os.path.exists(self.name):
            errors.append("Folder does not exist.")
        elif not len(os.listdir(self.name)) > 0:
            errors.append("Folder is empty.")
        if not self._check_metadata():
            collection_id = self.metadata.get(PRIMARY_ID)
            if collection_id == None:
                errors.append(
                    "Collection identifier could not be parsed from folder title."
                )
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
                    f.write("See logfile for more information.")
            except PermissionError as e:
                logger.error(f"Error writing to file: {e}")
            return False

    def _set_status(self, new_status: str) -> None:
        """Set TriggerFile status to new value. This renames the file.

        Keyword arguments:
        new_status -- keyword used as file extension to indicate status.
        """
        new_status = new_status.replace(" ", "")
        new_name = f"{self.name}{new_status}"
        try:
            os.rename(self.filename, new_name)
            logger.info(f"Renaming file to {new_status} file: {new_name}")
            self.status = new_status
            self.filename = new_name
        except Exception as e:
            logger.error(f"Failed to rename file: {self.filename} to {new_name}")

    def _check_metadata(self) -> bool:
        """Validates minimum metadata"""
        if self.metadata == None:
            logger.error("Metadata could not be parsed from file.")
            return False
        else:
            return self._check_ids()

    def _check_ids(self) -> bool:
        """Confirms a collection id is present. Adds a UUID if not present."""
        collection_id = self.metadata.get(PRIMARY_ID)
        logger.warning(self.metadata)
        transfer_id = self.metadata.get(UUID_ID)
        has_uuid = self._has_uuid(transfer_id)
        has_collection_id = self._has_collection_id(collection_id)
        if has_collection_id and not has_uuid:
            self._add_uuid()
            logger.warning("Adding uuid to item.")
        return has_collection_id

    def _has_uuid(self, ids: list | str) -> bool:
        """Checks each identifier and returns True if any can be parsed as a UUID."""
        parsed_uuid = []
        if ids is not None and len(ids) != 0:
            if type(ids) == str:
                ids = [ids]
            for id in ids:
                try:
                    test_uuid = uuid.UUID(id)
                    parsed_uuid.append(test_uuid)
                    logger.debug(f"ID {id} is a uuid")
                except Exception as e:
                    logger.debug(f"Failed to parse uuid from {id}")
        return len(parsed_uuid) != 0

    def _add_uuid(self) -> None:
        """Adds a UUID to the UUID field. Will overwrite any other ids in this field."""
        transfer_id = str(uuid.uuid4())
        self.metadata.update({UUID_ID: transfer_id})

    def _has_collection_id(self, ids: str | list) -> bool:
        """Uses IdParser to parse or validate collection identifiers."""
        if ids is None or len(ids) == 0:
            # try to parse from folder title
            ids = self.id_parser.get_ids(self.name, normalise=True)
            self.metadata.update({PRIMARY_ID: ids})
        if type(ids) == str:
            ids = [ids]
        if ids is not None and len(ids) != 0:
            for id in ids:
                if self.id_parser.validate_id(id) == True:
                    return True
        if ids is None or len(ids) == 0:
            logger.error("No ids present.")
        else:
            logger.warning(
                f"No collection id could be parsed from identifiers: {', '.join(ids)}"
            )
        return False

    def make_bag(self) -> bagit.Bag:
        """Builds a BagIt Bag based on transfer type."""
        self._set_status(".processing")
        bag = self.transfer_type.make_bag(self.name, self.metadata)
        return bag

    def cleanup_transfer(self) -> None:
        """Remove trigger file and directory. Won't work on collections with an error status.

        If appraisal directory is specified in .env, will move the remaining data to that location.
        """
        if not self.status == ".error":
            appraisal_dir = get_appraisal_dir()
            if appraisal_dir is not None:
                logger.info("Moving processed bag to appraisal directory.")
                shutil.move(self.name, appraisal_dir)
            else:
                logger.warning("No appraisal directory configured. Removing files.")
                shutil.rmtree(self.name)
            if os.path.isfile(self.filename):
                os.remove(self.filename)


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

    def validate_id(self, id: str) -> bool:
        """Returns True if submitted data matches valid ID pattern"""
        if id is None:
            return False
        return re.fullmatch(self.valid_pattern, id) is not None

    def normalise_id(
        self,
        id: str,
        default_regex: str = "[_\.]|\s",
        replace_with: str = "-",
        tests: list = [r"(MS)(\d+)", r"(SC)\D?(\d+)"],
        join_by: list = ["-", ""],
    ) -> str:
        """Normalise based on patterns for easy searching.
        Defaults to replacing underscores, periods and whitespace with hyphens.
        Includes optional list of regex tests with matchgroups and delimiters to
        address specific issues.

        Keyword arguments:
        id -- identifier to be normalised
        default_regex -- matching patterns will be replaced (default "[_\.]|\s")
        replace_with -- what default_regex is replaced with (default "-")
        tests -- patterns with capturing groups to match and rejoin (default [r"(MS)(\d+)",r"(SC)\D?(\d+)"])
        join_by -- correlating delimiters for joins (default ["-",""])
        """
        # check if id is a MS number.
        norm_id = norm_id = re.sub(default_regex, replace_with, id)
        if tests is not None and join_by is not None and ((len(tests) == len(join_by))):
            for i in range(len(tests)):
                results = re.match(tests[i], id)
                if results is not None:
                    norm_id = join_by[i].join(results.groups())
        if norm_id != id:
            logger.info(f"Normalised id {id} to {norm_id}")
        return norm_id

    def get_ids(self, string: str, normalise: bool = True) -> list | None:
        """Finds identifiers in strings based on supplied identifier pattern regex.
        Returns ids as a list which is compatible with Python BagIt.
        If no ids can be parsed, returns None.

        Keyword arguments:
        string -- String to be searched for identifiers.
        normalise -- Option to set identifiers to normalised values before returning. (default True)
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
    """Transfer interface declares operations common to all transfer types."""

    @abstractmethod
    def build_metadata(self, path: str, id_parser: IdParser):
        pass

    @abstractmethod
    def make_bag(self, path: str, metadata: dict):
        pass


class BagTransfer(Transfer):
    """Concrete Transfer class for handling Bagged data."""

    def build_metadata(self, path: str, id_parser: IdParser) -> dict:
        """Loads metadata from baginfo.txt"""
        bag = bagit.Bag(path)
        metadata = bag.info
        return metadata

    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        """Loads a bag and replaces any metadata keys with values supplied in a dict."""
        bag = bagit.Bag(path)
        for key in metadata.keys():
            bag.info[key] = metadata.get(key)
        bag.save()
        return bag


class NewTransfer(Transfer):
    """Concrete Transfer class for handling unbagged folders of data."""

    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        """Run BagIt on a folder with supplied metadata dictionary."""
        bag = bagit.make_bag(path, bag_info=metadata, checksums=get_hash_config())
        return bag

    def build_metadata(self, path: str, id_parser: IdParser) -> dict:
        """Parses and structures key metadata values based on folder properties.
        Folder name must contain identifier.
        """
        root, filename = os.path.split(path)
        owner = self._get_dir_owner(path)
        identifier = id_parser.get_ids(path)
        metadata = {
            PRIMARY_ID: identifier,
            CONTACT: owner,
            EXTERNAL_DESCRIPTION: filename,
        }
        source_org = get_source_org()
        if source_org is not None:
            metadata.update({SOURCE_ORGANIZATION: source_org})
        return metadata

    def _get_dir_owner(
        self,
        path: str,
        regex_pattern: str = "STAFF.*",
        capturing_pattern: str = "STAFF\\\\([A-Za-z]+)\s",
    ) -> str:
        """Get the folder owner label as a string using either Unix owner or Windows dir command.

        Keyword arguments:
        path -- folder in question
        regex_pattern -- pattern that matches username in Windows dir dump
        capturing_pattern -- riff on regex_pattern, but only getting a single match group within the string
        """

        root, folder = os.path.split(path)
        try:  # to use Unix file properties
            filepath = Path(path)
            return filepath.owner()
        except KeyError as e:
            logger.error(f"KeyError: {e}")
            stat = os.stat(path)
            return stat.st_uid
        except NotImplementedError as e:
            logger.warning(
                f"Unable to parse using Path.owner: {e} \nTrying subprocess..."
            )
            try:  # to use the windows dir command to grab the data then parse it
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
            regex = f"{regex_pattern}{folder}"
            row = re.findall(regex, owner_data.stdout)
            try:
                owner = re.findall(f"{capturing_pattern}", row[0])
                owner = owner[0]
            except IndexError:
                logger.error(
                    f"Error identifying owner from directory data for directory: {folder}"
                )
                owner = None
        return owner


class TransferType:
    """TransferType defines the interfaces that will be used to process transfers."""

    def __init__(self, transfer: Transfer) -> None:
        """Set Transfer object at runtime"""
        self._transfer = transfer

    @property
    def transfer(self) -> Transfer:
        return self._transfer

    @transfer.setter
    def transfer(self, transfer: Transfer) -> None:
        self._transfer = transfer

    def build_metadata(self, path: str, id_parser: IdParser) -> dict:
        metadata = self._transfer.build_metadata(path, id_parser)
        return metadata

    def make_bag(self, path: str, metadata: dict) -> bagit.Bag:
        bag = self._transfer.make_bag(path, metadata)
        return bag


def guess_primary_id(
    identifiers: list, identifier_prefixes: list = ["RA", "PA", "SC", "POL", "H", "MS"]
) -> str:
    """Supply a list of identifiers and an ordered list of prefixes and the algorithm will return the first one that matches.
    Use a better algorithm if you can.
    """
    if identifiers == None:
        return None
    # if there is only one, return it
    if type(identifiers) == str:
        return identifiers
    # sort the identifiers for quicker processing
    identifiers.sort()
    # iterate through until one matches
    for prefix in identifier_prefixes:
        # creates a list containing only identifiers that start with the prefix.
        result = list(filter(lambda x: x.startswith(prefix), identifiers))
        # returns the first result
        if len(result) > 0:
            return result[0]
    return None


def process_transfer(
    source_folder: str,
    output_folder: str,
    rsync_flags: str = "-vrlt",
    robocopy_flags: str = "/e /z /copy:DAT /dcopy:DAT /v",
) -> bool:
    """Copies data from folder to output folder using Robocopy for Windows or rsync for
    Linux. Default flags are provided in each the respective functions.
    """
    WORKING_OS = platform.system()

    if WORKING_OS == "Linux":
        logger.info("Platform is Linux. Attempting to copy using rsync...")
        try:
            rsync_copy(source_folder, output_folder, rsync_flags)
            return True
        except Exception as e:
            logger.info(f"Rsync failed with exception: {e}")
    elif WORKING_OS == "Windows":
        logger.info("Platform is Windows. Attempting to copy using Robocopy...")
        try:
            robocopy_copy(source_folder, output_folder, robocopy_flags)
            return True
        except Exception as e:
            logger.info(f"Robocopy failed with exception: {e}")
    try:
        logger.warning(
            f"Copy methods failed, attempting to copy with shutil.copytree(). This may cause loss of date metadata. Followup required."
        )
        shutil.copytree(source_folder, output_folder)
        return True
    except Exception as e:
        logger.error(f"Copying with shutil.copytree failed.")
        return False


def robocopy_copy(
    folder: str, output_dir: str, flags: str = "/e /z /copy:DAT /dcopy:DAT /v"
) -> None:
    try:
        result = subprocess.run(["robocopy", folder, output_dir, flags], check=True)
        logger.info(result.stdout)
        if result.stderr:
            logger.error("ROBOCOPY ERROR...")
            logger.error(result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f"robocopy failed for folder {folder} to {output_dir}")
        logger.error(f"Error: {e}")
        raise


def rsync_copy(folder: str, output_dir: str, flags: str = "-vrlt") -> None:
    """Copies data from folder to output_dir using rsync subprocess with -vrlt flags.
    Default rsync flags evaluate to verbose, recursive, links, preserve modification times.

    Keyword arguments:
    folder -- path to data for copying
    output_dir -- location to copy to
    flags -- passed to rsync subprocess, (default -vrlt)
    """
    # may need to add bandwidth limit --bwlimit=1000
    try:
        result = subprocess.run(
            ["rsync", flags, "--checksum", f"{folder}/", output_dir], check=True
        )
        logger.info("Retrieving stdout...")
        logger.info(result.stdout)
        if result.stderr:
            logger.error("RSYNC ERROR...")
            logger.error(result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f"rsync failed for folder {folder} to {output_dir}")
        logger.error(f"Error: {e}")
        raise


def compute_manifest_hash(folder: str, target_manifest="manifest-sha256.txt") -> str:
    """Creates a single hash using sha256 of a target manifest for comparing bag content similarity."""
    hash_sha256 = hashlib.sha256()
    file_path = os.path.join(folder, target_manifest)

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):  # Read in 4 KB chunks
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()


# bagit_transfer functions


def load_id_parser(
    identifier_pattern: str = IDENTIFIER_REGEX,
    validation_pattern: str = VALIDATION_REGEX,
) -> IdParser:
    """Returns configured IdParser based on regex in shared_constants.py"""
    return IdParser(validation_pattern, identifier_pattern)


def load_config() -> dict:
    config = {
        "TRANSFER_DIR": os.getenv("TRANSFER_DIR"),
        "ARCHIVE_DIR": os.getenv("ARCHIVE_DIR"),
        "LOGGING_DIR": os.getenv("LOGGING_DIR"),
        "VALIDATION_DB": os.getenv("VALIDATION_DB"),
        "DATABASE": os.getenv("DATABASE"),
        "HASH_ALGORITHMS": os.getenv("HASH_ALGORITHMS"),
        "SOURCE_ORG": os.getenv("SOURCE_ORG"),
        "REPORT_DIR": os.getenv("REPORT_DIR"),
        "APPRAISAL_DIR": os.getenv("APPRAISAL_DIR"),
    }
    return config


config = load_config()


def get_appraisal_dir() -> str:
    return config.get("APPRAISAL_DIR")


def get_source_org() -> str:
    return config.get("SOURCE_ORG")


def get_hash_config() -> str:
    config = load_config()
    return get_hash_algorithms(config.get("HASH_ALGORITHMS"))


def get_hash_algorithms(string_input) -> str:
    if string_input == None:
        hash_algorithms = ["none"]
    elif "," in string_input:
        hash_algorithms = string_input.split(",")
    else:
        hash_algorithms = [string_input]
    valid_hashes = []
    for hash in hash_algorithms:
        if hash in hashlib.algorithms_guaranteed:
            valid_hashes.append(hash)
        else:
            logger.warning(
                f"Submitted hash {hash} isn't valid. Removing from hash config."
            )
    if len(valid_hashes) == 0:
        logger.warning(
            "No valid hash algorithms parsed. Using defaults: md5, sha256..."
        )
        return ["md5", "sha256"]
    return valid_hashes


def runfile_check(directory):
    runfile = os.path.join(directory, RUNNING)

    if os.path.exists(directory) and os.path.isfile(runfile):
        sys.exit()
    else:
        with open(runfile, "w") as f:
            f.write("Running...")
            logger.debug(f"Creating runfile: {runfile}")


def runfile_cleanup(directory):
    runfile = os.path.join(directory, RUNNING)

    os.remove(runfile)
    logger.debug(f"Removing runfile: {runfile}")
    sys.exit()


def parse_uuids(list):
    valid = []
    for id in list:
        try:
            uuid.UUID(id)
            valid.append(id)
        except Exception as e:
            logger.error(f"Error parsing id {e}")
            continue
    return valid


def validate_bag_at(directory) -> tuple[list, list]:
    """Multi-step process that validates the bag and returns a tuple of UUID and errors.

    Keyword arguments:
    directory -- path to bag to be checked"""
    bag_uuid = []
    errors = []

    try:
        bag = bagit.Bag(directory)
    except Exception as e:
        logger.error(f"Error validating bag {directory}: {e}")
        errors.append(f"{e}")
        # update database with failure
        return (bag_uuid, errors)

    # try getting the UUID
    try:
        bag_uuid = bag.info[UUID_ID]
        if type(bag_uuid) == list:
            bag_uuid = parse_uuids(bag_uuid)
        else:
            bag_uuid = parse_uuids([bag_uuid])
        if len(bag_uuid) == 0:
            errors.append("Bag UUID not present in bag-info.txt")
        elif len(bag_uuid) > 1:
            errors.append(f"Too many UUIDs parsed from bag: {';'.join(bag_uuid)}")
    except KeyError as e:
        logger.error(f"Error parsing UUID from bag {directory}: {e}")
        errors.append("Bag UUID not present in bag-info.txt")

    # finally try validating the bag
    try:
        bag.validate()
        logger.info(f"Validated bag at: {directory}")
    except bagit.BagValidationError as e:
        logger.warning(f"Error validating bag at {directory} with UUID {bag_uuid}: {e}")
        errors.append(f"{e}")

    return (bag_uuid, errors)
