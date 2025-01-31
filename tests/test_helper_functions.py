from src.helper_functions import *
import pytest

## Test TriggerFile

## Expected headers are configured using .env. Tests are based on currently expected ones:
# ["Source-Organization","Contact-Name","External-Description","Internal-Sender-Identifier"]


@pytest.fixture
def valid_metadata_no_uuid():
    data = {
        "Contact-Name": "sbourke",
        "External-Description": "A test metadata package for a valid transfer.",
        "External-Identifier": "RA-9999-12",
    }
    yield data


@pytest.fixture
def valid_trigger_file(tmp_path):
    file = tmp_path / "RA.9999.99_valid_trigger.ok"
    file.write_text("")
    dir = tmp_path / "RA.9999.99_valid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    yield file


@pytest.fixture
def invalid_trigger_file(tmp_path):
    file = tmp_path / "invalid_trigger.ok"
    file.write_text("")
    dir = tmp_path / "invalid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    yield file


@pytest.fixture
def existing_bag_trigger(tmp_path):
    file = tmp_path / "test_bag.ok"
    dir = tmp_path / "test_bag"
    dir.mkdir()
    bagit.make_bag(
        dir,
        {
            "Contact-Name": "sbourke",
            "External-Description": "A test metadata package for a valid transfer.",
            "External-Identifier": "RA-9999-12",
            UUID_ID: "6c7e785f-5aa9-486b-9772-35ef009fbc38",
        },
    )
    yield file


@pytest.fixture
def id_parser():
    # Assumes the identifiers will be final value or followed by " ", "_" or "-".
    final = "(?=[_-]|\\s|$)"
    # Assumes identifier parts will be separated by "-", "." or "_".
    sep = "[\\-\\._]?\\s?"
    identifier_pattern = (
        # SC numbers
        f"(SC{sep}\\d{{4,}}{final})|"
        # RA numbers
        f"(RA{sep}\\d{{4}}{sep}\\d+{final})|"
        # PA numbers
        f"(PA{sep}\\d\\d)(\\d\\d)?({sep}\\d+{final})|"
        # MS numbers
        f"(MS{sep}\\d{{2,}}{final})|"
        # capture case for PO numbers that should fail validation
        f"(PO{sep}\\d+{sep}slvdb){final}|"
        # Legacy purchase orders
        f"(POL{sep})?(\\d{{3,}}{sep}slvdb){final}|"
        # Current purchase orders
        f"(POL{sep}\\d{{3,}}{final})|"
        # H numbers
        f"(H\\d\\d)(\\d\\d)?({sep}\\d+){final}"
    )
    validation_pattern = r"SC\d{4,}|RA-\d{4}-\d+|PA-\d{2}-\d+|MS-?\d{2,}|POL-\d{3,}|(POL-)?\d{3,}-slvdb|H\d{2}(\d\d)?-\d+"
    return IdParser(validation_pattern, identifier_pattern)


@pytest.fixture
def existing_bag(tmp_path):
    dir = tmp_path / "test_bag"
    dir.mkdir()
    file = dir / "file.txt"
    with open(file, "w") as f:
        f.write("Text in file.")
    bag = bagit.make_bag(
        dir,
        {
            "Source-Organization": "Home",
            "Contact-Name": "Name",
            "External-Description": "A test metadata package for a valid transfer.",
            "External-Identifier": "RA-9999-99",
            "Internal-Sender-Identifier": "ce2c5343-0f5c-45e1-9cd1-5e10e748efef",
        },
    )
    yield bag


@pytest.fixture
def invalid_trigger_path(tmp_path):
    file = tmp_path / "invalid_trigger.ok"
    dir = tmp_path / "invalid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    yield tmp_path


"""TriggerFile tests"""


def test_load_metadata_path_correct(valid_trigger_file, id_parser):
    tf = TriggerFile(valid_trigger_file, id_parser)
    assert tf.get_metadata().get("External-Description") == "RA.9999.99_valid_trigger"


def test_validation_succeeds(valid_trigger_file, id_parser):
    tf = TriggerFile(valid_trigger_file, id_parser)
    tf.load_metadata()
    assert tf.validate() == True


def test_get_metadata_from_bag(existing_bag_trigger, valid_metadata_no_uuid, id_parser):
    data = valid_metadata_no_uuid
    data.update({UUID_ID: "6c7e785f-5aa9-486b-9772-35ef009fbc38"})
    tf = TriggerFile(existing_bag_trigger, id_parser)
    # clean up the extra keys we don't need
    result = tf.get_metadata()
    for key in ["Payload-Oxum", "Bagging-Date", "Bag-Software-Agent"]:
        result.pop(key)
    assert data == result


def test_process_valid_bag_is_valid(existing_bag_trigger, id_parser):
    tf = TriggerFile(existing_bag_trigger, id_parser)
    assert tf.validate() == True


def test_bag_with_uuid_is_not_updated_during_validation(
    existing_bag_trigger, id_parser
):
    expected = "6c7e785f-5aa9-486b-9772-35ef009fbc38"
    tf = TriggerFile(existing_bag_trigger, id_parser)
    tf.validate()
    md = tf.get_metadata()
    print(md)
    uuid = md.get(UUID_ID)
    assert uuid == expected


def test_error_file_creation(invalid_trigger_file, id_parser):
    logger.warning(
        f"Invalid trigger file fiture exists? {os.path.exists(invalid_trigger_file)}"
    )
    tf = TriggerFile(invalid_trigger_file, id_parser)
    tf.validate()
    file = f"{tf.get_directory()}.error"
    logger.warning(file)
    assert os.path.isfile(file)


def test_error_file_data(invalid_trigger_file, id_parser):
    tf = TriggerFile(invalid_trigger_file, id_parser)
    tf.validate()
    file = f"{tf.get_directory()}.error"
    expected = f"Error parsing metadata.{os.linesep}See logfile for more information."
    with open(file, "r") as f:
        data = f.read()
    assert expected == data


def test_not_ok_file_raises_exception(tmp_path, id_parser):
    file = tmp_path / "error_raise.txt"
    with pytest.raises(ValueError):
        tf = TriggerFile(file, id_parser)


"""IdParser Tests"""


@pytest.mark.parametrize(
    "input, expected",
    [
        ("SC1234", True),
        ("1234-slvdb", True),
        ("POL-1234", True),
        ("POL-1234-slvdb", True),
        ("MS-12345", True),
        ("MS12345", True),
        ("RA-1234-12", True),
        ("PA-12-1234", True),
        ("H1988-123", True),
        ("H88-123", True),
        ("12345-slvdb", True),
    ],
)
def test_validate_id_true_for_valid(id_parser, input, expected):
    parser = id_parser
    output = parser.validate_id(input)
    assert output == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("SCC1234", False),
        ("LOL-1234-slvdb", False),
        ("1234", False),
        ("POL-12a34-slvdb", False),
        ("MS-12ab345", False),
        ("xyz", False),
        ("PA-12", False),
        ("MS1234A", False),
        ("some words", False),
        ("SC1234-whole-folder-title", False),
        ("H123-12", False),
        ("PA-9999-99", False),
        ("PO-12345-slvdb", False),
        ("PO-1234", False),
    ],
)
def test_validate_id_false_for_invalid(id_parser, input, expected):
    parser = id_parser
    output = parser.validate_id(input)
    assert output == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("SC1234_something_something", ["SC1234"]),
        ("something_SC1234", ["SC1234"]),
        ("abc SC1234 something something", ["SC1234"]),
        ("POL-123456-slvdb_Name_20240620", ["POL-123456-slvdb"]),
        ("YMS12345_My_Thesis_PA_99_999", ["MS-12345", "PA-99-999"]),
        ("A_photographer_named_something_RA_9999_99", ["RA-9999-99"]),
        ("PO-1234-slvdb_is_not_valid", []),
        ("12345-slvdb_test_POL", ["12345-slvdb"]),
        (
            "12345-slvdb_23456-slvdb_34567-slvdb",
            ["12345-slvdb", "23456-slvdb", "34567-slvdb"],
        ),
    ],
)
def test_find_id_in_folder(id_parser, input, expected):
    parser = id_parser
    output = parser.get_ids(input, True)
    assert output == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("MS 12345", "MS-12345"),
        ("MS12345", "MS-12345"),
        ("RA.2012.12", "RA-2012-12"),
        ("PA_9999_99", "PA-9999-99"),
        ("SC 12345", "SC12345"),
        ("SC12345", "SC12345"),
    ],
)
def test_normalise_id(id_parser, input, expected):
    parser = id_parser
    output = parser.normalise_id(input)
    assert output == expected


# test_timed_rsync_copy
def test_timed_rsync_copy_takes_time(existing_bag, tmp_path):
    time = timed_rsync_copy(existing_bag, tmp_path)
    assert 1 > time > 0


def test_timed_rsync_copy_succesfully_copies_bag(existing_bag, tmp_path):
    timed_rsync_copy(existing_bag, tmp_path)
    new_bag = bagit.Bag(os.path.join(tmp_path, str(existing_bag)))
    assert new_bag.validate()


@pytest.fixture
def manifest_filepath(tmp_path):
    dir = tmp_path / "hash"
    dir.mkdir()
    file = dir / "manifest-sha256.txt"
    with open(file, "w") as f:
        f.write(
            "This file should actually contain a manifest of hashes for each file in a bag."
        )
    yield dir


# test_compute_manifest_hash
def test_compute_manifest_hash(manifest_filepath):
    expected = (
        "C2AC092D01314AB876B109F50B74F34B7285D428D2AC504E0EA22F7960AF110D".lower()
    )
    hash = compute_manifest_hash(manifest_filepath)
    assert hash == expected


# test_cleanup_transfer
def test_cleaup_transfer(valid_trigger_file, id_parser):
    tf = TriggerFile(valid_trigger_file, id_parser)
    path = tf.get_directory()
    tf.cleanup_transfer()
    assert not os.path.exists(path)


# test guessing primary id
@pytest.mark.parametrize(
    "input, expected",
    [
        (["SC1234", "abc", "POL-1234", "RA-9999-99"], "RA-9999-99"),
        (["SC1234", "RA-8888-88", "POL-1234", "RA-9999-99"], "RA-8888-88"),
        (["nonsense"], None),
        (["SC1234"], "SC1234"),
        ("RA-999-99", "RA-999-99"),
    ],
)
def test_guess_primary_id(input, expected):
    result = guess_primary_id(input)
    assert result == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ("md5,sha256,sha512", ["md5", "sha256", "sha512"]),
        (None, ["md5", "sha256"]),
        ("sha512", ["sha512"]),
        ("exception", ["md5", "sha256"]),
        ("nonsense,md5", ["md5"]),
    ],
)
def test_get_hash_algorithms(input, expected):
    result = get_hash_algorithms(input)
    assert result == expected
