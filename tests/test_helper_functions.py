from src.helper_functions import *
from src.config import Config
import pytest
import unicodedata

## Test TriggerFile

## Expected headers are configured using .env. Tests are based on currently expected ones:
# ["Source-Organization","Contact-Name","External-Description","Internal-Sender-Identifier"]

SET_UUID_1 = "ce2c5343-0f5c-45e1-9cd1-5e10e748efef"
SET_UUID_2 = "6c7e785f-5aa9-486b-9772-35ef009fbc38"


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
            UUID_ID: SET_UUID_2,
        },
    )
    yield file


@pytest.fixture
def id_parser():
    config = Config(os.path.join("src","conf","test_config.json"))
    id_parser = config.get_id_parser()
    return id_parser


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
            "Internal-Sender-Identifier": SET_UUID_1,
        },
    )
    yield bag


@pytest.fixture
def unicode_bag(tmp_path):
    dir = tmp_path / "test_bag"
    dir.mkdir()
    nfc_filename = unicodedata.normalize("NFC", "âââââ.txt")
    nfd_filename = unicodedata.normalize("NFD", "âââââ.txt")
    file_a = dir / nfc_filename
    file_b = dir / nfd_filename
    with open(file_a, "w") as f:
        f.write("Text in file a.")
    with open(file_b, "w") as f:
        f.write("Text in file b.")
    bag = bagit.make_bag(
        dir,
        {
            "Source-Organization": "Home",
            "Contact-Name": "Name",
            "External-Description": "A test metadata package for a valid transfer.",
            "External-Identifier": "RA-9999-99",
            "Internal-Sender-Identifier": SET_UUID_2,
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
    expected = f"Collection identifier could not be parsed from folder title.\nError parsing metadata.\nSee logfile for more information.\n"
    with open(file, "r") as f:
        data = f.read()
    data = data.replace('\r\n','\n')
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
        ("COMY99999", True),
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
        ("COMY9999", False)
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
        ("A folder COMY99999", ["COMY99999"]),
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
        ("COMY99999","COMY99999")
    ],
)
def test_normalise_id(id_parser, input, expected):
    parser = id_parser
    output = parser.normalise_id(input)
    assert output == expected


def test_process_transfer_succesfully_copies_bag(existing_bag, tmp_path):
    process_transfer(existing_bag, tmp_path)
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


# test bag validation


def test_validate_bag_at_fake_path():
    uuid, errors = validate_bag_at("path")
    assert errors[0].startswith("Expected bagit.txt does not exist")


def test_validate_bag_at_uuid(existing_bag):
    uuid, errors = validate_bag_at(existing_bag.path)
    assert uuid == [SET_UUID_1]


def test_validate_bag_at_no_uuid(existing_bag):
    existing_bag.info[UUID_ID] = [None]
    existing_bag.save()
    result = validate_bag_at(existing_bag.path)
    assert result == ([], ["Bag UUID not present in bag-info.txt"])


def test_validate_bag_at_two_uuids(existing_bag):
    existing_bag.info[UUID_ID] = [SET_UUID_1, SET_UUID_2]
    existing_bag.save()
    result = validate_bag_at(existing_bag.path)
    assert result == (
        [SET_UUID_1, SET_UUID_2],
        [f"Too many UUIDs parsed from bag: {';'.join([SET_UUID_1,SET_UUID_2])}"],
    )


def test_validate_bag_at_valid(existing_bag):
    result = validate_bag_at(existing_bag.path)
    assert result == ([SET_UUID_1], [])


def test_validate_bag_at_no_manifest(existing_bag):
    os.remove(os.path.join(existing_bag.path, "manifest-sha256.txt"))
    result = validate_bag_at(existing_bag.path)
    assert result == (
        [SET_UUID_1],
        [
            "Bag is incomplete: manifest-sha256.txt exists in manifest but was not found on filesystem"
        ],
    )


def test_validate_bag_at_no_baginfo(existing_bag):
    os.remove(os.path.join(existing_bag.path, "bag-info.txt"))
    result = validate_bag_at(existing_bag.path)
    assert result == (
        [],
        [
            "Bag UUID not present in bag-info.txt",
            "Bag is incomplete: bag-info.txt exists in manifest but was not found on filesystem",
        ],
    )


def test_transfer_unicode_normalisation_bag(unicode_bag, tmp_path):
    """Test that bags with files different by normalisation only
    are still valid"""
    archive_dir = tmp_path / "output"
    archive_dir.mkdir()
    process_transfer(str(unicode_bag), archive_dir)
    bag_path = os.path.normpath(archive_dir)
    bag = bagit.Bag(bag_path)
    valid = bag.is_valid()
    dir_list = os.listdir(os.path.join(bag_path, "data"))
    assert (valid == True) and (len(dir_list) == 2)
