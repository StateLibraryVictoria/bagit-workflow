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
    dir = tmp_path / "RA.9999.99_valid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    yield file


@pytest.fixture
def invalid_trigger_file(tmp_path):
    file = tmp_path / "invalid_trigger.ok"
    dir = tmp_path / "invalid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    data = {"Source-Organization": "Home"}
    output = json.dumps(data)
    file.write_text(output)
    yield file


@pytest.fixture
def existing_bag_info_file(tmp_path):
    file = tmp_path / "test_bag.ok"
    dir = tmp_path / "test_bag"
    dir.mkdir()
    bagit.make_bag(
        dir,
        {
            "Contact-Name": "sbourke",
            "External-Description": "A test metadata package for a valid transfer.",
            "External-Identifier": "RA-9999-12",
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

"""TriggerFile tests"""

def test_load_metadata_path_correct(valid_trigger_file, id_parser):
    tf = TriggerFile(valid_trigger_file, id_parser)
    assert tf.get_metadata().get("External-Description") == "RA.9999.99_valid_trigger"


def test_header_validation_fails(invalid_trigger_file, id_parser):
    tf = TriggerFile(invalid_trigger_file, id_parser)
    tf.load_metadata()
    assert tf.validate() == False


def test_validation_succeeds(valid_trigger_file, id_parser):
    tf = TriggerFile(valid_trigger_file, id_parser)
    tf.load_metadata()
    assert tf.validate() == True



def test_get_metadata_from_bag(existing_bag_info_file, valid_metadata_no_uuid, id_parser):
    data = valid_metadata_no_uuid
    tf = TriggerFile(existing_bag_info_file, id_parser)
    # clean up the extra keys we don't need
    result = tf.get_metadata()
    for key in ["Payload-Oxum", "Bagging-Date", "Bag-Software-Agent"]:
        result.pop(key)
    assert data == result


def test_error_file_creation(invalid_trigger_file, tmp_path, id_parser):
    tf = TriggerFile(invalid_trigger_file, id_parser)
    tf.validate()
    file = tmp_path / "invalid_trigger.error"
    assert os.path.isfile(file)


def test_error_file_data(invalid_trigger_file, tmp_path, id_parser):
    tf = TriggerFile(invalid_trigger_file, id_parser)
    tf.validate()
    file = tmp_path / "invalid_trigger.error"
    expected = f'Error parsing metadata.{os.linesep}'
    with open(file, "r") as f:
        data = f.read()
    assert expected == data


def test_not_ok_file_raises_exception(tmp_path, id_parser):
    file = tmp_path / "error_raise.txt"
    with pytest.raises(ValueError):
        tf = TriggerFile(file, id_parser)

def test_ok_filepath_not_exist_raises(valid_trigger_file, id_parser):
    filepath, status = os.path.splitext(valid_trigger_file)
    for root, dirs, files in os.walk(filepath):
        for file in files:
            os.remove(os.path.join(root, file))
    os.rmdir(filepath)
    with pytest.raises(OSError):
        tf = TriggerFile(valid_trigger_file, id_parser)

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
