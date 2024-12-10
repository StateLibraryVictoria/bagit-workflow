from src.helper_functions import *
import pytest

## Test TriggerFile

## Expected headers are configured using .env. Tests are based on currently expected ones:
# ["Source-Organization","Contact-Name","External-Description","Internal-Sender-Identifier"]


@pytest.fixture
def valid_metadata_no_uuid():
    data = {
        "Source-Organization": "Home",
        "Contact-Name": "Susannah Bourke",
        "External-Description": "A test metadata package for a valid transfer.",
        "Internal-Sender-Identifier": "/path/to/folder",
    }
    yield data


@pytest.fixture
def valid_trigger_file(tmp_path):
    file = tmp_path / "valid_trigger.ok"
    data = {
        "Source-Organization": "Home",
        "Contact-Name": "Susannah Bourke",
        "External-Description": "A test metadata package for a valid transfer.",
        "Internal-Sender-Identifier": "/path/to/folder",
    }
    output = json.dumps(data)
    dir = tmp_path / "valid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    file.write_text(output)
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
            "Source-Organization": "Home",
            "Contact-Name": "Susannah Bourke",
            "External-Description": "A test metadata package for a valid transfer.",
            "Internal-Sender-Identifier": "/path/to/folder",
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

def test_load_metadata(valid_trigger_file):
    tf = TriggerFile(valid_trigger_file)
    assert tf.get_metadata().get("Source-Organization") == "Home"


def test_header_validation_fails(invalid_trigger_file):
    tf = TriggerFile(invalid_trigger_file)
    tf.load_metadata()
    assert tf.validate() == False


def test_header_validation_succeeds(valid_trigger_file):
    tf = TriggerFile(valid_trigger_file)
    tf.load_metadata()
    assert tf.validate() == True


def test_get_metadata_from_ok(valid_trigger_file, valid_metadata_no_uuid):
    data = valid_metadata_no_uuid
    tf = TriggerFile(valid_trigger_file)
    assert data == tf.get_metadata()


def test_get_metadata_from_bag(existing_bag_info_file, valid_metadata_no_uuid):
    data = valid_metadata_no_uuid
    tf = TriggerFile(existing_bag_info_file)
    # clean up the extra keys we don't need
    result = tf.get_metadata()
    for key in ["Payload-Oxum", "Bagging-Date", "Bag-Software-Agent"]:
        result.pop(key)
    assert data == result


def test_error_file_creation(invalid_trigger_file, tmp_path):
    tf = TriggerFile(invalid_trigger_file)
    tf.validate()
    file = tmp_path / "invalid_trigger.error"
    assert os.path.isfile(file)


def test_error_file_data(invalid_trigger_file, tmp_path):
    tf = TriggerFile(invalid_trigger_file)
    tf.validate()
    file = tmp_path / "invalid_trigger.error"
    expected = f'Headers do not match config.{os.linesep}Error parsing metadata. Are your values filled in?{os.linesep}{{"Source-Organization": "Home", "Contact-Name": "Input this value.", "External-Description": "Input this value.", "Internal-Sender-Identifier": "Input this value."}}'
    with open(file, "r") as f:
        data = f.read()
    assert expected == data


def test_not_ok_file_raises_exception(tmp_path):
    file = tmp_path / "error_raise.txt"
    with pytest.raises(ValueError):
        tf = TriggerFile(file)

"""MetadataChecker tests"""

def test_add_uuid_if_not_present(valid_metadata_no_uuid):
    data = valid_metadata_no_uuid
    mc = MetadataChecker()
    metadata_with_uuid = mc.validate(data)
    result = metadata_with_uuid.get("External-Identifier")
    assert result is not None


def test_add_uuid_has_other_ids(valid_metadata_no_uuid):
    data = valid_metadata_no_uuid
    data.update({"External-Identifier": "Some other id"})
    mc = MetadataChecker()
    metadata_with_uuid = mc.validate(data)
    result = metadata_with_uuid.get("External-Identifier")
    assert len(result) == 2


def test_dont_add_uuid_if_exists(valid_metadata_no_uuid):
    data = valid_metadata_no_uuid
    data.update({"External-Identifier": str(uuid.uuid4())})
    mc = MetadataChecker()
    metadata_with_uuid = mc.validate(data)
    result = metadata_with_uuid.get("External-Identifier")
    result_uuid = uuid.UUID(result)
    assert type(result_uuid) == uuid.UUID

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
