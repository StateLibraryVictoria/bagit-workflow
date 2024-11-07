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
