from src.helper_functions import *
import pytest

## Test TriggerFile

## Expected headers are configured using .env. Tests are based on currently expected ones:
# ["Source-Organization","Contact-Name","External-Description","Internal-Sender-Identifier"]

@pytest.fixture
def valid_trigger_file(tmp_path):
    file = tmp_path / "valid_trigger.ok"
    data = {"Source-Organization" : "Home",
            "Contact-Name" : "Susannah Bourke",
            "External-Description" : "A test metadata package for a valid transfer.",
            "Internal-Sender-Identifier" : "/path/to/folder",}
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
    data = {"Source-Organization" : "Home"}
    output = json.dumps(data)
    file.write_text(output)
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

def test_get_metadata(valid_trigger_file):
    data = {"Source-Organization" : "Home",
            "Contact-Name" : "Susannah Bourke",
            "External-Description" : "A test metadata package for a valid transfer.",
            "Internal-Sender-Identifier" : "/path/to/folder",}
    tf = TriggerFile(valid_trigger_file)
    assert data == tf.get_metadata()

def test_error_file_creation(invalid_trigger_file, tmp_path):
    tf = TriggerFile(invalid_trigger_file)
    tf.validate()
    file = tmp_path / "invalid_trigger.error"
    assert os.path.isfile(file)

def test_not_ok_file_raises_exception(tmp_path):
    file = tmp_path / "error_raise.txt"
    with pytest.raises(ValueError):
        tf = TriggerFile(file)
