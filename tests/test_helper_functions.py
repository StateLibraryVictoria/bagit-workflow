from src.helper_functions import *
import pytest

## Test TriggerFile

## Expected headers are configured using .env. Tests are based on currently expected ones:
# ["record-set", "Source-Organization","Contact-Name","External-Description","Internal-Sender-Identifier"]

@pytest.fixture
def valid_tigger_file(tmp_path):
    file = tmp_path / "valid_trigger.ok"
    data = {"record-set" : "Personal",
            "Source-Organization" : "Home",
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
def invalid_tigger_file(tmp_path):
    file = tmp_path / "valid_trigger.ok"
    dir = tmp_path / "valid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    file2.write_text("some text")
    data = {"record-set" : "Personal"}
    output = json.dumps(data)
    file.write_text(output)
    yield file
        
def test_load_metadata(valid_tigger_file):
    file = valid_tigger_file
    tf = TriggerFile(file)
    assert tf.get_metadata().get("record-set") == "Personal"

def test_header_validation_fails(invalid_tigger_file):
    tf = TriggerFile(invalid_tigger_file)
    tf.load_metadata()
    assert tf.validate() == False

def test_header_validation_succeeds(valid_tigger_file):
    tf = TriggerFile(valid_tigger_file)
    tf.load_metadata()
    assert tf.validate() == True

def test_get_metadata(valid_tigger_file):
    data = {"record-set" : "Personal",
            "Source-Organization" : "Home",
            "Contact-Name" : "Susannah Bourke",
            "External-Description" : "A test metadata package for a valid transfer.",
            "Internal-Sender-Identifier" : "/path/to/folder",}
    tf = TriggerFile(valid_tigger_file)
    assert data == tf.get_metadata()