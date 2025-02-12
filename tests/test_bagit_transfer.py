from bagit_transfer import *
import pytest
import shutil


@pytest.fixture
def stable_path(tmp_path):
    return tmp_path


@pytest.fixture
def mock_config(stable_path, monkeypatch):
    logging_dir = stable_path / "logging"
    transfer = stable_path / "transfer"
    archive = stable_path / "archive"
    appraisal = stable_path / "appraisal"
    database = stable_path / "database"
    dirs = [logging_dir, transfer, archive, appraisal, database]
    for dir in dirs:
        os.mkdir(dir)
    database = os.path.join(database, "database.db")
    monkeypatch.setenv("LOGGING_DIR", logging_dir)
    monkeypatch.setenv("DATABASE", database)
    monkeypatch.setenv("ARCHIVE_DIR", archive)
    monkeypatch.setenv("TRANSFER_DIR", transfer)
    monkeypatch.setenv("APPRAISAL_DIR", appraisal)


SET_UUID_1 = "ce2c5343-0f5c-45e1-9cd1-5e10e748efef"
SET_UUID_2 = "6c7e785f-5aa9-486b-9772-35ef009fbc38"


@pytest.fixture
def existing_bag(stable_path):
    dir = stable_path / "test_bag"
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


def test_should_exit_if_no_dir(mock_config, monkeypatch):
    monkeypatch.setitem(config, "ARCHIVE_DIR", None)
    with pytest.raises(SystemExit):
        main()


def test_successful_run_bag(stable_path, existing_bag, mock_config):
    transfer_dir = stable_path / "transfer"
    archive_dir = stable_path / "archive"
    shutil.move(str(existing_bag), transfer_dir)
    ok_file = transfer_dir / "test_bag.ok"
    ok_file.write_text("")
    data = os.listdir(transfer_dir)
    print(data)
    with pytest.raises(SystemExit):
        main()
    data = os.listdir(archive_dir)
    outcome = os.path.join(archive_dir, "RA-9999-99", "t1", "bag-info.txt")
    print(data)
    assert os.path.exists(outcome)
