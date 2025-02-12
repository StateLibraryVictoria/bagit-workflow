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
    config = {
        "LOGGING_DIR": str(logging_dir),
        "DATABASE": str(database),
        "ARCHIVE_DIR": str(archive),
        "TRANSFER_DIR": str(transfer),
        "APPRAISAL_DIR": str(appraisal),
    }
    return config


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


def test_should_exit_if_no_dir():
    with pytest.raises(SystemExit):
        main()


def test_successful_run_bag(stable_path, existing_bag, mock_config, monkeypatch):
    # set up the paths and stage bag
    transfer_dir = mock_config.get("TRANSFER_DIR")
    archive_dir = mock_config.get("ARCHIVE_DIR")
    appraisal_dir = mock_config.get("APPRAISAL_DIR")
    shutil.move(str(existing_bag), transfer_dir)
    ok_file = os.path.join(transfer_dir, "test_bag.ok")
    with open(ok_file, "w") as f:
        f.write("")

    # replace config with mock
    monkeypatch.setattr("bagit_transfer.load_config", lambda: mock_config)
    with pytest.raises(SystemExit):
        main()

    # files we expect
    outcome = os.path.join(archive_dir, "RA-9999-99", "t1", "bag-info.txt")
    db = os.path.join(stable_path, "database", "database.db")
    # transfer should be empty
    in_transfer = os.listdir(transfer_dir)
    # appraisal should have the original folder
    in_appraisal = os.path.join(appraisal_dir, "test_bag")
    assert (
        os.path.exists(outcome)
        and os.path.exists(db)
        and len(in_transfer) == 0
        and os.path.exists(in_appraisal)
    )
