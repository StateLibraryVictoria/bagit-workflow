from droid_report_check import *
import shutil
import pytest

@pytest.fixture
def stable_path(tmp_path):
    return tmp_path

@pytest.fixture
def mock_config(stable_path):
    logging_dir = stable_path / "logging"
    transfer = stable_path / "transfer"
    report = stable_path / "report"
    droid_output = stable_path / "droid_output"
    dirs = [logging_dir, transfer, report, droid_output]
    for dir in dirs:
        os.mkdir(dir)
    config = {
        "LOGGING_DIR": str(logging_dir),
        "REPORT_DIR": str(report),
        "TRANSFER_DIR": str(transfer),
        "DROID_OUTPUT_DIR": str(droid_output),
    }
    return config

@pytest.fixture
def simple_transfer(mock_config):
    staging = mock_config.get("TRANSFER_DIR")
    shutil.copytree(os.path.join(os.getcwd(),"tests","data"),staging, dirs_exist_ok=True)

# successful validation run
def test_simple_transfer_is_ok(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    expected = os.path.join(transfer_dir, "droid-test.ok")
    assert os.path.exists(expected)

def test_simple_transfer_moves_droid_report(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    unexpected = os.path.join(transfer_dir, "droid-test","droid-test_DROID.csv")
    exists = os.path.exists(unexpected)
    assert not exists

def test_catches_extra_file(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")
    new_file = os.path.join(transfer_dir,"droid-test","extrafile.txt")
    droid_report = os.path.join(transfer_dir,"droid-test","droid-test_DROID.csv")
    with open (new_file, 'w') as f:
        f.write('something')

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    expected = os.path.join(transfer_dir, "droid-test.error")
    expected_error = "Manifest does not contain file: extrafile.txt"
    with open(expected, 'r') as f:
        error = ",".join(f.readlines())
    assert os.path.exists(expected)
    assert error == expected_error
    assert os.path.isfile(droid_report)

def test_catches_changed_file(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")
    new_file = os.path.join(transfer_dir,"droid-test","test_file_01.txt")
    droid_report = os.path.join(transfer_dir,"droid-test","droid-test_DROID.csv")
    with open (new_file, 'w') as f:
        f.write('something')

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    expected = os.path.join(transfer_dir, "droid-test.error")
    expected_error = "File match errors identified"
    with open(expected, 'r') as f:
        error = ",".join(f.readlines())
    assert os.path.exists(expected)
    assert error == expected_error
    assert os.path.isfile(droid_report)

def test_catches_missing_droid_report(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")
    droid_report = os.path.join(transfer_dir,"droid-test","droid-test_DROID.csv")
    os.remove(droid_report)

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    expected = os.path.join(transfer_dir, "droid-test.error")
    expected_error = "No droid report in folder."
    with open(expected, 'r') as f:
        error = ",".join(f.readlines())
    assert os.path.exists(expected)
    assert error == expected_error

def test_catches_missing_file(stable_path, mock_config, simple_transfer, monkeypatch):
    transfer_dir = os.path.join(stable_path, "transfer")
    droid_report = os.path.join(transfer_dir,"droid-test","droid-test_DROID.csv")
    new_file = os.path.join(transfer_dir,"droid-test","test_file_01.txt")
    os.remove(new_file)

    # set config
    monkeypatch.setattr("droid_report_check.load_config", lambda: mock_config)
    # run main
    main()

    expected = os.path.join(transfer_dir, "droid-test.error")
    expected_error = "Error: Unable to find file"
    with open(expected, 'r') as f:
        error = ",".join(f.readlines())
    assert os.path.exists(expected)
    assert error.startswith(expected_error)
    assert os.path.isfile(droid_report)