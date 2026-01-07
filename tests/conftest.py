import pytest
import os


# set environenment variables for testing - directory is test data directories, THREDDS is fake
@pytest.fixture(scope="session", autouse=True)
def set_test_env_vars():
    os.environ["DATA_ROOT"] = os.path.abspath("tests/data/")
    os.environ["OUTPUT_DIR"] = os.path.abspath("tests/output/")
    os.environ["THREDDS_HTTP_BASE"] = "http://thredds.test/fileserver"
    os.environ["THREDDS_DAP_BASE"] = "http://thredds.test/dap/"
