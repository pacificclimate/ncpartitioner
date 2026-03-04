import pytest
import os

# WARNING: in the test suite, paths are sometimes specified absolutely and sometimes relatively.
# This seems to be required to make tests run correctly on github, but it's not entirely
# clear why.
# Changing relative paths to absolute paths in tests may cause tests to fail on github
# (but not locally).
# Specifically, test_check_range() fails in github's CI containers with absolute paths, but they're used in
# other tests.


# set environenment variables for testing - directory is test data directories, THREDDS location
# is a placeholder, checked against expected values but never accessed directly.
@pytest.fixture(scope="session", autouse=True)
def set_test_env_vars():
    os.environ["DATA_ROOT"] = os.path.abspath("tests/data/")
    os.environ["OUTPUT_DIR"] = os.path.abspath("tests/output/")
    os.environ["THREDDS_HTTP_BASE"] = "http://thredds.test/fileserver"
    os.environ["THREDDS_DAP_BASE"] = "http://thredds.test/dap/"
