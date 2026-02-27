import pytest

from peter.storage.paths import validate_site_code


def test_validate_site_code_uppercases():
    assert validate_site_code("abc123") == "ABC123"


def test_validate_site_code_rejects_bad():
    with pytest.raises(Exception):
        validate_site_code("ab")
    with pytest.raises(Exception):
        validate_site_code("abc-123")
