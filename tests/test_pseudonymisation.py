import os
import pytest

from pseudonymisation_cnps_anstat import (
    generer_id_anstat,
    generer_id_anstat_pbkdf2,
    charger_cle_secrete_depuis_env,
    generer_sel_unique,
)


def test_generer_sel_unique_len():
    s = generer_sel_unique()
    # token_hex(32) -> 64 chars
    assert isinstance(s, str) and len(s) == 64


def test_hmac_deterministic():
    key = "0123456789abcdef"
    a = generer_id_anstat("194011724471", key)
    b = generer_id_anstat("194011724471", key)
    assert a == b


def test_hmac_diff_for_close_cnps():
    key = "0123456789abcdef"
    a = generer_id_anstat("194011724471", key)
    b = generer_id_anstat("194011724472", key)
    assert a != b


def test_pbkdf2_different_output_from_hmac():
    key = "mysecret"
    hmac_id = generer_id_anstat("194011724471", key)
    pbkdf2_id = generer_id_anstat_pbkdf2("194011724471", key, iterations=1000)
    assert hmac_id != pbkdf2_id


def test_loader_missing_raises():
    # Ensure ANSTAT_SECRET_KEY not set
    os.environ.pop("ANSTAT_SECRET_KEY", None)
    with pytest.raises(EnvironmentError):
        charger_cle_secrete_depuis_env("ANSTAT_SECRET_KEY", allow_generate=False)


def test_loader_allow_generate():
    os.environ.pop("ANSTAT_SECRET_KEY", None)
    val = charger_cle_secrete_depuis_env("ANSTAT_SECRET_KEY", allow_generate=True)
    assert isinstance(val, str) and len(val) == 64
