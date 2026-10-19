import base64

from dagster_sling import SlingResource

from orpheus_engine.defs.sling.assets import _isolate_missing_program_connection
from orpheus_engine.defs.sling.programs.theseus import _theseus_db_connection


URL = "postgres://mail_user:p%40ss@db.example.com:34822/postgres"
KEY = "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----\n"


def _clear(monkeypatch):
    for var in (
        "THESEUS_COOLIFY_URL",
        "THESEUS_SSH_TUNNEL",
        "THESEUS_SSH_PRIVATE_KEY_B64",
        "AUTH_SSH_PRIVATE_KEY_B64",
    ):
        monkeypatch.delenv(var, raising=False)


def test_tunnels_to_db_host_loopback_with_auth_key(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("THESEUS_COOLIFY_URL", URL)
    monkeypatch.setenv("AUTH_SSH_PRIVATE_KEY_B64", base64.b64encode(KEY.encode()).decode())

    conn = _theseus_db_connection()
    assert conn.host == "127.0.0.1"
    assert conn.port == "34822"
    assert conn.database == "postgres"
    assert conn.user == "mail_user"
    assert conn.password == "p@ss"
    assert conn.sslmode == "disable"
    assert conn.ssh_tunnel == "ssh://warehouse_sync@db.example.com:22"
    assert conn.ssh_private_key == KEY
    SlingResource(connections=[conn]).process_config_and_initialize()


def test_tunnel_and_key_overrides(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("THESEUS_COOLIFY_URL", URL)
    monkeypatch.setenv("THESEUS_SSH_TUNNEL", "ssh://other@bastion.example.com:2222")
    monkeypatch.setenv("THESEUS_SSH_PRIVATE_KEY_B64", base64.b64encode(b"theseus-key").decode())
    monkeypatch.setenv("AUTH_SSH_PRIVATE_KEY_B64", base64.b64encode(b"auth-key").decode())

    conn = _theseus_db_connection()
    assert conn.ssh_tunnel == "ssh://other@bastion.example.com:2222"
    assert conn.ssh_private_key == "theseus-key"


def test_unset_url_is_isolated(monkeypatch):
    _clear(monkeypatch)
    conn = _isolate_missing_program_connection(_theseus_db_connection())
    assert conn.connection_string == (
        "postgres://missing-env-theseus-coolify-url.invalid:5432/unset"
    )
    SlingResource(connections=[conn]).process_config_and_initialize()
