import pytest
from fastapi.testclient import TestClient

import app.api as api_module


class DummyDB:
    def __init__(self, names=None):
        self._names = names or []

    async def list_collection_names(self):
        return self._names

    async def count_documents(self, query):
        return 0


class DummyAdmin:
    async def command(self, cmd):
        return {"ok": 1}


class DummyClient:
    def __init__(self):
        self.admin = DummyAdmin()

    def __getitem__(self, name):
        return DummyDB()


@pytest.fixture(autouse=True)
def patch_client(monkeypatch):
    # Replace motor client with a dummy sync-able object for testing
    monkeypatch.setattr(api_module, "client", DummyClient())
    monkeypatch.setattr(api_module, "db", DummyDB())


def test_health_endpoint():
    client = TestClient(api_module.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_collections_empty():
    client = TestClient(api_module.app)
    r = client.get("/collections")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
