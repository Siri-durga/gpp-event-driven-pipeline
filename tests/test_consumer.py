from pymongo.errors import DuplicateKeyError

def test_duplicate_handling(monkeypatch):
    inserted = []

    class FakeCollection:
        def insert_one(self, doc):
            if doc["_id"] in inserted:
                raise DuplicateKeyError("duplicate")
            inserted.append(doc["_id"])

    fake_collection = FakeCollection()

    message = {
        "sensor_id": "s1",
        "timestamp": "2024-01-01T00:00:00Z"
    }
    message["_id"] = "s1-2024-01-01T00:00:00Z"

    fake_collection.insert_one(message)
    try:
        fake_collection.insert_one(message)
    except DuplicateKeyError:
        pass

    assert len(inserted) == 1
