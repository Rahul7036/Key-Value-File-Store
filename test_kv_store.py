import pytest
import time
import threading
import os
from kv_store import (
    KeyValueStore, KeyNotFoundError, KeyExistsError, ValueTooLargeError,
    KeyTooLongError, FileSizeLimitExceededError, BatchSizeLimitExceededError
)

@pytest.fixture
def store():
    store = KeyValueStore("test_store.db")
    yield store
    os.remove("test_store.db")

def test_create_and_read(store):
    store.create("test_key", {"value": "test_value"})
    assert store.read("test_key") == {"value": "test_value"}

def test_delete(store):
    store.create("test_key", {"value": "test_value"})
    store.delete("test_key")
    with pytest.raises(KeyNotFoundError):
        store.read("test_key")

def test_ttl(store):
    store.create("test_key", {"value": "test_value"}, ttl=1)
    assert store.read("test_key") == {"value": "test_value"}
    time.sleep(1.1)
    with pytest.raises(KeyNotFoundError):
        store.read("test_key")

def test_key_not_found(store):
    with pytest.raises(KeyNotFoundError):
        store.read("non_existent_key")

def test_key_exists(store):
    store.create("test_key", {"value": "test_value"})
    with pytest.raises(KeyExistsError):
        store.create("test_key", {"value": "another_value"})

def test_value_too_large(store):
    large_value = {"value": "x" * (16 * 1024 + 1)}
    with pytest.raises(ValueTooLargeError):
        store.create("large_key", large_value)

def test_key_too_long(store):
    long_key = "x" * 33
    with pytest.raises(KeyTooLongError):
        store.create(long_key, {"value": "test"})

def test_batch_create(store):
    batch = {
        f"key_{i}": {"value": f"value_{i}"}
        for i in range(10)
    }
    results = store.batch_create(batch)
    assert all(result == "Success" for result in results.values())
    for key, value in batch.items():
        assert store.read(key) == value

def test_batch_create_limit_exceeded(store):
    batch = {
        f"key_{i}": {"value": f"value_{i}"}
        for i in range(1001)
    }
    with pytest.raises(BatchSizeLimitExceededError):
        store.batch_create(batch)

def test_concurrent_access(store):
    def worker(worker_id):
        for i in range(10):
            key = f"worker_{worker_id}_key_{i}"
            value = {"value": f"worker_{worker_id}_value_{i}"}
            store.create(key, value)
            assert store.read(key) == value
            store.delete(key)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Verify that no keys are left after all operations
    assert len(store.data) == 0

def test_delete_expired_key(store):
    store.create("test_key", {"value": "test_value"}, ttl=1)
    time.sleep(1.1)
    with pytest.raises(KeyNotFoundError, match="Key has expired"):
        store.delete("test_key")

def test_batch_create_with_errors(store):
    batch = {
        "key1": {"value": "value1"},
        "key2": {"value": "x" * (16 * 1024 + 1)},  # Too large
        "key3": {"value": "value3"}
    }
    results = store.batch_create(batch)
    assert results["key1"] == "Success"
    assert "ValueTooLargeError" in results["key2"]
    assert results["key3"] == "Success"
    assert store.read("key1") == {"value": "value1"}
    assert store.read("key3") == {"value": "value3"}
    with pytest.raises(KeyNotFoundError):
        store.read("key2")

def test_persistence(store):
    store.create("persist_key", {"value": "persist_value"})
    del store

    new_store = KeyValueStore("test_store.db")
    assert new_store.read("persist_key") == {"value": "persist_value"}

# This test might take a while and consume a lot of disk space
@pytest.mark.skip(reason="This test consumes a lot of disk space")
def test_file_size_limit(store):
    i = 0
    try:
        while True:
            store.create(f"key_{i}", {"value": "x" * 1000})
            i += 1
    except FileSizeLimitExceededError:
        pass
    
    assert os.path.getsize("test_store.db") <= 1 * 1024 * 1024 * 1024  # 1GB

if __name__ == "__main__":
    pytest.main()