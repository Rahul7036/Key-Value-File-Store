from kv_store import KeyValueStore, KeyNotFoundError, KeyExistsError

# Create a new store
store = KeyValueStore("test_store.db")

# Test create and read
print("Testing create and read:")
store.create("test_key", {"value": "test_value"})
print(store.read("test_key"))  # Should print: {'value': 'test_value'}

# Test delete
print("\nTesting delete:")
store.delete("test_key")
try:
    store.read("test_key")
except KeyNotFoundError:
    print("Key successfully deleted")

# Test key not found
print("\nTesting key not found:")
try:
    store.read("non_existent_key")
except KeyNotFoundError:
    print("KeyNotFoundError raised as expected")

# Test key exists
print("\nTesting key exists:")
store.create("new_existing_key", {"value": "existing_value"})
try:
    store.create("new_existing_key", {"value": "new_value"})
except KeyExistsError:
    print("KeyExistsError raised as expected")

# Test TTL
print("\nTesting TTL:")
import time
store.create("ttl_key", {"value": "ttl_value"}, ttl=2)
print(store.read("ttl_key"))  # Should print: {'value': 'ttl_value'}
print("Waiting for TTL to expire...")
time.sleep(3)
try:
    store.read("ttl_key")
except KeyNotFoundError:
    print("TTL key expired as expected")

# Test batch create
print("\nTesting batch create:")
batch = {
    "batch_key1": {"value": "batch_value1"},
    "batch_key2": {"value": "batch_value2"}
}

start_time = time.time()
store.batch_create(batch)
end_time = time.time()

print(f"Batch create took {end_time - start_time:.2f} seconds")

# Only proceed with reads if batch_create completes
if end_time - start_time < 60:  # Timeout after 1 minute
    print(store.read("batch_key1"))
    print(store.read("batch_key2"))
else:
    print("Batch create operation timed out")

print("\nAll manual tests completed.")