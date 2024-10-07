import json
import os
import threading
import time
from typing import Dict, Any, Optional
import portalocker  # We'll use this for cross-platform file locking

MAX_KEY_LENGTH = 32
MAX_VALUE_SIZE = 16 * 1024  # 16KB
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB
BATCH_LIMIT = 100  # Maximum number of items in a batch operation

class KeyValueStoreError(Exception):
    """Base exception for KeyValueStore errors"""
    pass

class KeyNotFoundError(KeyValueStoreError):
    """Raised when a key is not found"""
    pass

class KeyExistsError(KeyValueStoreError):
    """Raised when trying to create a key that already exists"""
    pass

class ValueTooLargeError(KeyValueStoreError):
    """Raised when the value size exceeds the limit"""
    pass

class KeyTooLongError(KeyValueStoreError):
    """Raised when the key length exceeds the limit"""
    pass

class FileSizeLimitExceededError(KeyValueStoreError):
    """Raised when the file size would exceed the limit"""
    pass

class BatchSizeLimitExceededError(KeyValueStoreError):
    """Raised when the batch size exceeds the limit"""
    pass

class KeyValueStore:
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path or os.path.join(os.getcwd(), "kv_store.db")
        self.data: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.file_lock = threading.Lock()
        
        self.load_data()
        
        # Start background thread for TTL cleanup
        threading.Thread(target=self._ttl_cleanup, daemon=True).start()

    def create(self, key: str, value: Dict[str, Any], ttl: Optional[int] = None) -> None:
        if len(key) > MAX_KEY_LENGTH:
            raise KeyTooLongError(f"Key must be {MAX_KEY_LENGTH} characters or less")
        
        serialized_value = json.dumps(value)
        if len(serialized_value) > MAX_VALUE_SIZE:
            raise ValueTooLargeError(f"Value must be {MAX_VALUE_SIZE} bytes or less when serialized")
        
        with self.lock:
            if key in self.data:
                # Instead of raising an error, we'll update the existing key
                print(f"Warning: Key '{key}' already exists. Updating its value.")
                self.data[key]["value"] = value
                if ttl:
                    self.data[key]["expiry"] = time.time() + ttl
                else:
                    self.data[key]["expiry"] = None
            else:
                new_entry = {
                    "value": value,
                    "expiry": time.time() + ttl if ttl else None
                }
                
                # Check if adding this entry would exceed the file size limit
                if self._would_exceed_file_size_limit(key, new_entry):
                    raise FileSizeLimitExceededError("Adding this entry would exceed the file size limit")
                
                self.data[key] = new_entry
            
            self.save_data()

    def read(self, key: str) -> Dict[str, Any]:
        with self.lock:
            if key not in self.data:
                raise KeyNotFoundError("Key not found")
            
            entry = self.data[key]
            if entry["expiry"] and time.time() > entry["expiry"]:
                del self.data[key]
                self.save_data()
                raise KeyNotFoundError("Key has expired")
            
            return entry["value"]

    def delete(self, key: str) -> None:
        with self.lock:
            if key not in self.data:
                raise KeyNotFoundError("Key not found")
            
            del self.data[key]
            self.save_data()

    def batch_create(self, items: Dict[str, Dict[str, Any]]) -> None:
        if len(items) > BATCH_LIMIT:
            raise BatchSizeLimitExceededError(f"Batch size cannot exceed {BATCH_LIMIT} items")
        
        with self.lock:
            for key, value in items.items():
                self.create(key, value)

    def load_data(self) -> None:
        with self.file_lock:
            if os.path.exists(self.file_path):
                with portalocker.Lock(self.file_path, 'r', timeout=10) as f:
                    self.data = json.load(f)

    def save_data(self) -> None:
        with self.file_lock:
            with portalocker.Lock(self.file_path, 'w', timeout=10) as f:
                json.dump(self.data, f)

    def _ttl_cleanup(self) -> None:
        while True:
            with self.lock:
                current_time = time.time()
                expired_keys = [
                    key for key, entry in self.data.items()
                    if entry["expiry"] and current_time > entry["expiry"]
                ]
                for key in expired_keys:
                    del self.data[key]
                
                if expired_keys:
                    self.save_data()
            
            time.sleep(1)  # Check every second

    def _would_exceed_file_size_limit(self, new_key: str, new_entry: Dict[str, Any]) -> bool:
        # Estimate the size of the new data
        current_size = os.path.getsize(self.file_path) if os.path.exists(self.file_path) else 0
        new_data_size = len(json.dumps({new_key: new_entry}))
        return current_size + new_data_size > MAX_FILE_SIZE

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save_data()