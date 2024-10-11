# Key-Value File Store

A high-performance, scalable key-value data store implemented in Python.

## Features

- Create, read, and delete key-value pairs
- Support for TTL (Time To Live) on keys
- Batch operations
- Thread-safe and supports concurrent access
- Persistent storage using file-based backend
- Cross-platform file locking

## Requirements

- Python 3.7+
- portalocker library

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/key-value-file-store.git
   ```

2. Install the required dependencies:
   ```bash
   pip install portalocker
   ```

## Running Tests

To run the unit tests:

## Limitations

- Maximum key length: 32 characters
- Maximum value size: 16 KB
- Maximum file size: 1 GB
- Maximum batch size: 100 items

## License

This project is licensed under the MIT License - see the LICENSE file for details.

