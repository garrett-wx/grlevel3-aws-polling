# NEXRAD Level III Super-Res Fetcher

A multi-threaded NEXRAD Level III fetcher from AWS S3, keeping a configurable number of files per site/product, and serving them via a simple HTTP server.

**Specifically laid out for use with [GRLevel3](https://www.grlevelx.com/grlevel3_3/) software**, generating the exact `grlevel3.cfg` format expected by GRLevel3 clients.

## Features

* Polls the `unidata-nexrad-level3` S3 bucket for specified Level III products across multiple radar sites.
* Keeps only the latest `MAX_FILES` per site/product to conserve disk space.
* Auto-generates both `config.cfg` and a **GRLevel3-compatible** `grlevel3.cfg` specifically for GRLevel3 clients.
* Uses a configurable worker thread pool to perform concurrent downloads efficiently.

## Prerequisites

* Python 3.7 or higher
* `boto3`, `botocore`, and `Flask` Python packages
* Internet access to AWS public S3 bucket (no AWS credentials required)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/garrett-wx/grlevel3-aws-polling.git
   cd grlevel3-aws-polling
   ```

   (Optional) Create and activate a virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Open `getL3.py` and adjust the following constants at the top of the file:

* `SITES`: List of 3-letter radar site IDs (e.g., `['DLH','MPX','MQT', ...]`). Note that you do not include the "K" in "KDLH".
* `BUCKET`: S3 bucket name (default: `unidata-nexrad-level3`).
* `MAX_FILES`: Maximum number of files to retain per site/product.
* `POLL_INTERVAL`: Polling interval in seconds (controls how often the script checks S3 for new files).
* `ROOT`: Local directory where files will be stored (default: `level3_service`).
* `WORKER_COUNT`: Number of threads in the download pool. - Look at task manager if you do not know how many threads your CPU has. Using too many could slow your PC.

  * Spawns exactly `WORKER_COUNT` worker threads.
  * Each thread dequeues pending file keys and downloads them independently.
  * Adjust to match your network speed and CPU capacity.

## Threading Model

The script uses Python’s `concurrent.futures.ThreadPoolExecutor` to manage a pool of worker threads:

1. **Queueing**: All new S3 object keys are placed into a thread-safe queue.
2. **Workers**: The executor starts `WORKER_COUNT` threads, each continuously pulling keys from the queue.
3. **Download Tasks**: Each thread downloads its assigned file and writes it to the local directory structure.
4. **Cleanup**: After download, the thread triggers pruning of older files if the site/product exceeds `MAX_FILES`.

This model ensures that downloads happen in parallel, maximizing throughput while keeping CPU and network usage under control.

## Usage

1. **Start the fetcher** (downloads and config files):

   ```bash
   python getL3.py
   ```

   This will run indefinitely, polling AWS every `POLL_INTERVAL` seconds.

2. **Serve files via HTTP**:

   * **Static HTTP server** (recommended):

     ```bash
     cd level3_service
     python3 -m http.server 5000
     ```
     For this example the Polling URL would be hosted at http://localhost:5000
     
3. **Access data and config**:
   * Fetched Level III files from browser: `http://localhost:5000/{SITE}/{PRODUCT}/{YYYYMMDD_HHMMSS}`

   For example:

   ```
   http://localhost:5000/DLH/N0B/20250521_123000
   ```

## Directory Structure

```
nexrad-level3-fetcher/
├── getl3_simple.py       # Main fetcher script
├── requirements.txt      # pip dependency list
└── level3_service/       # Auto-generated directory (The server needs to be pointed at this Directory.)
    ├── config.cfg
    ├── grlevel3.cfg      # GRLevel3-compatible configuration
    ├── DLH/
    │   ├── N0B/
    │   │   ├── 20250521_123000
    │   │   └── dir.list
    │   └── ...
    └── ...
```

## Customization

* To add or remove radar sites, edit the `SITES` list.
* To include additional product codes, update the `PRODUCT_CODES` set.
* You can integrate this into Docker or systemd for production deployments.

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

## Author

Garrett Helms (@WxGareBear)
