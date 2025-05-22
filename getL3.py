#!/usr/bin/env python3
"""
getL3.py – multi-threaded NEXRAD Level III fetcher from AWS,
                  keeps MAX_FILES per site/product.
Author: Garrett Helms (@WxGareBear)
"""
# Make Sure to start a localhost server with python on the directory "level3_service"

import os
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import botocore
from botocore.client import Config
from flask import Flask, send_from_directory

# === CONFIG Sites To Poll, Only Nexrad Currently. ===
SITES         = ["TLX", "GSP"] # 3 Letter Site Name (TDWR's not supported yet.) Knock off the leading K, P, or T of the Radar Callsign.

BUCKET        = "unidata-nexrad-level3"
MAX_FILES     = 10
POLL_INTERVAL = 30  # seconds
ROOT          = Path("level3_service")

# Fix worker count (CPU Bound Usage)
# Adjust to your CPU thread count and the programs performance.
WORKER_COUNT  = 6

# I only recommend changing these if you know what you are doing.
# I may release documentation on products soon with help from the ROC.
PRODUCT_CODES = sorted({
    'DAA', 'DHR', 'DOD', 'DPR', 'DSD', 'DSP', 'DTA', 'DU3', 'DU6', 'DVL',
    'EET', 'GSM', 'HHC',
    'N0B',  # Super Resolution Base Reflectivity (0.5°)
    'N0C', 'N0E',  # Super Resolution Spectrum Width (0.5°)
    'N0G',  # Super Resolution Base Velocity (0.5°)
    'N0H', 'N0K', 'N0M', 'N0R', 'N0S', 'N0U', 'N0V', 'N0X', 'N0Z',
    'N1B',  # Super Resolution Base Reflectivity (1.3–1.5°)
    'N1C', 'N1E',  # Super Resolution Spectrum Width (1.3–1.5°)
    'N1G',  # Super Resolution Base Velocity (1.3–1.5°)
    'N1H', 'N1K', 'N1M', 'N1P', 'N1Q', 'N1S', 'N1U', 'N1X',
    'N2B',  # Super Resolution Base Reflectivity (2.4–2.5°)
    'N2C', 'N2E',  # Super Resolution Spectrum Width (2.4–2.5°)
    'N2G',  # Super Resolution Base Velocity (2.4–2.5°)
    'N2H', 'N2K', 'N2M', 'N2Q', 'N2S', 'N2U', 'N2X',
    'N3B',  # Super Resolution Base Reflectivity (3.1–3.5°)
    'N3C', 'N3E',  # Super Resolution Spectrum Width (3.1–3.5°)
    'N3G',  # Super Resolution Base Velocity (3.1–3.5°)
    'N3H', 'N3K', 'N3M', 'N3Q', 'N3S', 'N3U', 'N3X',
    'NAB',  # Super Resolution Base Reflectivity (0.9°)
    'NAC', 'NAE',  # Super Resolution Spectrum Width (0.9°)
    'NAG',  # Super Resolution Base Velocity (0.9°)
    'NAH', 'NAK', 'NAM', 'NAQ', 'NAU', 'NAX',
    'NBB',  # Super Resolution Base Reflectivity (1.8°)
    'NBC', 'NBE',  # Super Resolution Spectrum Width (1.8°)
    'NBG',  # Super Resolution Base Velocity (1.8°)
    'NBH', 'NBK', 'NBM', 'NBQ', 'NBU', 'NBX',
    'NCR', 'NET', 'NMD', 'NST', 'NVL', 'NVW', 'NTP',
    'NXB',  # Super Resolution Base Reflectivity (-0.5 to -0.1°)
    'NXE',  # Super Resolution Spectrum Width (-0.5 to -0.1°)
    'NXG',  # Super Resolution Base Velocity (-0.5 to -0.1°)
    'NXU',  # Super Resolution Base Velocity (-0.5 to -0.1°)
    'NYB',  # Super Resolution Base Reflectivity (0.0 to 0.2°)
    'NYE',  # Super Resolution Spectrum Width (0.0 to 0.2°)
    'NYG',  # Super Resolution Base Velocity (0.0 to 0.2°)
    'NYU',  # Super Resolution Base Velocity (0.0 to 0.2°)
    'NZB',  # Super Resolution Base Reflectivity (0.3 to 0.4°)
    'NZE',  # Super Resolution Spectrum Width (0.3 to 0.4°)
    'NZG',  # Super Resolution Base Velocity (0.3 to 0.4°)
    'NZU',  # Super Resolution Base Velocity (0.3 to 0.4°)
    'OHA', 'SPD', 'SRV',
    'TR0', 'TR1', 'TR2', 'TV0', 'TV1', 'TV2', 'TZ0', 'TZ1', 'TZ2', 'TZL'
})



# unsigned boto3 client
s3 = boto3.client("s3", config=Config(signature_version=botocore.UNSIGNED))

app = Flask(__name__, static_folder=str(ROOT), static_url_path='')
# Flask isn't quite fully ready yet.

def dt_from_key(key: str) -> datetime:
    tail = key.split("_", 2)[-1]
    return datetime.strptime(tail[:19], "%Y_%m_%d_%H_%M_%S").replace(tzinfo=timezone.utc)


def list_today_files(site: str, product: str):
    now = datetime.now(timezone.utc)
    prefix = f"{site}_{product}_{now.year:04}_{now.month:02}_{now.day:02}_"
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)
    objs = []
    for pg in pages:
        for obj in pg.get("Contents", []):
            objs.append((obj["Key"], obj["LastModified"]))
    objs.sort(key=lambda x: x[1], reverse=True)
    return [key for key, _ in objs[:MAX_FILES]]


def download_and_place(key: str, site: str, product: str):
    fname = key.split("/")[-1]
    dt = dt_from_key(fname)
    stamp = dt.strftime("%Y%m%d_%H%M%S")

    dest_dir = ROOT / site / product
    dest_dir.mkdir(parents=True, exist_ok=True)

    final = dest_dir / stamp
    if final.exists():
        return

    fd, tmp = tempfile.mkstemp(dir=str(dest_dir))
    os.close(fd)

    try:
        s3.download_file(BUCKET, key, tmp)
        Path(tmp).replace(final)
    except Exception:
        os.remove(tmp)
        raise


def prune_old(site: str, product: str):
    dirp = ROOT / site / product
    if not dirp.exists():
        return

    stamps = []
    for f in dirp.iterdir():
        if "_" in f.name:
            try:
                dt = datetime.strptime(f.name, "%Y%m%d_%H%M%S")
                stamps.append((f, dt))
            except ValueError:
                pass

    stamps.sort(key=lambda x: x[1], reverse=True)
    for f, _ in stamps[MAX_FILES:]:
        f.unlink()


def write_dir_list(site: str, product: str):
    dirp = ROOT / site / product
    if not dirp.exists():
        return
    entries = []
    for f in dirp.iterdir():
        if "_" in f.name:
            try:
                dt = datetime.strptime(f.name, "%Y%m%d_%H%M%S")
                entries.append((f.name, dt))
            except ValueError:
                pass
    entries.sort(key=lambda x: x[1], reverse=True)
    with open(dirp / "dir.list", "w") as fh:
        for name, _ in entries[:MAX_FILES]:
            fh.write(name + "\n")


def write_global_config():
    ROOT.mkdir(parents=True, exist_ok=True)
    cfg = ROOT / "config.cfg"
    with open(cfg, "w") as fh:
        fh.write("; autogenerated\nListFile: dir.list\n\n")
        for site in SITES:
            fh.write(f"Site: {site}\n")
        fh.write("\n")
        for p in PRODUCT_CODES:
            fh.write(f"Product: {p}   SSS/{p}\n")

    clone = ROOT / "grlevel3.cfg"
    with open(cfg) as src, open(clone, "w") as dst:
        for line in src:
            if line.startswith("Site: "):
                site = line.split()[1]
                dst.write(f"Site: K{site}\n")
            else:
                dst.write(line)


def sync_site_product(site: str, prod: str):
    try:
        keys = list_today_files(site, prod)
        for k in keys:
            try:
                download_and_place(k, site, prod)
            except Exception as e:
                print(f"[!] Download error {site}/{prod}/{k}: {e}")
        prune_old(site, prod)
        write_dir_list(site, prod)
    except Exception as e:
        print(f"[!] Error in sync {site}/{prod}: {e}")


def sync_once():
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        futures = []
        for site in SITES:
            for prod in PRODUCT_CODES:
                futures.append(executor.submit(sync_site_product, site, prod))
        for f in as_completed(futures):
            pass
    write_global_config()

@app.route("/<path:path>")
def serve_file(path):
    return send_from_directory(str(ROOT), path)

@app.route("/")
def index():
    return send_from_directory(str(ROOT), "config.cfg")


if __name__ == "__main__":
    while True:
        sync_once()
        time.sleep(POLL_INTERVAL)
