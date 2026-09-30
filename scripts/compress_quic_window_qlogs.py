"""Losslessly compress the October 1 QUIC window matrix qlogs for Git."""

import gzip
import hashlib
import shutil
from pathlib import Path


repo = Path(__file__).resolve().parents[1]
folders = (
    repo / "test-results/quic-window-matrix-20261001",
    repo / "test-results/quic-window-matrix-transparent-20261001",
)


def digest(stream):
    hasher = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        hasher.update(chunk)
    return hasher.digest()


for folder in folders:
    for source in sorted(folder.glob("*.qlog.sqlog")):
        archive = source.with_name(source.name + ".gz")
        if archive.exists():
            raise FileExistsError(archive)
        with source.open("rb") as input_file, archive.open("wb") as output_file:
            with gzip.GzipFile(fileobj=output_file, mode="wb", filename="", mtime=0) as compressed:
                shutil.copyfileobj(input_file, compressed)
        with source.open("rb") as original, gzip.open(archive, "rb") as restored:
            if digest(original) != digest(restored):
                raise ValueError(f"gzip verification failed for {source}")
        print(f"{source.name}: {source.stat().st_size} -> {archive.stat().st_size}")
        source.unlink()
