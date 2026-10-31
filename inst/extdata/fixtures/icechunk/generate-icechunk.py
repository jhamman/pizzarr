"""Generate the Icechunk test repositories used by tests/testthat/test-icechunk.R.

Writes icechunk.zip next to this script, holding:

- basic.icechunk/   arrays covering dtypes, codecs, sharding and missing
                    chunks, plus branch / tag / snapshot history;
- virtual.icechunk/ one array whose chunks are virtual references into
                    virtual-source/data.bin, through a container named
                    "fixtures" (relative vcc://fixtures/... references, so
                    the repository can be unzipped anywhere);
- virtual-source/   the file the virtual chunks point into;
- refs.json         snapshot ids the tests need.

Values follow simple formulas so the tests can recompute them in R; see
test-icechunk.R.

Requires: icechunk 2.3, zarr >= 3.1, numpy. Run from this directory:

    python generate-icechunk.py
"""

import json
import os
import shutil
import tempfile
import zipfile

import icechunk
import numpy as np
import zarr
from zarr.codecs import (
    BloscCodec,
    BytesCodec,
    Crc32cCodec,
    GzipCodec,
    ShardingCodec,
    TransposeCodec,
    ZstdCodec,
)

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_ZIP = os.path.join(HERE, "icechunk.zip")


def write_basic(path):
    # Store every chunk as its own object rather than inline in the manifest,
    # so reads exercise Icechunk's chunk fetches.
    config = icechunk.RepositoryConfig(inline_chunk_threshold_bytes=0)
    repo = icechunk.Repository.create(icechunk.local_filesystem_storage(path), config=config)

    # --- first commit: data arrays and version marker = 1 ---------------
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, zarr_format=3)
    root.attrs["title"] = "pizzarr icechunk fixture"

    # 2-d arrays of shape (6, 5), chunks (3, 2): value = 10 * i + j
    base = np.add.outer(np.arange(6) * 10, np.arange(5))
    dtype_codecs = [
        ("int8", None, "plain"),
        ("int16", GzipCodec(level=1), "gzip"),
        ("int32", ZstdCodec(level=1), "zstd"),
        ("int64", BloscCodec(cname="lz4"), "blosc"),
        ("uint8", None, "plain"),
        ("uint16", GzipCodec(), "gzip"),
        ("uint32", ZstdCodec(), "zstd"),
        ("uint64", BloscCodec(cname="zstd"), "blosc"),
        ("float32", ZstdCodec(), "zstd"),
        ("float64", GzipCodec(), "gzip"),
    ]
    for dtype, compressor, label in dtype_codecs:
        a = root.create_array(
            f"{dtype}_{label}",
            shape=(6, 5),
            chunks=(3, 2),
            dtype=dtype,
            compressors=compressor,
            filters=None,
            fill_value=0,
        )
        values = base.astype(dtype)
        if dtype.startswith("float"):
            values = values + 0.5
        a[:] = values

    b = root.create_array(
        "bool", shape=(7,), chunks=(3,), dtype="bool", compressors=None, fill_value=False
    )
    b[:] = np.arange(7) % 3 == 0

    # Big-endian bytes, crc32c checksum and transpose.
    zarr.create_array(
        store=session.store,
        name="float64_bigendian_crc32c",
        shape=(4, 3),
        chunks=(2, 3),
        dtype="float64",
        serializer=BytesCodec(endian="big"),
        compressors=(Crc32cCodec(),),
        filters=None,
        fill_value=0,
    )[:] = np.arange(12, dtype="float64").reshape(4, 3) / 4
    zarr.create_array(
        store=session.store,
        name="int32_transpose",
        shape=(4, 6),
        chunks=(2, 3),
        dtype="int32",
        filters=(TransposeCodec(order=(1, 0)),),
        compressors=None,
        fill_value=0,
    )[:] = np.arange(24, dtype="int32").reshape(4, 6)

    # Missing chunks: shape (8, 8), chunks (4, 4), fill -1, only chunk (0, 0)
    # written.
    m = root.create_array(
        "missing", shape=(8, 8), chunks=(4, 4), dtype="int32", fill_value=-1,
        compressors=ZstdCodec(),
    )
    m[0:4, 0:4] = np.arange(16, dtype="int32").reshape(4, 4)

    # Sharding: shape (8, 8), shards (4, 4), inner chunks (2, 2), blosc.
    # value = 8 * i + j.
    s = root.create_array(
        "sharded", shape=(8, 8), shards=(4, 4), chunks=(2, 2), dtype="float32",
        compressors=BloscCodec(cname="zstd"), fill_value=0,
    )
    s[:] = np.arange(64, dtype="float32").reshape(8, 8)

    # Sparse sharded: shape (8, 8), shards (4, 4), inner chunks (2, 2),
    # fill NaN. Only inner chunk (0, 0) of shard (0, 0) is written: the rest
    # of that shard is missing inner chunks, the other shards are missing.
    sp = root.create_array(
        "sharded_sparse", shape=(8, 8), shards=(4, 4), chunks=(2, 2),
        dtype="float64", compressors=ZstdCodec(), fill_value=float("nan"),
    )
    sp[0:2, 0:2] = np.array([[1.0, 2.0], [3.0, 4.0]])

    # A nested group.
    sub = root.create_group("sub")
    sub.attrs["level"] = 1
    sub.create_array("arr", shape=(3,), chunks=(3,), dtype="int16", fill_value=0)[:] = [7, 8, 9]

    # Version marker: shape (4,), every value = the version number.
    root.create_array("version", shape=(4,), chunks=(2,), dtype="int32", fill_value=0)[:] = 1
    snap1 = session.commit("version 1")
    repo.create_tag("v1", snapshot_id=snap1)

    # --- main moves on to version 2 --------------------------------------
    session = repo.writable_session("main")
    zarr.open_array(session.store, path="version", mode="r+")[:] = 2
    snap2 = session.commit("version 2")

    # --- branch dev from version 1, then version 3 on it ------------------
    repo.create_branch("dev", snapshot_id=snap1)
    session = repo.writable_session("dev")
    zarr.open_array(session.store, path="version", mode="r+")[:] = 3
    snap3 = session.commit("version 3 on dev")

    return {"v1": snap1, "main": snap2, "dev": snap3}


def write_virtual(path, source_dir):
    os.makedirs(source_dir)
    values = np.arange(8, dtype="<f8") * 1.5
    with open(os.path.join(source_dir, "data.bin"), "wb") as f:
        f.write(b"HEAD")  # 4 bytes of padding so offsets are non-zero
        f.write(values.tobytes())

    prefix = "file://" + os.path.abspath(source_dir) + "/"
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            prefix, icechunk.local_filesystem_store(source_dir), name="fixtures"
        )
    )
    repo = icechunk.Repository.create(
        icechunk.local_filesystem_storage(path),
        config=config,
        authorize_virtual_chunk_access={
            prefix: icechunk.credentials.LocalFileSystemAccess
        },
    )
    session = repo.writable_session("main")
    root = zarr.group(store=session.store, zarr_format=3)
    root.create_array(
        "v", shape=(8,), chunks=(4,), dtype="float64", compressors=None,
        filters=None, fill_value=0,
    )
    for i in range(2):
        session.store.set_virtual_ref(
            f"v/c/{i}",
            "vcc://fixtures/data.bin",
            offset=4 + 32 * i,
            length=32,
        )
    session.commit("virtual chunks")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        refs = write_basic(os.path.join(tmp, "basic.icechunk"))
        write_virtual(
            os.path.join(tmp, "virtual.icechunk"), os.path.join(tmp, "virtual-source")
        )
        refs["generator"] = (
            f"icechunk {icechunk.__version__}, zarr {zarr.__version__}"
        )
        with open(os.path.join(tmp, "refs.json"), "w") as f:
            json.dump(refs, f, indent=2)

        if os.path.exists(OUT_ZIP):
            os.remove(OUT_ZIP)
        with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
            for dirpath, _, files in sorted(os.walk(tmp)):
                for name in sorted(files):
                    full = os.path.join(dirpath, name)
                    z.write(full, os.path.relpath(full, tmp))
    print("wrote", OUT_ZIP)


if __name__ == "__main__":
    main()
