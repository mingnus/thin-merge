# Introduction

thin_merge merges the data mappings of a dm-thin external snapshot with its origin, turning the external snapshot into a regular dm-thin device. This functionality is essential for live block copying from the external origin.


# Use case

The main use case of thin_merge is to clone a read-only thin volume to another thin-pool instantly for writable access, with dm-thin's external snapshot functionality. An external snapshot is initiated on the destination thin-pool for instant writable access, while the external origin is copied onto a temporary thin device of the destination pool in the background. Once the cloning of the external origin is complete, thin_merge merges the external snapshot with the temporary thin device into a one, as if the source is being copied live onto the destination volume.

The use case closely resembles that of dm-clone. However, dm-clone isn't optimized for copying thinly-provisioned sources, although it could be implemented.


# Building

Install the Rust toolchain, then run the build command:

```bash
cargo build --release
```

This will create the output binary ./target/release/thin_merge.


# Installing

Installation is done via the 'make' tool.

```bash
make install
```

You could change the installation location by specifying DESTDIR and prefix. For example:

```bash
make DESTDIR=/tmp/stage PREFIX=/usr/local install
```


# Quick examples

Given the metadata of the destination thin-pool, thin_merge creates an output metadata with merged mappings on a different device or file. The output is then used to replace the metadata of the destination pool, resulting in a thin-pool with one merged device.

```
thin_merge -i /dev/mapper/pool_meta -o /dev/mapper/output_meta --snapshot 1 --origin 2
```

Apply the `-m` option to access the metadata snapshot of a live thin-pool. Enabling asynchronous IO is also suggested (requires io_uring), as this operation is time-critical:

```
thin_merge -i /dev/mapper/pool_meta -o /dev/mapper/output_meta --snapshot 1 --origin 2 -m --io-engine async
```
