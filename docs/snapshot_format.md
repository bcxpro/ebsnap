# Binary snapshot format

A binary snapshot is a propietary format of _ebsnapshot_. It consists of a list of _SnapshotRecord_ records, which are defined in protobuf format. Each snapshot contains a sequence of records organized as follows:

- _SnapshotHeader_ record: contains the binary snapshot format version, snapshot id, parent snapshot id, volume id, volume size, description, and tags.
- _Block_ records: as many records of this type as there are non-empty blocks in the snapshot. Each record includes the block index, checksum algorithm (currently only SHA256 is supported), checksum (the bytes that compose the checksum, such as the 32 bytes that compose a SHA256), encoding type (RAW, GZIP, or LZ4), and data (the bytes composing the block data).
- _BlockList_ record: an ordered list of the block indexes that were included in the previous Block records.

In the file, each encoded record has the format:

- A 4-byte big-endian uint32 length, indicating the length of the marshalled record.
- A marshalled protobuf of SnapshotRecord (that is a marshalled _SnapshotHeader_, _Block_ or _BlockList_).
- A SHA256 hash (32 bytes) of the previous encoded bytes on record (the length + marshalled protobuf record).