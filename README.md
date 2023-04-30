[![build](https://github.com/bcxpro/ebsnapshot/actions/workflows/build.yml/badge.svg)](https://github.com/bcxpro/ebsnapshot/actions/workflows/build.yml)
[![tests](https://github.com/bcxpro/ebsnapshot/actions/workflows/code.yml/badge.svg)](https://github.com/bcxpro/ebsnapshot/actions/workflows/code.yml)
[![Release][release-badge]][release]

**_ebsnapshot_** is a tool that simplifies the process of backing up and restoring AWS EC2 EBS snapshots. With _ebsnapshot_, you can create backups of your snapshots and store them in Amazon S3 or in files on your local machine. Additionally, _ebsnapshot_ supports backup to Amazon Glacier, providing a cost-effective way to archive your data. _ebsnapshot_ also includes compression functionality to help reduce the storage space required for your backups.

If you're looking to backup your snapshots for disaster recovery purposes or simply to archive your data, _ebsnapshot_ can help you.

## Formats
### Binary format
_ebsnapshot_ [snapshot binary format](./docs/snapshot_format.md) is a proprietary compressed format used to store disk images from EBS snapshots. The format is designed to provide a tradeoff between storage costs and restore speeds. The compression used in this format results in slower processing, but it makes archiving files less expensive.

_ebsnapshot_ is capable of creating these binary snapshots files, and restoring them by creating an EBS snapshot from a binary snapshot file that was previously backed up. This feature enables _ebsnapshot_ to provide faster and more cost-effective snapshot backups, as well as streamlined snapshot restores.

Please note that _ebsnapshot_'s snapshot binary format is a proprietary format and can only be read and written by _ebsnapshot_. Other backup tools or services are not compatible with this format.

### Raw format
_ebsnapshot_ is capable of creating backups of EBS snapshots in the raw disk binary format. This format represents a sector-by-sector copy of the EBS volume, including all data and metadata. It can be useful in scenarios where the original volume needs to be restored as an exact replica, such as in disaster recovery or forensic analysis.

To create a backup in raw disk binary format, _ebsnapshot_ reads the EBS snapshot block-by-block and writes the content to a binary file. The resulting file has the same size as the original EBS volume, and can be used to restore the volume using various disk imaging tools.

It's important to note that the raw disk binary format doesn't provide any compression or deduplication capabilities, which can result in larger backup sizes compared to other formats. Additionally, the format may not be suitable for backups of large volumes due to its size and potential limitations of the backup storage.

In addition, _ebsnapshot_ can restore a raw binary disk file and create an EBS snapshot from it. This feature is particularly useful for converting existing disks in raw binary format to EBS snapshots, which can then be used to create EBS volumes.

### Format conversion

_ebsnapshot_ provides a conversion functionality to convert between two different disk image formats: the raw disk format and the _ebsnapshot_ snapshot binary format. The conversion process can be performed using the ebsnapshot convert command.

Converting a raw disk binary format to a _ebsnapshot_ snapshot binary format can be useful for archiving the disk image in S3 or other storage systems, as the _ebsnapshot_ snapshot binary format is compressed and can take up less storage space. 

## Examples

### Backup to S3

Backup an EBS snapshot to an S3 object in binary format.

````sh
ebsnapshot backup -s snap-066ede6825bc967e3 -d s3://mybucket/root.snap
````

Backup an EBS snapshot to an S3 object in raw format:

````sh
ebsnapshot backup -s snap-066ede6825bc967e3 -d s3://mybucket/root.snap --rawdst
````

Backup an EBS snapshot to an S3 object in binary format. Store the object in Glacier Deep Archive directly.

````sh
ebsnapshot backup -s snap-066ede6825bc967e3 -d s3://mybucket/root.snap --storage-class DEEP_ARCHIVE
````

### Backup to files

Backup an EBS snapshot to a file in binary format.

````sh
ebsnapshot backup -s snap-066ede6825bc967e3 -d /mnt/backups/root.snap
````

Backup an EBS snapshot to standard output in raw format. Then gzip it and store it in a file.

````sh
ebsnapshot backup -s snap-066ede6825bc967e3 -d - --rawdst | gzip > /tmp/backup.bin.gz
````

### Restore from S3

Restore a binary snapshot backed up on S3. As a result a new EBS snapshot is created.

```sh
ebsnapshot restore --source s3://brdev-backups/myvolume.snap
```

Restore a raw snapshot backed up on S3. As a result a new EBS snapshot is created.

```sh
ebsnapshot restore --source s3://brdev-backups/myvolume.bin --rawsrc
```

### Restore from files

Restore a binary snapshot backed up on S3. As a result a new EBS snapshot is created.

```sh
ebsnapshot restore --source /images/myvolume.snap
```

Restore a raw snapshot backed up on S3. As a result a new EBS snapshot is created.

```sh
ebsnapshot restore --source /images/myvolume.bin --rawsrc
```

### Convert formats

Convert a binary /dev/nvme1n1 snapshot stored in S3 to a raw disk on a Linux machine. This allows for a direct restore to a disk block device without the need to create an EBS snapshot to restore a volume. It's important to note that the destination disk device must be backed by a volume of the same size as the source snapshot, once uncompressed. This feature is particularly useful for scenarios where you want to avoid the overhead of creating an EBS snapshot.

You can also use this kind of conversion to restore a binary snapshot to an on-premise volume.
 
```sh
ebsnapshot convert --source s3://brdev-backups/myvolume.snap --dest /dev/nvme1n1 --rawdst
```

Given a Linux block device back it up to a binary snapshot stored in S3. This may be useful to archive an on-premise volume.

```sh
ebsnapshot convert --source /dev/nvme1n1 --rawsrc --dest s3://brdev-backups/myvolume.snap
```


## IAM Permissions to backup from and restore to EBS snapshots

Sample IAM policy document that allows to backups EBS snapshots to S3 and restore snapshots from S3, or backup and restore to local files or devices.


```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3ReadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::mybucket/*"
        },
        {
            "Sid": "S3WriteAccess",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:AbortMultipartUpload"
            ],
            "Resource": "arn:aws:s3:::mybucket/*"
        },
        {
            "Sid": "EBSAccess",
            "Effect": "Allow",
            "Action": [
                "ebs:ListSnapshotBlocks",
                "ebs:StartSnapshot",
                "ec2:CreateTags",
                "ebs:PutSnapshotBlock",
                "ebs:GetSnapshotBlock",
                "ebs:CompleteSnapshot"
            ],
            "Resource": "arn:aws:ec2:*::snapshot/*"
        },
        {
            "Sid": "EC2DescribeSnapshotsAccess",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeSnapshots"
            ],
            "Resource": "*"
        }
    ]
}
```

The policy has four statements, each with a unique Sid identifier:

* _S3ReadAccess_: This statement allows users to read (retrieve) objects from the S3 bucket named "mybucket" using the s3:GetObject action.
* _S3WriteAccess_: This statement allows users to write (upload) objects to the same S3 bucket using the s3:PutObject and s3:AbortMultipartUpload actions.
* _EBSAccess_: This statement grants access to create, modify, and retrieve data from Elastic Block Store (EBS) snapshots using the ebs:ListSnapshotBlocks, ebs:StartSnapshot, ec2:CreateTags, ebs:PutSnapshotBlock, ebs:GetSnapshotBlock, and ebs:CompleteSnapshot actions. The Resource in this statement specifies that the permissions apply to all EBS snapshots in all regions (arn:aws:ec2:\*::snapshot/*).
* _EC2DescribeSnapshotsAccess_: This statement allows users to list (retrieve information about) EBS snapshots in all regions using the ec2:DescribeSnapshots action. The Resource for this statement is set to "*", meaning that the permissions apply to all resources.

To remove the possibility of reading or writing to S3 just remove the `S3ReadAccess` and/or `S3WriteAccess` statements.

## Disclaimer

:warning: _ebsnapshot_ is an experimental tool for backing up and restoring EBS snapshots to and from S3/files. While we have extensively tested the tool to ensure it functions as intended, it is important to note that there is no warranty provided with the software. Therefore, we cannot guarantee that you will not lose any data while using this tool. It is important to use _ebsnapshot_ at your own risk and to ensure that you have appropriate backups and contingency plans in place before using it to handle critical data.

[release-badge]: https://img.shields.io/github/v/release/bcxpro/ebsnapshot?label=Release
[release]: https://github.com/bcxpro/ebsnapshot/releases/latest
