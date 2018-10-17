


Super simple ZFS snapshots for kafka.

```
usage: kafka_zfs_backup.py [-h] {snapshot,clean} ...

positional arguments:
  {snapshot,clean}  commands
    snapshot        Create snapshot.
    clean           Remove snapshots older than n hours.

usage: kafka_zfs_backup.py snapshot [-h] [-p POOL] [-e EXPIRE]
optional arguments:
  -h, --help            show this help message and exit
  -p POOL, --pool POOL  ZFS pool to snapshot.
  -e EXPIRE, --expire EXPIRE
                        Expire time for downtime


usage: kafka_zfs_backup.py clean [-h] [-t TIME] [-p POOL]
optional arguments:
  -h, --help            show this help message and exit
  -t TIME, --time TIME  Remove snapshots older than.
  -p POOL, --pool POOL  ZFS pool to snapshot.

```
