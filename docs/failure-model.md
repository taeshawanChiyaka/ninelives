# Failure model and guarantees

The cache write path protects against readers observing partially replaced files on filesystems that provide atomic same-filesystem rename semantics. Data and metadata are flushed before replacement, and POSIX directory metadata is fsynced after replacement. A process killed before replacement leaves an orphan temporary file, not a valid final entry. A kill after replacement leaves a complete entry subject to filesystem guarantees.

No portable library can promise survival of arbitrary power loss, controller write-cache loss, filesystem bugs, or network filesystem semantics. NFS and SMB behavior varies by server, mount options, and client. v0.1 does not claim distributed locking or multi-host safety.

The normal cache path treats invalid/corrupt cache entries as misses. Function execution errors are not swallowed. Storage write errors are logged and the computed value is returned unless `strict=True`.
