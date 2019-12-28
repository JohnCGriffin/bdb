# bdb - big disk branches

`bdb` takes a directory argument, probably the mount point of a file
system and prints out large directory branches recursively.  This was
in response to seeing growth in total disk space, but not having an
easy way to say which areas were contributing the most to disk use.

Originally, I ran this against month-old snapshots of a growing disk,
using the included python script to print those that had grown.

### Magnetic Disk Note

This has only been used against SSD and SSD-based EBS volumes.  In my
use cases, the default of 4 threads appears optimal for total speed.
The default 4 threads on magnetic disk is likely to be inappropriate
and should be set to 1 with the '-threads 1' option.

