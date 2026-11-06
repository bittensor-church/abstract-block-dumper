"""Which blocks a regular node may no longer hold, so that tasks read them from the archive network."""

# Blocks older than this threshold from the current head require the archive network.
ARCHIVE_BLOCK_THRESHOLD = 300
