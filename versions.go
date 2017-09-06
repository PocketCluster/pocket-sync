package gosync

const (
    PocketSyncDefaultBlockSize = 16384
    PocketSyncMagicString      = "G0S9ND" // just to confirm the file type is used correctly
    PocketSyncMajorVersion     = uint16(0)
    PocketSyncMinorVersion     = uint16(1)
    PocketSyncPatchVersion     = uint16(4)
)
