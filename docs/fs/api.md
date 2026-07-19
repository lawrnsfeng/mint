# API Reference: mint.fs

`mint.fs.asynk`'s `__init__.py` re-exports nothing, so every reference
below is a fully module-qualified path (unlike `mint.db.asynk`, which does
re-export its public surface).

## Protocol

::: mint.fs.asynk.interface.IFileStorage

## Implementations

::: mint.fs.asynk.abs.AzureBlobStorage

::: mint.fs.asynk.s3.S3Storage

## Result and data structures (`mint.fs.structs`)

::: mint.fs.structs.CopyResult

::: mint.fs.structs.RemoveResult

::: mint.fs.structs.MoveResult

::: mint.fs.structs.Stat

::: mint.fs.structs.ListItem

## Exceptions (`mint.fs.exc`)

::: mint.fs.exc.FileStorageError

::: mint.fs.exc.UndefinedBucketError

::: mint.fs.exc.ObjectNotFoundError

::: mint.fs.exc.InvalidArgumentsError

::: mint.fs.exc.OperationalError

::: mint.fs.exc.FolderAlreadyExistsError

::: mint.fs.exc.FileAlreadyExistsError

::: mint.fs.exc.MoveCleanupError

::: mint.fs.exc.TrailingSlashNotAllowedError

::: mint.fs.exc.UnsupportedRefTypeError

::: mint.fs.exc.AmbiguousFolderPathError

::: mint.fs.exc.CopySourceTooLargeError
