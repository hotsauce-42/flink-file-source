# SourceFunction

Flink 2.0.0 / Java 17 project: a re-implementation of Flink's continuous `FileSource` that
tracks a single `lastProcessedModificationTime` watermark instead of `HashSet<Path> alreadyProcessedPaths`,
so changed files are re-read and enumerator state stays bounded.

## Layout
- Real code: `src/main/java/org/example/filesource` (the `Own*` classes + the S3/MinIO override).
- `src/main/java/nessi/*` is **dead scratch code** (old Minio experiments, mostly commented out; gitignored) — ignore it.

## Conventions
- The project deliberately **forks Flink connector classes** with minimal changes (`OwnFileSource`,
  `OwnAbstractFileSource`, `OwnContinuousFileSplitEnumerator`, `MinioStreamFormatAdapter`, …).
  Keep forks faithful to the upstream class and list them in `src/main/java/org/example/filesource/README.md`.
- Formatting: Spotless (google-java-format). Run `mvn spotless:apply` before building — `spotless:check`
  runs at the `validate` phase and fails the build on violations.
- Tests: JUnit 5 (Jupiter) + AssertJ.

## Optional S3/MinIO client (`withFileSystem(...)`)
- `OwnFileSource.FileSourceBuilder.withFileSystem(S3FileSystemSettings)` makes one source read from a
  different S3/MinIO endpoint via a minio-backed Flink `FileSystem`, decoupled from the global filesystem.
- Unset = unchanged behavior (global `FileSystem`). Settings are serializable; the client is built lazily per JVM.

### Known issues / TODO
- `MinioInputStream` opens the object twice for split offset 0 (constructor `seek(0)` + adapter `seek(0)`) — one redundant `GetObject`; could lazy-init the stream.
- Bucket-root (empty prefix) input path makes `getFileStatus` call `statObject("")`, which minio rejects with `IllegalArgumentException` (not `ErrorResponseException`) → surfaces as `IOException` instead of being treated as a directory. Pass a non-empty prefix, or special-case empty key → dir.
- No live-MinIO integration test (none available in the env); only builder wiring is unit-tested in `S3FileSystemBuilderTest`.
