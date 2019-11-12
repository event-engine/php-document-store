# php-document-store

Event Engine PHP Document Store Contract

## Testing

This package includes an in-memory implementation of the `DocumentStore` interface which is useful for tests.
To be able to test the in-memory implementation in isolation we have to copy some classes from `event-engine/persistence` into the test namespace of this repo.
The implementation depends on classes from that other package, but we cannot pull it with composer due to circular dependencies.
We'll solve the issue in the future by moving the in-memory implementation to `event-engine/persistence`, but for now backwards compatibility is more important.
