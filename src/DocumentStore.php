<?php
/**
 * This file is part of the event-engine/document-store.
 * (c) 2018-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore;

use EventEngine\DocumentStore\Exception\RuntimeException;
use EventEngine\DocumentStore\Exception\UnknownCollection;
use EventEngine\DocumentStore\Filter\Filter;
use EventEngine\DocumentStore\OrderBy\OrderBy;

interface DocumentStore
{
    /**
     * @return string[] list of all available collections
     */
    public function listCollections(): array;

    /**
     * @param string $prefix
     * @return string[] of collection names
     */
    public function filterCollectionsByPrefix(string $prefix): array;

    /**
     * @param string $collectionName
     * @return bool
     */
    public function hasCollection(string $collectionName): bool;

    /**
     * @param string $collectionName
     * @param Index[] ...$indices
     * @throws RuntimeException if adding did not succeed
     */
    public function addCollection(string $collectionName, Index ...$indices): void;

    /**
     * @param string $collectionName
     * @throws RuntimeException if dropping did not succeed
     */
    public function dropCollection(string $collectionName): void;

    public function hasCollectionIndex(string $collectionName, string $indexName): bool;

    /**
     * @param string $collectionName
     * @param Index $index
     * @throws RuntimeException if adding did not succeed
     */
    public function addCollectionIndex(string $collectionName, Index $index): void;

    /**
     * @param string $collectionName
     * @param string|Index $index
     * @throws RuntimeException if dropping did not succeed
     */
    public function dropCollectionIndex(string $collectionName, $index): void;

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $doc
     * @throws UnknownCollection
     * @throws RuntimeException if adding did not succeed
     */
    public function addDoc(string $collectionName, string $docId, array $doc): void;

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws UnknownCollection
     * @throws RuntimeException if updating did not succeed
     */
    public function updateDoc(string $collectionName, string $docId, array $docOrSubset): void;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param array $set
     * @throws UnknownCollection
     * @throws RuntimeException in case of connection error or other issues
     */
    public function updateMany(string $collectionName, Filter $filter, array $set): void;

    /**
     * Same as updateDoc except that doc is added to collection if it does not exist.
     *
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws UnknownCollection
     * @throws RuntimeException if insert/update did not succeed
     */
    public function upsertDoc(string $collectionName, string $docId, array $docOrSubset): void;

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $doc
     * @throws UnknownCollection
     * @throws RuntimeException if updating did not succeed
     */
    public function replaceDoc(string $collectionName, string $docId, array $doc): void;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param array $set
     * @throws UnknownCollection
     * @throws RuntimeException in case of connection error or other issues
     */
    public function replaceMany(string $collectionName, Filter $filter, array $set): void;

    /**
     * @param string $collectionName
     * @param string $docId
     * @throws UnknownCollection
     * @throws RuntimeException if deleting did not succeed
     */
    public function deleteDoc(string $collectionName, string $docId): void;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @throws UnknownCollection
     * @throws RuntimeException in case of connection error or other issues
     */
    public function deleteMany(string $collectionName, Filter $filter): void;

    /**
     * @param string $collectionName
     * @param string $docId
     * @return array|null
     * @throws UnknownCollection
     */
    public function getDoc(string $collectionName, string $docId): ?array;

    /**
     * @param string $collectionName
     * @param PartialSelect $partialSelect
     * @param string $docId
     * @return array|null
     */
    public function getPartialDoc(string $collectionName, PartialSelect $partialSelect, string $docId): ?array;

    /**
     * @deprecated use findDocs instead
     *
     * @param string $collectionName
     * @param Filter $filter
     * @param int|null $skip
     * @param int|null $limit
     * @param OrderBy|null $orderBy
     * @return \Traversable list of docs
     * @throws UnknownCollection
     */
    public function filterDocs(string $collectionName, Filter $filter, ?int $skip = null, ?int $limit = null, ?OrderBy $orderBy = null): \Traversable;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param int|null $skip
     * @param int|null $limit
     * @param OrderBy|null $orderBy
     * @return \Traversable list of docs with key being the docId and value being the stored doc
     * @throws UnknownCollection
     */
    public function findDocs(string $collectionName, Filter $filter, ?int $skip = null, ?int $limit = null, ?OrderBy $orderBy = null): \Traversable;

    /**
     * @param string $collectionName
     * @param PartialSelect $partialSelect
     * @param Filter $filter
     * @param int|null $skip
     * @param int|null $limit
     * @param OrderBy|null $orderBy
     * @return \Traversable list of docs with key being the docId and value being the stored doc
     * @throws UnknownCollection
     */
    public function findPartialDocs(string $collectionName, PartialSelect $partialSelect, Filter $filter, ?int $skip = null, ?int $limit = null, ?OrderBy $orderBy = null): \Traversable;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @return array
     */
    public function filterDocIds(string $collectionName, Filter $filter): array;

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @return int The number of documents
     * @throws UnknownCollection
     */
    public function countDocs(string $collectionName, Filter $filter): int;
}
