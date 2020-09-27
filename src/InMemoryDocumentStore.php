<?php
/**
 * This file is part of event-engine/php-document-store.
 * (c) 2018-2019 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore;

use Codeliner\ArrayReader\ArrayReader;
use EventEngine\DocumentStore\Exception\RuntimeException;
use EventEngine\DocumentStore\Exception\UnknownCollection;
use EventEngine\DocumentStore\Filter\AndFilter;
use EventEngine\DocumentStore\Filter\EqFilter;
use EventEngine\DocumentStore\Filter\Filter;
use EventEngine\DocumentStore\OrderBy\AndOrder;
use EventEngine\DocumentStore\OrderBy\Asc;
use EventEngine\DocumentStore\OrderBy\Desc;
use EventEngine\DocumentStore\OrderBy\OrderBy;
use EventEngine\Persistence\InMemoryConnection;
use function array_key_exists;
use function count;
use function explode;
use function is_array;
use function json_encode;

final class InMemoryDocumentStore implements DocumentStore
{
    /**
     * @var InMemoryConnection
     */
    private $inMemoryConnection;

    public function __construct(InMemoryConnection $inMemoryConnection)
    {
        $this->inMemoryConnection = $inMemoryConnection;
    }

    /**
     * @return string[] list of all available collections
     */
    public function listCollections(): array
    {
        return \array_keys($this->inMemoryConnection['documents']);
    }

    /**
     * @param string $prefix
     * @return string[] of collection names
     */
    public function filterCollectionsByPrefix(string $prefix): array
    {
        return \array_filter(\array_keys($this->inMemoryConnection['documents']), function (string $colName) use ($prefix): bool {
            return \mb_strpos($colName, $prefix) === 0;
        });
    }

    /**
     * @param string $collectionName
     * @return bool
     */
    public function hasCollection(string $collectionName): bool
    {
        return \array_key_exists($collectionName, $this->inMemoryConnection['documents']);
    }

    /**
     * @param string $collectionName
     * @param Index[] ...$indices
     */
    public function addCollection(string $collectionName, Index ...$indices): void
    {
        $this->inMemoryConnection['documents'][$collectionName] = [];
        $this->inMemoryConnection['documentIndices'][$collectionName] = $indices;
    }

    /**
     * @param string $collectionName
     * @throws \Throwable if dropping did not succeed
     */
    public function dropCollection(string $collectionName): void
    {
        if ($this->hasCollection($collectionName)) {
            unset($this->inMemoryConnection['documents'][$collectionName]);
            unset($this->inMemoryConnection['documentIndices'][$collectionName]);
        }
    }

    public function hasCollectionIndex(string $collectionName, string $indexName): bool
    {
        foreach ($this->inMemoryConnection['documentIndices'][$collectionName] as $index) {
            if($index instanceof FieldIndex || $index instanceof MultiFieldIndex) {
                if($index->name() === $indexName) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param string $collectionName
     * @param Index $index
     * @throws RuntimeException if adding did not succeed
     */
    public function addCollectionIndex(string $collectionName, Index $index): void
    {
        $this->assertHasCollection($collectionName);

        if($index instanceof FieldIndex || $index instanceof MultiFieldIndex) {
            $this->dropCollectionIndex($collectionName, $index->name());
        }

        $docsCount = count($this->inMemoryConnection['documents'][$collectionName]);

        if($docsCount > 1 && ($index instanceof FieldIndex || $index instanceof MultiFieldIndex)  && $index->unique()) {
            $uniqueErrMsg = "Unique constraint violation. Cannot add unique index because existing documents conflict with it!";
            $assertMethod = $index instanceof FieldIndex? 'assertUniqueFieldConstraint' : 'assertMultiFieldUniqueConstraint';
            $checkCount = 0;
            $halfOfDocs = $docsCount / 2;

            foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $document) {
                if($checkCount > $halfOfDocs) {
                    break;
                }

                // Temp unset to prevent false positives
                unset($this->inMemoryConnection['documents'][$collectionName][$docId]);
                try {
                    $this->{$assertMethod}($collectionName, (string)$docId, $document, $index, $uniqueErrMsg);
                } catch (\Throwable $e) {
                    $this->inMemoryConnection['documents'][$collectionName][$docId] = $document;
                    throw $e;
                }

                $checkCount++;
            }
        }

        $this->inMemoryConnection['documentIndices'][$collectionName][] = $index;
    }

    /**
     * @param string $collectionName
     * @param string|Index $index
     * @throws RuntimeException if dropping did not succeed
     */
    public function dropCollectionIndex(string $collectionName, $index): void
    {
        if(is_string($index)) {
            foreach ($this->inMemoryConnection['documentIndices'][$collectionName] as $idxI => $existingIndex) {
                if($existingIndex instanceof FieldIndex || $existingIndex instanceof MultiFieldIndex) {
                    if($existingIndex->name() === $index) {
                        unset($this->inMemoryConnection['documentIndices'][$collectionName][$idxI]);
                    }
                }
            }

            $this->inMemoryConnection['documentIndices'][$collectionName] = array_values($this->inMemoryConnection['documentIndices'][$collectionName]);

            return;
        }

        foreach ($this->inMemoryConnection['documentIndices'][$collectionName] as $idxI => $existingIndex) {
            if($existingIndex === $index) {
                unset($this->inMemoryConnection['documentIndices'][$collectionName][$idxI]);
            }
        }

        $this->inMemoryConnection['documentIndices'][$collectionName] = array_values($this->inMemoryConnection['documentIndices'][$collectionName]);
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $doc
     * @throws \Throwable if adding did not succeed
     */
    public function addDoc(string $collectionName, string $docId, array $doc): void
    {
        $this->assertHasCollection($collectionName);

        if ($this->hasDoc($collectionName, $docId)) {
            throw new RuntimeException("Cannot add doc with id $docId. The doc already exists in collection $collectionName");
        }

        $this->assertUniqueConstraints($collectionName, $docId, $doc);

        $this->inMemoryConnection['documents'][$collectionName][$docId] = $doc;
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws \Throwable if updating did not succeed
     */
    public function updateDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $this->assertDocExists($collectionName, $docId);
        $this->assertUniqueConstraints($collectionName, $docId, $docOrSubset);

        $this->inMemoryConnection['documents'][$collectionName][$docId] = $this->arrayReplace(
            $this->inMemoryConnection['documents'][$collectionName][$docId],
            $docOrSubset
        );
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param array $set
     * @throws \Throwable in case of connection error or other issues
     */
    public function updateMany(string $collectionName, Filter $filter, array $set): void
    {
        $this->assertHasCollection($collectionName);

        $docs = $this->inMemoryConnection['documents'][$collectionName];

        foreach ($docs as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $this->updateDoc($collectionName, (string)$docId, $set);
            }
        }
    }

    /**
     * Same as updateDoc except that doc is added to collection if it does not exist.
     *
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws \Throwable if insert/update did not succeed
     */
    public function upsertDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        if ($this->hasDoc($collectionName, $docId)) {
            $this->updateDoc($collectionName, $docId, $docOrSubset);
        } else {
            $this->addDoc($collectionName, $docId, $docOrSubset);
        }
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @throws \Throwable if deleting did not succeed
     */
    public function deleteDoc(string $collectionName, string $docId): void
    {
        if ($this->hasDoc($collectionName, $docId)) {
            unset($this->inMemoryConnection['documents'][$collectionName][$docId]);
        }
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @throws \Throwable in case of connection error or other issues
     */
    public function deleteMany(string $collectionName, Filter $filter): void
    {
        $this->assertHasCollection($collectionName);

        $docs = $this->inMemoryConnection['documents'][$collectionName];

        foreach ($docs as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $this->deleteDoc($collectionName, (string)$docId);
            }
        }
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @return array|null
     */
    public function getDoc(string $collectionName, string $docId): ?array
    {
        return $this->inMemoryConnection['documents'][$collectionName][$docId] ?? null;
    }

    /**
     * @inheritDoc
     */
    public function getPartialDoc(string $collectionName, PartialSelect $partialSelect, string $docId): ?array
    {
        $doc = $this->inMemoryConnection['documents'][$collectionName][$docId] ?? null;

        if(null === $doc) {
            return null;
        }

        return $this->transformToPartialDoc($doc, $partialSelect);
    }

    /**
     * @inheritDoc
     */
    public function filterDocs(
        string $collectionName,
        Filter $filter,
        int $skip = null,
        int $limit = null,
        OrderBy $orderBy = null): \Traversable
    {
        $this->assertHasCollection($collectionName);

        $filteredDocs = [];

        foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $filteredDocs[$docId] = ['doc' => $doc, 'docId' => $docId];
            }
        }

        $filteredDocs = \array_values($filteredDocs);

        if ($orderBy !== null) {
            $this->sort($filteredDocs, $orderBy);
        }

        if ($skip !== null) {
            $filteredDocs = \array_slice($filteredDocs, $skip, $limit);
        } elseif ($limit !== null) {
            $filteredDocs = \array_slice($filteredDocs, 0, $limit);
        }

        $docsMap = [];

        foreach ($filteredDocs as $docAndId) {
            $docsMap[] = $docAndId['doc'];
        }

        return new \ArrayIterator($docsMap);
    }

    /**
     * @inheritDoc
     */
    public function findDocs(
        string $collectionName,
        Filter $filter,
        int $skip = null,
        int $limit = null,
        OrderBy $orderBy = null): \Traversable
    {
        $this->assertHasCollection($collectionName);

        $filteredDocs = [];

        foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $filteredDocs[$docId] = ['doc' => $doc, 'docId' => $docId];
            }
        }

        $filteredDocs = \array_values($filteredDocs);

        if ($orderBy !== null) {
            $this->sort($filteredDocs, $orderBy);
        }

        if ($skip !== null) {
            $filteredDocs = \array_slice($filteredDocs, $skip, $limit);
        } elseif ($limit !== null) {
            $filteredDocs = \array_slice($filteredDocs, 0, $limit);
        }

        $docsMap = [];

        foreach ($filteredDocs as $docAndId) {
            $docsMap[$docAndId['docId']] = $docAndId['doc'];
        }

        return new \ArrayIterator($docsMap);
    }

    /**
     * @param string $collectionName
     * @param PartialSelect $partialSelect
     * @param Filter $filter
     * @param int|null $skip
     * @param int|null $limit
     * @param OrderBy|null $orderBy
     * @return \Traversable list of docs
     */
    public function findPartialDocs(
        string $collectionName,
        PartialSelect $partialSelect,
        Filter $filter,
        int $skip = null,
        int $limit = null,
        OrderBy $orderBy = null): \Traversable
    {
        $this->assertHasCollection($collectionName);

        $filteredDocs = [];

        foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $filteredDocs[$docId] = ['doc' => $doc, 'docId' => $docId];
            }
        }

        $filteredDocs = \array_values($filteredDocs);

        if ($orderBy !== null) {
            $this->sort($filteredDocs, $orderBy);
        }

        if ($skip !== null) {
            $filteredDocs = \array_slice($filteredDocs, $skip, $limit);
        } elseif ($limit !== null) {
            $filteredDocs = \array_slice($filteredDocs, 0, $limit);
        }

        $docsMap = [];

        foreach ($filteredDocs as $docAndId) {
            $docsMap[$docAndId['docId']] = $this->transformToPartialDoc($docAndId['doc'], $partialSelect);
        }

        return new \ArrayIterator($docsMap);
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @return array
     */
    public function filterDocIds(
        string $collectionName,
        Filter $filter
    ): array {
        $this->assertHasCollection($collectionName);

        $docIds = [];
        foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $doc) {
            if ($filter->match($doc, (string)$docId)) {
                $docIds[] = $docId;
            }
        }

        return $docIds;
    }

    /**
     * @inheritDoc
     */
    public function countDocs(string $collectionName, Filter $filter) : int
    {
        $this->assertHasCollection($collectionName);

        $counter = 0;
        foreach ($this->inMemoryConnection['documents'][$collectionName] as $docId => $doc) {
            if ($filter->match($doc, $docId)) {
                $counter++;
            }
        }

        return $counter;
    }

    private function hasDoc(string $collectionName, string $docId): bool
    {
        if (! $this->hasCollection($collectionName)) {
            return false;
        }

        return \array_key_exists($docId, $this->inMemoryConnection['documents'][$collectionName]);
    }

    private function assertHasCollection(string $collectionName): void
    {
        if (! $this->hasCollection($collectionName)) {
            throw UnknownCollection::withName($collectionName);
        }
    }

    private function assertDocExists(string $collectionName, string $docId): void
    {
        $this->assertHasCollection($collectionName);

        if (! $this->hasDoc($collectionName, $docId)) {
            throw new RuntimeException("Doc with id $docId does not exist in collection $collectionName");
        }
    }

    private function assertUniqueConstraints(string $collectionName, string $docId, array $docOrSubset): void
    {
        $indices = $this->inMemoryConnection['documentIndices'][$collectionName];

        foreach ($indices as $index) {
            if($index instanceof FieldIndex) {
                $this->assertUniqueFieldConstraint($collectionName, $docId, $docOrSubset, $index);
            }

            if($index instanceof MultiFieldIndex) {
                $this->assertMultiFieldUniqueConstraint($collectionName, $docId, $docOrSubset, $index);
            }
        }
    }

    private function assertUniqueFieldConstraint(string $collectionName, string $docId, array $docOrSubset, FieldIndex $index, string $errMsg = null): void
    {
        if(!$index->unique()) {
            return;
        }

        $reader = new ArrayReader($docOrSubset);

        if(!$reader->pathExists($index->field())) {
            return;
        }

        $value = $reader->mixedValue($index->field());

        $check = new EqFilter($index->field(), $value);

        foreach ($this->inMemoryConnection['documents'][$collectionName] as $existingDocId => $existingDoc) {
            if (!$check->match($existingDoc, (string)$existingDocId)) {
                continue;
            }

            if ((string)$existingDocId === $docId) {
                continue;
            }

            throw new RuntimeException(
                $errMsg ?? "Unique constraint violation. Cannot insert or update document with id $docId, because a document with same value for field: {$index->field()} exists already!"
            );
        }

        return;
    }

    private function assertMultiFieldUniqueConstraint(string $collectionName, string $docId, array $docOrSubset, MultiFieldIndex $index, string $errMsg = null): void
    {
        if(!$index->unique()) {
            return;
        }

        if($this->hasDoc($collectionName, $docId)) {
            $effectedDoc = $this->getDoc($collectionName, $docId);
            $docOrSubset = $this->arrayReplace($effectedDoc, $docOrSubset);
        }

        $reader = new ArrayReader($docOrSubset);

        $checkList = [];
        $notExistingFieldsCheckList = [];
        $fieldNames = [];

        foreach ($index->fields() as $fieldIndex) {
            $fieldNames[] = $fieldIndex->field();
            if($reader->pathExists($fieldIndex->field())) {
                $checkList[] = new EqFilter($fieldIndex->field(), $reader->mixedValue($fieldIndex->field()));
            } else {
                $notExistingFieldsCheckList[] = new EqFilter($fieldIndex->field(), null);
            }
        }

        if(count($checkList) === 0) {
            return;
        }

        $checkList = array_merge($checkList, $notExistingFieldsCheckList);

        if(count($checkList) > 1) {
            $a = $checkList[0];
            $b = $checkList[1];
            $rest = array_slice($checkList, 2);
            if(!$rest) {
                $rest = [];
            }
            $checkList = new AndFilter($a, $b, ...$rest);
        } else {
            $checkList = $checkList[0];
        }

        foreach ($this->inMemoryConnection['documents'][$collectionName] as $existingDocId => $existingDoc) {
            if (!$checkList->match($existingDoc, (string)$existingDocId)) {
                continue;
            }

            if ((string)$existingDocId === $docId) {
                continue;
            }

            $fieldNamesStr = implode(", ", $fieldNames);
            throw new RuntimeException(
                $errMsg ?? "Unique constraint violation. Cannot insert or update document with id $docId, because a document with same values for fields: {$fieldNamesStr} exists already!"
            );
        }

        return;
    }

    private function sort(&$docs, OrderBy $orderBy)
    {
        $defaultCmp = function ($a, $b) {
            return ($a < $b) ? -1 : (($a > $b) ? 1 : 0);
        };

        $getField = function (array $doc, OrderBy $orderBy) {
            if ($orderBy instanceof Asc || $orderBy instanceof Desc) {
                $field = $orderBy->prop();

                return (new ArrayReader($doc['doc']))->mixedValue($field);
            }

            throw new \RuntimeException(\sprintf(
                'Unable to get field from doc: %s. Given OrderBy is neither an instance of %s nor %s',
                \json_encode($doc['doc']),
                Asc::class,
                Desc::class
            ));
        };

        $docCmp = null;
        $docCmp = function (array $docA, array $docB, OrderBy $orderBy) use (&$docCmp, $defaultCmp, $getField) {
            $orderByB = null;

            if ($orderBy instanceof AndOrder) {
                $orderByB = $orderBy->b();
                $orderBy = $orderBy->a();
            }

            $valA = $getField($docA, $orderBy);
            $valB = $getField($docB, $orderBy);

            if (\is_string($valA) && \is_string($valB)) {
                $orderResult = \strcasecmp($valA, $valB);
            } else {
                $orderResult = $defaultCmp($valA, $valB);
            }

            if ($orderResult === 0 && $orderByB) {
                $orderResult = $docCmp($docA, $docB, $orderByB);
            }

            if ($orderResult === 0) {
                return 0;
            }

            if ($orderBy instanceof Desc) {
                return $orderResult * -1;
            }

            return $orderResult;
        };

        \usort($docs, function (array $docA, array $docB) use ($orderBy, $docCmp) {
            return $docCmp($docA, $docB, $orderBy);
        });
    }

    /**
     * The internal array_replace_recursive function also replaces sequential arrays recursively. This method aims to
     * behave identical to array_replace_recursive but only when dealing with associative arrays. Sequential arrays
     * are handled as if they were scalar types instead.
     * 
     * @param array $array1
     * @param array $array2
     * @return array
     */
    private function arrayReplace(array $array1, array $array2): array
    {
        foreach ($array2 as $key2 => $value2) {
            $array1[$key2] = $value2;
        }

        return $array1;
    }

    private function transformToPartialDoc(array $doc, PartialSelect $partialSelect): array
    {
        $partialDoc = [];
        $reader = new ArrayReader($doc);

        foreach ($partialSelect->fieldAliasMap() as ['field' => $field, 'alias' => $alias]) {
            $value = $reader->mixedValue($field);

            if($alias === PartialSelect::MERGE_ALIAS) {
                if(null === $value) {
                    continue;
                }

                if(!is_array($value)) {
                    throw new RuntimeException('Merge not possible. $merge alias was specified for field: ' . $field . ' but field value is not an array: ' . json_encode($value));
                }

                foreach ($value as $k => $v) {
                    $partialDoc[$k] = $v;
                }

                continue;
            }

            $keys = explode('.', $alias);

            $ref = &$partialDoc;
            foreach ($keys as $i => $key) {
                if(!array_key_exists($key, $ref)) {
                    $ref[$key] = [];
                }
                $ref = &$ref[$key];
            }
            $ref = $value;
            unset($ref);
        }

        return $partialDoc;
    }
}
