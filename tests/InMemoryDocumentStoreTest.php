<?php
declare(strict_types=1);

namespace EventEngineTest\DocumentStore;

use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Filter\AndFilter;
use EventEngine\DocumentStore\Filter\AnyFilter;
use EventEngine\DocumentStore\Filter\EqFilter;
use EventEngine\DocumentStore\Filter\GtFilter;
use EventEngine\DocumentStore\Filter\LtFilter;
use EventEngine\DocumentStore\Filter\OrFilter;
use EventEngine\DocumentStore\InMemoryDocumentStore;
use EventEngine\DocumentStore\MultiFieldIndex;
use EventEngine\Persistence\InMemoryConnection;
use PHPUnit\Framework\TestCase;

final class InMemoryDocumentStoreTest extends TestCase
{
    /**
     * @var InMemoryDocumentStore
     */
    private $store;

    protected function setUp()
    {
        parent::setUp();
        $this->store = new InMemoryDocumentStore(new InMemoryConnection());
    }

    /**
     * @test
     */
    public function it_adds_collection()
    {
        $this->store->addCollection('test');
        $this->assertTrue($this->store->hasCollection('test'));
    }

    /**
     * @test
     */
    public function it_adds_collection_with_unique_index()
    {
        $this->store->addCollection('test', FieldIndex::namedIndexForField('unique_prop_idx', 'some.prop', FieldIndex::SORT_ASC, true));
        $this->assertTrue($this->store->hasCollectionIndex('test', 'unique_prop_idx'));
    }

    /**
     * @test
     */
    public function it_adds_and_updates_a_doc()
    {
        $this->store->addCollection('test');

        $doc = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    'nested' => 42
                ]
            ],
            'baz' => 'bat',
        ];

        $this->store->addDoc('test', '1', $doc);

        $persistedDoc = $this->store->getDoc('test', '1');

        $this->assertEquals($doc, $persistedDoc);

        $doc['baz'] = 'changed val';

        $this->store->updateDoc('test', '1', $doc);

        $filter = new EqFilter('baz', 'changed val');

        $filteredDocs = $this->store->filterDocs('test', $filter);

        $this->assertCount(1, $filteredDocs);
    }

    /**
     * @test
     */
    public function it_updates_a_subset_of_a_doc()
    {
        $this->store->addCollection('test');

        $doc = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    'nested' => 42
                ]
            ],
            'baz' => 'bat',
        ];

        $this->store->addDoc('test', '1', $doc);

        $this->store->updateDoc('test', '1', [
            'some' => [
                'prop' => 'fuzz'
            ]
        ]);

        $filteredDocs = iterator_to_array($this->store->filterDocs('test', new EqFilter('some.prop', 'fuzz')));
        $this->assertEquals(42, $filteredDocs[0]['some']['other']['nested']);
    }

    /**
     * @test
     */
    public function it_retrieves_doc_ids_by_filter()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', 'a', ['number' => 10]);
        $this->store->addDoc('test', 'b', ['number' => 20]);
        $this->store->addDoc('test', 'c', ['number' => 30]);

        $result = $this->store->filterDocIds('test', new OrFilter(
            new GtFilter('number', 21),
            new LtFilter('number', 19)
        ));

        $this->assertEquals(['a', 'c'], $result);
    }

    /**
     * @test
     */
    public function it_ensures_unique_constraints_for_a_field()
    {
        $this->store->addCollection('test', FieldIndex::namedIndexForField('unique_prop_idx', 'some.prop', FieldIndex::SORT_ASC, true));

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo']]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'foo']]);
    }

    /**
     * @test
     */
    public function it_ensures_unique_constraints_for_a_field_for_update()
    {
        $this->store->addCollection('test', FieldIndex::namedIndexForField('unique_prop_idx', 'some.prop', FieldIndex::SORT_ASC, true));

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo']]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $this->store->updateDoc('test', '2', ['some' => ['prop' => 'foo']]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function it_allows_updating_with_unique_constraints_for_a_field()
    {
        $this->store->addCollection('test', FieldIndex::namedIndexForField('unique_prop_idx', 'some.prop', FieldIndex::SORT_ASC, true));

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo']]);

        $this->store->updateDoc('test', '1', ['some' => ['prop' => 'foo', 'new' => 'prop']]);
    }

    /**
     * @test
     */
    public function it_ensures_unique_constraints_for_multiple_fields()
    {
        $multiFieldIndex = MultiFieldIndex::forFields(['some.prop', 'some.other.prop'], true);

        $this->store->addCollection('test', $multiFieldIndex);

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $this->store->addDoc('test', '4', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
    }

    /**
     * @test
     */
    public function it_ensures_unique_constraints_for_multiple_fields_for_update()
    {
        $multiFieldIndex = MultiFieldIndex::forFields(['some.prop', 'some.other.prop'], true);

        $this->store->addCollection('test', $multiFieldIndex);

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $this->store->updateDoc('test', '2', ['some' => ['prop' => 'foo']]);
    }

    /**
     * @test
     * @doesNotPerformAssertions
     */
    public function it_allows_updating_with_unique_constraints_for_multiple_fields()
    {
        $multiFieldIndex = MultiFieldIndex::forFields(['some.prop', 'some.other.prop'], true);

        $this->store->addCollection('test', $multiFieldIndex);

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);

        $this->store->updateDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat'], 'new' => 'prop']]);
    }

    /**
     * @test
     */
    public function it_blocks_adding_a_unique_index_if_it_conflicts_with_existing_docs()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $uniqueIndex = FieldIndex::forField('some.prop', FieldIndex::SORT_ASC, true);
        $this->store->addCollectionIndex('test', $uniqueIndex);
    }

    /**
     * @test
     */
    public function it_does_not_block_adding_a_unique_index_if_no_conflict_exists()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'baz']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $uniqueIndex = FieldIndex::namedIndexForField('test_idx', 'some.other.prop', FieldIndex::SORT_ASC, true);
        $this->store->addCollectionIndex('test', $uniqueIndex);
        $this->assertTrue($this->store->hasCollectionIndex('test', 'test_idx'));
    }

    /**
     * @test
     */
    public function it_blocks_adding_a_unique_multi_field_index_if_it_conflicts_with_existing_docs()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $uniqueIndex = MultiFieldIndex::forFields(['some.prop', 'some.other.prop'], true);
        $this->store->addCollectionIndex('test', $uniqueIndex);
    }

    /**
     * @test
     */
    public function it_does_not_block_adding_a_unique_multi_field_index_if_no_conflict_exists()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $uniqueIndex = MultiFieldIndex::namedIndexForFields('test_idx', ['some.prop', 'some.other.prop'], true);
        $this->store->addCollectionIndex('test', $uniqueIndex);
        $this->assertTrue($this->store->hasCollectionIndex('test', 'test_idx'));
    }

    /**
     * @test
     */
    public function it_updates_many()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->store->updateMany(
            'test',
            new EqFilter('some.other.prop', 'bat'),
            ['some' => ['prop' => 'fuzz']]
        );

        $filteredDocs = iterator_to_array($this->store->filterDocs('test', new EqFilter('some.prop', 'fuzz')));

        $this->assertCount(2, $filteredDocs);
        $this->assertEquals('fuzz', $filteredDocs[0]['some']['prop']);
        $this->assertEquals('fuzz', $filteredDocs[1]['some']['prop']);
    }

    /**
     * @test
     */
    public function it_deletes_many()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->store->deleteMany(
            'test',
            new EqFilter('some.other.prop', 'bat')
        );

        $filteredDocs = iterator_to_array($this->store->filterDocs('test', new AnyFilter()));
        
        $this->assertCount(1, $filteredDocs);
        $this->assertEquals(['some' => ['prop' => 'bar']], $filteredDocs[0]);
    }

    /**
     * @test
     */
    public function it_does_not_update_numeric_arrays_recursively()
    {
        $this->store->addCollection('test');

        $this->store->addDoc('test', 'doc', [
            'a' => ['a' => 10, 'b' => 20],
            'b' => [10, 20, 30],
            'c' => [],
            'd' => [false, true],
            'e' => ['a' => 'b'],
            'f' => [10, 20],
            'g' => ['x' => 10, 'y' => 20],
        ]);

        $this->store->updateDoc('test', 'doc', [
            'a' => ['b' => 21, 'c' => 30],
            'b' => [10, 30],
            'c' => [true],
            'd' => [],
            'e' => [],
            'f' => ['x' => 10, 'y' => 20],
            'g' => [30, 40]
        ]);

        $this->assertEquals(
            [
                'a' => ['a' => 10, 'b' => 21, 'c' => 30],
                'b' => [10, 30],
                'c' => [true],
                'd' => [],
                'e' => ['a' => 'b'],
                'f' => [10, 20, 'x' => 10, 'y' => 20],
                'g' => ['x' => 10, 'y' => 20, 30, 40]
            ],
            $this->store->getDoc('test', 'doc')
        );
    }
}
