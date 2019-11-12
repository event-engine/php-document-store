<?php
declare(strict_types=1);

namespace EventEngineTest\DocumentStore;

use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Filter\EqFilter;
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
    public function it_ensures_unique_constraints_for_multiple_fields()
    {
        $multiFieldIndex = MultiFieldIndex::forFields(['some.prop', 'some.other.prop'], true);

        $this->store->addCollection('test', $multiFieldIndex);

        $this->store->addDoc('test', '1', ['some' => ['prop' => 'foo', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '2', ['some' => ['prop' => 'bar', 'other' => ['prop' => 'bat']]]);
        $this->store->addDoc('test', '3', ['some' => ['prop' => 'bar']]);

        $this->expectExceptionMessageRegExp('/^Unique constraint violation/');
        $this->store->updateDoc('test', '2', ['some' => ['prop' => 'foo']]);
    }
}
