<?php
declare(strict_types=1);

namespace EventEngine\DocumentStore;

use EventEngine\DocumentStore\Exception\RuntimeException;
use function get_class;
use function gettype;
use function is_int;
use function is_object;
use function is_string;

/**
 * Class PartialSelect
 *
 * You can pass a list of fields to PartialSelect which should be included in a partial document loaded from the
 * document store. The list can contain a mapping from alias to field or only the field.
 *
 * You can access nested fields using dot notation. Same applies for aliases.
 *
 * @example
 *
 * $partialSelect = new PartialSelect([
 *      'topLevelField',
 *      'aliasName' => 'anotherField',
 *      'nested.alias' => 'nested.field',
 * ]);
 *
 * Resulting partial document:
 *
 * [
 *   'topLevelField' => 'some value',
 *   'aliasName' => 'another value',
 *   'nested' => [
 *     'alias' => 'nested value'
 *   ]
 * ]
 *
 * In case a field does not exist in the document, it is set to NULL in the resulting partial document.
 *
 * A special "$merge" alias allows to merge all nested fields from the original field into top level partial document.
 *
 * @example
 *
 * Original document:
 *
 * [
 *   'topLevelField' => 'some value',
 *   'nested' => [
 *     'subField' => 'nested value'
 *   ]
 * ]
 *
 * $partialSelect = new PartialSelect([
 *      '$merge' => 'nested',
 *      'topLevelField'
 * ]);
 *
 * Resulting partial document:
 *
 * [
 *   'subField' => 'nested value'
 *   'topLevelField' => 'some value',
 * ]
 *
 * @package EventEngine\DocumentStore
 */
final class PartialSelect
{
    public const MERGE_ALIAS = '$merge';

    /**
     * @var array<array-key, array{field: string, alias: string}>
     */
    private $fields;

    public function __construct(array $fieldList)
    {
        $this->populateFieldList($fieldList);
    }

    public function withField(string $field): PartialSelect
    {
        $clone = clone $this;
        $clone->fields[] = [
            'field' => $field,
            'alias' => $field,
        ];
        return $clone;
    }

    public function withFieldAlias(string $field, string $alias): PartialSelect
    {
        $clone = clone $this;
        $clone->fields[] = [
            'field' => $field,
            'alias' => $alias,
        ];
        return $clone;
    }

    public function withMergedField(string $field): PartialSelect
    {
        $clone = clone $this;
        $clone->fields[] = [
            'field' => $field,
            'alias' => self::MERGE_ALIAS,
        ];
        return $clone;
    }

    /**
     * @return array<array-key, array{field: string, alias: string}>
     */
    public function fieldAliasMap(): array
    {
        return $this->fields;
    }

    private function populateFieldList(array $fieldList): void
    {
        foreach ($fieldList as $aliasOrIndex => $field) {
            if(is_int($aliasOrIndex)) {
                $aliasOrIndex = $field;
            }

            if(!is_string($aliasOrIndex)) {
                throw new RuntimeException("Expected field definition to be a string. Got " . (is_object($aliasOrIndex) ? get_class($aliasOrIndex) : gettype($aliasOrIndex)));
            }

            if(!is_string($field)) {
                throw new RuntimeException("Expected field definition to be a string. Got " . (is_object($field) ? get_class($field) : gettype($field)));
            }

            $this->fields[] = [
                'field' => $field,
                'alias' => $aliasOrIndex
            ];
        }
    }
}
