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

final class MultiFieldIndex implements Index
{
    /**
     * @var FieldIndex[]
     */
    private $fields;

    /**
     * @var bool
     */
    private $unique;

    /**
     * @var string|null
     */
    private $name;

    public static function forFields(array $fieldNames, bool $unique = false): self
    {
        return self::fromArray([
            'fields' => $fieldNames,
            'unique' => $unique,
        ]);
    }

    public static function namedIndexForFields(string $idxName, array $fieldNames, bool $unique = false): self
    {
        return self::fromArray([
            'name' => $idxName,
            'fields' => $fieldNames,
            'unique' => $unique,
        ]);
    }

    public static function fromArray(array $data): Index
    {
        $fields = \array_map(function (string $field): FieldIndex {
            return FieldIndex::forFieldInMultiFieldIndex($field);
        }, $data['fields'] ?? []);

        return new self(
            $data['unique'] ?? false,
            $data['name'] ?? null,
            ...$fields
        );
    }

    private function __construct(bool $unique,string $name = null, FieldIndex ...$fields)
    {
        if (\count($fields) <= 1) {
            throw new \InvalidArgumentException('MultiFieldIndex should contain at least two fields');
        }

        $this->fields = $fields;
        $this->unique = $unique;
        $this->name = $name;
    }

    /**
     * @return FieldIndex[]
     */
    public function fields(): array
    {
        return $this->fields;
    }

    /**
     * @return bool
     */
    public function unique(): bool
    {
        return $this->unique;
    }

    public function name(): ?string
    {
        return $this->name;
    }

    public function toArray(): array
    {
        return [
            'fields' => \array_map(function (FieldIndex $field): string {
                return $field->field();
            }, $this->fields),
            'unique' => $this->unique,
            'name' => $this->name,
        ];
    }

    public function equals($other): bool
    {
        if (! $other instanceof self) {
            return false;
        }

        return $this->toArray() === $other->toArray();
    }

    public function __toString(): string
    {
        return \json_encode($this->toArray());
    }
}
