<?php
/**
 * This file is part of event-engine/php-document-sore.
 * (c) 2018-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore\OrderBy;

final class DocId implements OrderBy
{
    private $direction;

    public function __construct($direction = 'ASC') {
        $this->direction = $direction;
    }

    public static function fromArray(array $data): OrderBy
    {
        return new self($data['direction'] ?? OrderBy::ASC);
    }

    public function toArray(): array
    {
        return ['direction' => $this->direction];
    }

    public function direction()
    {
        return $this->direction;
    }
}
