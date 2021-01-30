<?php
/**
 * This file is part of event-engine/php-document-store.
 * (c) 2018-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore;

interface Index
{
    public const SORT_ASC = 1;
    public const SORT_DESC = -1;

    public function toArray();

    public static function fromArray(array $data): Index;
}
