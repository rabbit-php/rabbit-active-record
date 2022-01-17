<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

use Rabbit\Pool\ConnectionInterface;

/**
 * ActiveRecordInterface.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @author Carsten Brandt <mail@cebe.cc>
 * @since 2.0
 */
interface ActiveRecordInterface
{
    public function primaryKey(): array;

    public function attributes(): array;

    public function getAttribute(string $name);

    public function setAttribute(string $name, $value): void;

    public function hasAttribute(string $name): bool;

    public function getPrimaryKey(): ?array;

    public function getOldPrimaryKey(): ?array;

    public function isPrimaryKey(array $keys): bool;

    public function find(): ActiveQueryInterface;

    public function findOne(string|array $condition): array|null;

    public function findAll(string|array $condition): array;

    public function updateAll(array $attributes, string|array $condition = ''): int;

    public function deleteAll(string|array $condition = null): int;

    public function save(bool $runValidation = true, array $attributeNames = null): bool;

    public function insert(bool $runValidation = true, array $attributes = null): bool;

    public function update(bool $runValidation = true, array $attributeNames = null): int;

    public function delete(): int;

    public function getIsNewRecord(): bool;

    public function equals(ActiveRecordInterface $record): bool;

    public function getRelation(string $name, bool $throwException = true): ?ActiveQueryInterface;

    public function populateRelation(string $name, ?array $records): void;

    public function link(string $name, ActiveRecordInterface $model, array $extraColumns = []): void;

    public function unlink(string $name, ActiveRecordInterface $model, bool $delete = false): void;

    public function getDb(): ConnectionInterface;
}
