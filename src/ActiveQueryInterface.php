<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

use Rabbit\DB\QueryInterface;

interface ActiveQueryInterface extends QueryInterface
{
    public function one(): ?array;

    public function indexBy(string|callable $column): QueryInterface;

    public function with(): self;

    public function via(string $relationName, callable $callable = null): self;

    public function findFor(string $name, ActiveRecordInterface $model);
}
