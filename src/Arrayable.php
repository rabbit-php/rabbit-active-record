<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

interface Arrayable extends \Rabbit\Base\Contract\ArrayAble
{
    public function fields(): array;

    public function extraFields(): array;

    public function toArray(array $fields = [], array $expand = [], bool $recursive = true): array;
}
