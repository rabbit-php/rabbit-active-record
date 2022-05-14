<?php

declare(strict_types=1);

namespace Rabbit\ActiveRecord;

use Rabbit\Pool\ConnectionInterface;

class BaseRecord extends ActiveRecord
{
    public function __construct(string $tableName, string|ConnectionInterface $dbName)
    {
        $this->tableName = $tableName;
        $this->db = is_string($dbName) ? service('db')->get($dbName) : $dbName;
    }

    public static function build(string $table, string|ConnectionInterface $db): BaseActiveRecord
    {
        return new static($table, $db);
    }
}
