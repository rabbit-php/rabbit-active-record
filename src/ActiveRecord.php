<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

use Throwable;
use Rabbit\Base\App;
use Rabbit\DB\Expression;
use Rabbit\DB\TableSchema;
use Rabbit\Base\Helper\Inflector;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\StaleObjectException;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\DB\Query;

class ActiveRecord extends BaseActiveRecord
{
    /**
     * The insert operation. This is mainly used when overriding [[transactions()]] to specify which operations are transactional.
     */
    const OP_INSERT = 0x01;
    /**
     * The update operation. This is mainly used when overriding [[transactions()]] to specify which operations are transactional.
     */
    const OP_UPDATE = 0x02;
    /**
     * The delete operation. This is mainly used when overriding [[transactions()]] to specify which operations are transactional.
     */
    const OP_DELETE = 0x04;
    /**
     * All three operations: insert, update, delete.
     * This is a shortcut of the expression: OP_INSERT | OP_UPDATE | OP_DELETE.
     */
    const OP_ALL = 0x07;

    protected string $tableName = '';

    /**
     * @return array
     */
    public function getRelations(): array
    {
        return [];
    }

    public function getScenes(): array
    {
        return [];
    }

    public function loadDefaultValues(bool $skipIfSet = true): self
    {
        foreach ($this->getTableSchema()->columns as $column) {
            if ($column->defaultValue !== null && (!$skipIfSet || $this->{$column->name} === null)) {
                $this->{$column->name} = $column->defaultValue;
            }
        }
        return $this;
    }

    public function findBySql(string $sql, array $params = []): ActiveQueryInterface
    {
        $query = $this->find();
        $query->sql = $sql;

        return $query->params($params);
    }

    protected function findByCondition(string|array $condition): ActiveQueryInterface
    {
        $query = $this->find();

        if (!ArrayHelper::isAssociative($condition)) {
            // query by primary key
            $primaryKey = $this->primaryKey();
            if (isset($primaryKey[0])) {
                $pk = $primaryKey[0];
                if (!empty($query->join) || !empty($query->joinWith)) {
                    $pk = $this->tableName() . '.' . $pk;
                }
                // if condition is scalar, search for a single primary key, if it is array, search for multiple primary key values
                $condition = [$pk => is_array($condition) ? array_values($condition) : $condition];
            } else {
                throw new InvalidConfigException('"' . get_called_class() . '" must have a primary key.');
            }
        } elseif (is_array($condition)) {
            $condition = $this->filterCondition($condition);
        }

        return $query->andWhere($condition);
    }

    protected function filterCondition(array $condition): array
    {
        $result = [];
        // valid column names are table column names or column names prefixed with table name
        $columnNames = $this->getTableSchema()->getColumnNames();
        $tableName = $this->tableName();
        $columnNames = array_merge($columnNames, array_map(fn ($columnName) => "$tableName.$columnName", $columnNames));
        foreach ($condition as $key => $value) {
            if (is_string($key) && !in_array($key, $columnNames, true)) {
                throw new InvalidArgumentException('Key "' . $key . '" is not a column name and can not be used as a filter');
            }
            $result[$key] = is_array($value) ? array_values($value) : $value;
        }

        return $result;
    }

    public function updateAll(array $attributes, string|array $condition = '', array $params = []): int
    {
        $command = $this->db->createCommand();
        $command->update($this->tableName(), $attributes, $condition, $params);

        return (int)$command->execute();
    }

    public function updateAllCounters(array $counters, string|array $condition = '', $params = []): int
    {
        foreach ($counters as $name => $value) {
            $counters[$name] = new Expression("[[$name]]+?", ["?" => $value]);
        }
        $command = $this->db->createCommand();
        $command->update($this->tableName(), $counters, $condition, $params);

        return (int)$command->execute();
    }

    public function deleteAll(string|array $condition = null, array $params = []): int
    {
        $command = $this->db->createCommand();
        $command->delete($this->tableName(), $condition, $params);

        return (int)$command->execute();
    }

    public function find(): ActiveQueryInterface
    {
        return create(ActiveQuery::class, ['db' => $this->db, 'modelClass' => get_called_class()], false);
    }

    public function tableName(): string
    {
        if ($this->tableName === '') {
            $this->tableName = '{{%' . Inflector::camel2id(StringHelper::basename(get_called_class()), '_') . '}}';
        }
        return $this->tableName;
    }

    public function getTableSchema(): TableSchema
    {
        $tableSchema = $this->db->getSchema()->getTableSchema($this->tableName());

        if ($tableSchema === null) {
            throw new InvalidConfigException('The table does not exist: ' . $this->tableName());
        }

        return $tableSchema;
    }

    public function primaryKey(): array
    {
        return $this->getTableSchema()->primaryKey;
    }

    public function attributes(): array
    {
        return array_keys($this->getTableSchema()->columns);
    }

    public function transactions(): array
    {
        return [];
    }

    public function insertByQuery(Query $query, bool $withUpdate = false): bool
    {
        return $this->db->transaction(function () use ($query, $withUpdate): bool {
            return (bool)$this->db->createCommand()->insert($this->tableName(), $query, $withUpdate)->execute();
        });
    }

    public function insert(bool $runValidation = true, array $attributes = null): bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            App::info('Model not inserted due to validation error.', 'db');
            return false;
        }

        if (!$this->isTransactional(self::OP_INSERT)) {
            return $this->insertInternal($attributes);
        }

        $transaction = $this->db->beginTransaction();
        try {
            $result = $this->insertInternal($attributes);
            if ($result === false) {
                $transaction->rollBack();
            } else {
                $transaction->commit();
            }

            return (bool)$result;
        } catch (Throwable $e) {
            $transaction->rollBack();
            throw $e;
        }
    }

    protected function insertInternal(array $attributes = null): bool
    {
        $values = $this->getDirtyAttributes($attributes);
        if (($primaryKeys = $this->db->schema->insert($this->tableName(), $values)) === false) {
            return false;
        }
        foreach ($primaryKeys as $name => $value) {
            $id = $this->getTableSchema()->columns[$name]->phpTypecast($value);
            $this->setAttribute($name, $id);
            $values[$name] = $id;
        }

        $this->setOldAttributes($values);
        return true;
    }

    public function update(bool $runValidation = true, array $attributeNames = null): int
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            App::info('Model not updated due to validation error.', 'db');
            return 0;
        }

        if (!$this->isTransactional(self::OP_UPDATE)) {
            return $this->updateInternal($attributeNames);
        }

        $transaction = $this->db->beginTransaction();
        try {
            $result = $this->updateInternal($attributeNames);
            if ($result === false) {
                $transaction->rollBack();
            } else {
                $transaction->commit();
            }

            return (int)$result;
        } catch (Throwable $e) {
            $transaction->rollBack();
            throw $e;
        }
    }

    public function delete(): int
    {
        if (!$this->isTransactional(self::OP_DELETE)) {
            return (int)$this->deleteInternal();
        }

        $transaction = $this->db->beginTransaction();
        try {
            $result = $this->deleteInternal();
            if ($result === false) {
                $transaction->rollBack();
            } else {
                $transaction->commit();
            }

            return $result;
        } catch (Throwable $e) {
            $transaction->rollBack();
            throw $e;
        }
    }

    protected function deleteInternal(): int
    {
        // we do not check the return value of deleteAll() because it's possible
        // the record is already deleted in the database and thus the method will return 0
        $condition = $this->getOldPrimaryKey();
        $lock = $this->optimisticLock();
        if ($lock !== null) {
            $condition[$lock] = $this->$lock;
        }
        $result = $this->deleteAll($condition);
        if ($lock !== null && !$result) {
            throw new StaleObjectException('The object being deleted is outdated.');
        }
        $this->setOldAttributes(null);

        return (int)$result;
    }

    public function equals(ActiveRecordInterface $record): bool
    {
        if ($this->isNewRecord || $record->isNewRecord) {
            return false;
        }

        return $this->tableName() === $record->tableName() && $this->getPrimaryKey() === $record->getPrimaryKey();
    }

    public function isTransactional(int $operation): bool
    {
        $scenario = $this->getScenario();
        $transactions = $this->transactions();

        return isset($transactions[$scenario]) && ($transactions[$scenario] & $operation);
    }
}
