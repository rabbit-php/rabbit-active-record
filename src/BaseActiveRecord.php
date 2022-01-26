<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

use InvalidArgumentException;
use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Exception\UnknownMethodException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\StaleObjectException;
use Rabbit\Pool\ConnectionInterface;

abstract class BaseActiveRecord extends Model implements ActiveRecordInterface
{
    protected array $_attributes = [];

    protected ?array $_oldAttributes = null;

    protected array $_related = [];

    protected array $_relationsDependencies = [];

    protected bool $autoAlias = true;

    protected ConnectionInterface $db;

    public function __construct(string|ConnectionInterface $db = null)
    {
        if (is_string($db)) {
            $this->db = getDI('db')->get($db);
        } elseif ($db === null) {
            $this->db = $this->getDb();
        } else {
            $this->db = $db;
        }
    }

    public function findOne(string|array $condition): ?array
    {
        return $this->findByCondition($condition)->one();
    }

    public function findAll(string|array $condition): array
    {
        return $this->findByCondition($condition)->all();
    }

    protected function findByCondition(string|array $condition): ActiveQueryInterface
    {
        $query = $this->find();

        if (!is_array($condition)) {
            $condition = [$condition];
        }

        if (ArrayHelper::isIndexed($condition)) {
            // query by primary key
            $primaryKey = $this->primaryKey();
            if (isset($primaryKey[0])) {
                // if condition is scalar, search for a single primary key, if it is array, search for multiple primary key values
                $condition = [$primaryKey[0] => is_array($condition) ? array_values($condition) : $condition];
            } else {
                throw new InvalidConfigException('"' . get_called_class() . '" must have a primary key.');
            }
        }

        return $query->andWhere($condition);
    }

    public function updateAll(array $attributes, string|array $condition = ''): int
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported.');
    }

    public function updateAllCounters(array $counters, string|array $condition = ''): int
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported.');
    }

    public function deleteAll(string|array $condition = null): int
    {
        throw new NotSupportedException(__METHOD__ . ' is not supported.');
    }

    public function optimisticLock(): ?string
    {
        return null;
    }

    /**
     * {@inheritdoc}
     */
    public function canGetProperty(string $name, bool $checkVars = true, bool $checkBehaviors = true): bool
    {
        if (parent::canGetProperty($name, $checkVars, $checkBehaviors)) {
            return true;
        }

        try {
            return $this->hasAttribute($name);
        } catch (\Exception $e) {
            // `hasAttribute()` may fail on base/abstract classes in case automatic attribute list fetching used
            return false;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function canSetProperty(string $name, bool $checkVars = true, bool $checkBehaviors = true): bool
    {
        if (parent::canSetProperty($name, $checkVars, $checkBehaviors)) {
            return true;
        }

        try {
            return $this->hasAttribute($name);
        } catch (\Exception $e) {
            // `hasAttribute()` may fail on base/abstract classes in case automatic attribute list fetching used
            return false;
        }
    }

    public function __get($name)
    {
        if (isset($this->_attributes[$name]) || array_key_exists($name, $this->_attributes)) {
            return $this->_attributes[$name];
        }

        if ($this->hasAttribute($name)) {
            return null;
        }

        if (isset($this->_related[$name]) || array_key_exists($name, $this->_related)) {
            return $this->_related[$name];
        }
        $value = parent::__get($name);
        if ($value instanceof ActiveQueryInterface) {
            $this->setRelationDependencies($name, $value);
            return $this->_related[$name] = $value->findFor($name, $this);
        }

        return $value;
    }

    public function __set($name, $value)
    {
        if ($this->hasAttribute($name)) {
            if (
                !empty($this->_relationsDependencies[$name])
                && (!array_key_exists($name, $this->_attributes) || $this->_attributes[$name] !== $value)
            ) {
                $this->resetDependentRelations($name);
            }
            $this->_attributes[$name] = $value;
        } else {
            parent::__set($name, $value);
        }
    }

    public function __isset($name)
    {
        try {
            return $this->__get($name) !== null;
        } catch (\Throwable $t) {
            return false;
        }
    }

    public function __unset($name)
    {
        if ($this->hasAttribute($name)) {
            unset($this->_attributes[$name]);
            if (!empty($this->_relationsDependencies[$name])) {
                $this->resetDependentRelations($name);
            }
        } elseif (array_key_exists($name, $this->_related)) {
            unset($this->_related[$name]);
        } elseif ($this->getRelation($name, false) === null) {
            parent::__unset($name);
        }
    }

    public function hasOne(string $class, array $link): ActiveQueryInterface
    {
        return $this->createRelationQuery($class, $link, false);
    }

    public function hasMany(string $class, array $link): ActiveQueryInterface
    {
        return $this->createRelationQuery($class, $link, true);
    }

    protected function createRelationQuery(string $class, array $link, bool $multiple): ActiveQueryInterface
    {
        /* @var $class ActiveRecordInterface */
        /* @var $query ActiveQuery */
        $query = create($class)->find();
        $query->primaryModel = $this;
        $query->link = $link;
        $query->multiple = $multiple;
        return $query;
    }

    public function populateRelation(string $name, ?array $records): void
    {
        foreach ($this->_relationsDependencies as &$relationNames) {
            unset($relationNames[$name]);
        }

        $this->_related[$name] = $records;
    }

    public function isRelationPopulated(string $name): bool
    {
        return array_key_exists($name, $this->_related);
    }

    public function getRelatedRecords(): array
    {
        return $this->_related;
    }

    public function hasAttribute(string $name): bool
    {
        return isset($this->_attributes[$name]) || in_array($name, $this->attributes(), true);
    }

    public function getAttribute(string $name)
    {
        return isset($this->_attributes[$name]) ? $this->_attributes[$name] : null;
    }

    public function setAttribute(string $name, $value): void
    {
        if ($this->hasAttribute($name)) {
            if (
                !empty($this->_relationsDependencies[$name])
                && (!array_key_exists($name, $this->_attributes) || $this->_attributes[$name] !== $value)
            ) {
                $this->resetDependentRelations($name);
            }
            $this->_attributes[$name] = $value;
        } else {
            throw new InvalidArgumentException(get_class($this) . ' has no attribute named "' . $name . '".');
        }
    }

    public function getOldAttributes(): array
    {
        return $this->_oldAttributes === null ? [] : $this->_oldAttributes;
    }

    public function setOldAttributes(?array $values): void
    {
        $this->_oldAttributes = $values;
    }

    public function getOldAttribute(string $name)
    {
        return isset($this->_oldAttributes[$name]) ? $this->_oldAttributes[$name] : null;
    }

    public function setOldAttribute(string $name, $value): void
    {
        if (isset($this->_oldAttributes[$name]) || $this->hasAttribute($name)) {
            $this->_oldAttributes[$name] = $value;
        } else {
            throw new InvalidArgumentException(get_class($this) . ' has no attribute named "' . $name . '".');
        }
    }

    public function markAttributeDirty(string $name): void
    {
        unset($this->_oldAttributes[$name]);
    }

    public function isAttributeChanged(string $name, bool $identical = true): bool
    {
        if (isset($this->_attributes[$name], $this->_oldAttributes[$name])) {
            if ($identical) {
                return $this->_attributes[$name] !== $this->_oldAttributes[$name];
            }

            return $this->_attributes[$name] != $this->_oldAttributes[$name];
        }

        return isset($this->_attributes[$name]) || isset($this->_oldAttributes[$name]);
    }

    public function getDirtyAttributes(?array $names = null): array
    {
        if ($names === null) {
            $names = $this->attributes();
        }
        $names = array_flip($names);
        $attributes = [];
        if ($this->_oldAttributes === null) {
            foreach ($this->_attributes as $name => $value) {
                if (isset($names[$name])) {
                    $attributes[$name] = $value;
                }
            }
        } else {
            foreach ($this->_attributes as $name => $value) {
                if (isset($names[$name]) && (!array_key_exists(
                    $name,
                    $this->_oldAttributes
                ) || $value !== $this->_oldAttributes[$name])) {
                    $attributes[$name] = $value;
                }
            }
        }

        return $attributes;
    }

    public function save(bool $runValidation = true, array $attributeNames = null): bool
    {
        if ($this->getIsNewRecord()) {
            return $this->insert($runValidation, $attributeNames);
        }

        return $this->update($runValidation, $attributeNames) !== 0;
    }

    public function update(bool $runValidation = true, array $attributeNames = null): int
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            return 0;
        }

        return $this->updateInternal($attributeNames);
    }

    public function updateAttributes(array $attributes): int
    {
        $attrs = [];
        foreach ($attributes as $name => $value) {
            if (is_int($name)) {
                $attrs[] = $value;
            } else {
                $this->$name = $value;
                $attrs[] = $name;
            }
        }

        $values = $this->getDirtyAttributes($attrs);
        if (empty($values) || $this->getIsNewRecord()) {
            return 0;
        }

        $rows = $this->updateAll($values, $this->getOldPrimaryKey());

        foreach ($values as $name => $value) {
            $this->_oldAttributes[$name] = $this->_attributes[$name];
        }

        return (int)$rows;
    }

    protected function updateInternal(array $attributes = null): int
    {
        $values = $this->getDirtyAttributes($attributes);
        if (empty($values)) {
            return 0;
        }
        $condition = $this->getOldPrimaryKey();
        $lock = $this->optimisticLock();
        if ($lock !== null) {
            $values[$lock] = $this->$lock + 1;
            $condition[$lock] = $this->$lock;
        }
        // We do not check the return value of updateAll() because it's possible
        // that the UPDATE statement doesn't change anything and thus returns 0.
        $rows = $this->updateAll($values, $condition);

        if ($lock !== null && !$rows) {
            throw new StaleObjectException('The object being updated is outdated.');
        }

        if (isset($values[$lock])) {
            $this->$lock = $values[$lock];
        }

        $changedAttributes = [];
        foreach ($values as $name => $value) {
            $changedAttributes[$name] = isset($this->_oldAttributes[$name]) ? $this->_oldAttributes[$name] : null;
            $this->_oldAttributes[$name] = $value;
        }

        return $rows;
    }

    public function updateCounters(array $counters): bool
    {
        if ($this->updateAllCounters($counters, $this->getOldPrimaryKey()) > 0) {
            foreach ($counters as $name => $value) {
                if (!isset($this->_attributes[$name])) {
                    $this->_attributes[$name] = $value;
                } else {
                    $this->_attributes[$name] += $value;
                }
                $this->_oldAttributes[$name] = $this->_attributes[$name];
            }

            return true;
        }

        return false;
    }

    public function delete(): int
    {
        $result = 0;
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
        $this->_oldAttributes = null;

        return $result;
    }

    public function getIsNewRecord(): bool
    {
        return $this->_oldAttributes === null;
    }

    public function setIsNewRecord(bool $value): void
    {
        $this->_oldAttributes = $value ? null : $this->_attributes;
    }

    public function equals(ActiveRecordInterface $record): bool
    {
        if ($this->getIsNewRecord() || $record->getIsNewRecord()) {
            return false;
        }

        return get_class($this) === get_class($record) && $this->getPrimaryKey() === $record->getPrimaryKey();
    }

    public function getPrimaryKey(): ?array
    {
        $keys = $this->primaryKey();
        $values = [];
        foreach ($keys as $name) {
            $values[$name] = isset($this->_attributes[$name]) ? $this->_attributes[$name] : null;
        }

        return $values;
    }

    public function getOldPrimaryKey(): ?array
    {
        $keys = $this->primaryKey();
        if (empty($keys)) {
            throw new Exception(get_class($this) . ' does not have a primary key. You should either define a primary key for the corresponding table or override the primaryKey() method.');
        }

        $values = [];
        foreach ($keys as $name) {
            $values[$name] = isset($this->_oldAttributes[$name]) ? $this->_oldAttributes[$name] : null;
        }

        return $values;
    }

    /**
     * @return static
     */
    public static function instantiate(): self
    {
        return new static();
    }

    public function offsetExists(mixed $offset): bool
    {
        return $this->__isset($offset);
    }

    public function getRelation(string $name, bool $throwException = true): ?ActiveQueryInterface
    {
        $getter = 'get' . $name;
        try {
            // the relation could be defined in a behavior
            $relation = $this->$getter();
            $this->autoAlias && $relation->alias($name);
        } catch (UnknownMethodException $e) {
            if ($throwException) {
                throw new InvalidArgumentException(get_class($this) . ' has no relation named "' . $name . '".', 0, $e);
            }

            return null;
        }
        if (!$relation instanceof ActiveQueryInterface) {
            if ($throwException) {
                throw new InvalidArgumentException(get_class($this) . ' has no relation named "' . $name . '".');
            }

            return null;
        }

        if (method_exists($this, $getter)) {
            // relation name is case sensitive, trying to validate it when the relation is defined within this class
            $method = new \ReflectionMethod($this, $getter);
            $realName = lcfirst(substr($method->getName(), 3));
            if ($realName !== $name) {
                if ($throwException) {
                    throw new InvalidArgumentException('Relation names are case sensitive. ' . get_class($this) . " has a relation named \"$realName\" instead of \"$name\".");
                }

                return null;
            }
        }

        return $relation;
    }

    public function link(string $name, ActiveRecordInterface $model, array $extraColumns = []): void
    {
        $relation = $this->getRelation($name);

        if ($relation->via !== null) {
            if ($this->getIsNewRecord() || $model->getIsNewRecord()) {
                throw new InvalidCallException('Unable to link models: the models being linked cannot be newly created.');
            }
            if (is_array($relation->via)) {
                /* @var $viaRelation ActiveQuery */
                [$viaName, $viaRelation] = $relation->via;
                $viaClass = $viaRelation->modelClass;
                // unset $viaName so that it can be reloaded to reflect the change
                unset($this->_related[$viaName]);
            } else {
                $viaRelation = $relation->via;
                $viaTable = reset($relation->via->from);
            }
            $columns = [];
            foreach ($viaRelation->link as $a => $b) {
                $columns[$a] = $this->$b;
            }
            foreach ($relation->link as $a => $b) {
                $columns[$b] = $model->$a;
            }
            foreach ($extraColumns as $k => $v) {
                $columns[$k] = $v;
            }
            if (is_array($relation->via)) {
                /* @var $viaClass ActiveRecordInterface */
                /* @var $record ActiveRecordInterface */
                $record = create($viaClass, ['db' => $this->db], false);
                foreach ($columns as $column => $value) {
                    $record->$column = $value;
                }
                $record->insert(false);
            } else {
                /* @var $viaTable string */
                $this->db->createCommand()
                    ->insert($viaTable, $columns)->execute();
            }
        } else {
            $p1 = $model->isPrimaryKey(array_keys($relation->link));
            $p2 = $this->isPrimaryKey(array_values($relation->link));
            if ($p1 && $p2) {
                if ($this->getIsNewRecord() && $model->getIsNewRecord()) {
                    throw new InvalidCallException('Unable to link models: at most one model can be newly created.');
                } elseif ($this->getIsNewRecord()) {
                    $this->bindModels(array_flip($relation->link), $this, $model);
                } else {
                    $this->bindModels($relation->link, $model, $this);
                }
            } elseif ($p1) {
                $this->bindModels(array_flip($relation->link), $this, $model);
            } elseif ($p2) {
                $this->bindModels($relation->link, $model, $this);
            } else {
                throw new InvalidCallException('Unable to link models: the link defining the relation does not involve any primary key.');
            }
        }

        // update lazily loaded related objects
        if (!$relation->multiple) {
            $this->_related[$name] = $model;
        } elseif (isset($this->_related[$name])) {
            if ($relation->indexBy !== null) {
                if ($relation->indexBy instanceof \Closure) {
                    $index = call_user_func($relation->indexBy, $model);
                } else {
                    $index = $model->{$relation->indexBy};
                }
                $this->_related[$name][$index] = $model;
            } else {
                $this->_related[$name][] = $model;
            }
        }
    }

    public function unlink(string $name, ActiveRecordInterface $model, bool $delete = false): void
    {
        $relation = $this->getRelation($name);

        if ($relation->via !== null) {
            if (is_array($relation->via)) {
                /* @var $viaRelation ActiveQuery */
                [$viaName, $viaRelation] = $relation->via;
                $viaClass = $viaRelation->modelClass;
                unset($this->_related[$viaName]);
            } else {
                $viaRelation = $relation->via;
                $viaTable = reset($relation->via->from);
            }
            $columns = [];
            foreach ($viaRelation->link as $a => $b) {
                $columns[$a] = $this->$b;
            }
            foreach ($relation->link as $a => $b) {
                $columns[$b] = $model->$a;
            }
            $nulls = [];
            foreach (array_keys($columns) as $a) {
                $nulls[$a] = null;
            }
            if (is_array($relation->via)) {
                /* @var $viaClass ActiveRecordInterface */
                if ($delete) {
                    $viaClass->deleteAll($columns);
                } else {
                    $viaClass->updateAll($nulls, $columns);
                }
            } else {
                /* @var $viaTable string */
                /* @var $command Command */
                $command = $this->db->createCommand();
                if ($delete) {
                    $command->delete($viaTable, $columns)->execute();
                } else {
                    $command->update($viaTable, $nulls, $columns)->execute();
                }
            }
        } else {
            $p1 = $model->isPrimaryKey(array_keys($relation->link));
            $p2 = $this->isPrimaryKey(array_values($relation->link));
            if ($p2) {
                if ($delete) {
                    $model->delete();
                } else {
                    foreach ($relation->link as $a => $b) {
                        $model->$a = null;
                    }
                    $model->save(false);
                }
            } elseif ($p1) {
                foreach ($relation->link as $a => $b) {
                    if (is_array($this->$b)) { // relation via array valued attribute
                        if (($key = array_search($model->$a, $this->$b, false)) !== false) {
                            $values = $this->$b;
                            unset($values[$key]);
                            $this->$b = array_values($values);
                        }
                    } else {
                        $this->$b = null;
                    }
                }
                $delete ? $this->delete() : $this->save(false);
            } else {
                throw new InvalidCallException('Unable to unlink models: the link does not involve any primary key.');
            }
        }

        if (!$relation->multiple) {
            unset($this->_related[$name]);
        } elseif (isset($this->_related[$name])) {
            /* @var $b ActiveRecordInterface */
            foreach ($this->_related[$name] as $a => $b) {
                if ($model->getPrimaryKey() === $b->getPrimaryKey()) {
                    unset($this->_related[$name][$a]);
                }
            }
        }
    }

    public function unlinkAll(string $name, bool $delete = false): void
    {
        $relation = $this->getRelation($name);

        if ($relation->via !== null) {
            if (is_array($relation->via)) {
                /* @var $viaRelation ActiveQuery */
                [$viaName, $viaRelation] = $relation->via;
                $viaClass = $viaRelation->modelClass;
                unset($this->_related[$viaName]);
            } else {
                $viaRelation = $relation->via;
                $viaTable = reset($relation->via->from);
            }
            $condition = [];
            $nulls = [];
            foreach ($viaRelation->link as $a => $b) {
                $nulls[$a] = null;
                $condition[$a] = $this->$b;
            }
            if (!empty($viaRelation->where)) {
                $condition = ['and', $condition, $viaRelation->where];
            }
            if (!empty($viaRelation->on)) {
                $condition = ['and', $condition, $viaRelation->on];
            }
            if (is_array($relation->via)) {
                /* @var $viaClass ActiveRecordInterface */
                if ($delete) {
                    $viaClass->deleteAll($condition);
                } else {
                    $viaClass->updateAll($nulls, $condition);
                }
            } else {
                /* @var $viaTable string */
                /* @var $command Command */
                $command = $this->db->createCommand();
                if ($delete) {
                    $command->delete($viaTable, $condition)->execute();
                } else {
                    $command->update($viaTable, $nulls, $condition)->execute();
                }
            }
        } else {
            /* @var $relatedModel ActiveRecordInterface */
            $relatedModel = $relation->modelClass;
            if (!$delete && count($relation->link) === 1 && is_array($this->{$b = reset($relation->link)})) {
                // relation via array valued attribute
                $this->$b = [];
                $this->save(false);
            } else {
                $nulls = [];
                $condition = [];
                foreach ($relation->link as $a => $b) {
                    $nulls[$a] = null;
                    $condition[$a] = $this->$b;
                }
                if (!empty($relation->where)) {
                    $condition = ['and', $condition, $relation->where];
                }
                if (!empty($relation->on)) {
                    $condition = ['and', $condition, $relation->on];
                }
                if ($delete) {
                    $relatedModel->deleteAll($condition);
                } else {
                    $relatedModel->updateAll($nulls, $condition);
                }
            }
        }

        unset($this->_related[$name]);
    }

    private function bindModels(array $link, ActiveRecordInterface $foreignModel, ActiveRecordInterface $primaryModel): void
    {
        foreach ($link as $fk => $pk) {
            $value = $primaryModel->$pk;
            if ($value === null) {
                throw new InvalidCallException('Unable to link models: the primary key of ' . get_class($primaryModel) . ' is null.');
            }
            if (is_array($foreignModel->$fk)) { // relation via array valued attribute
                $foreignModel->$fk = [...$foreignModel->$fk, $value];
            } else {
                $foreignModel->$fk = $value;
            }
        }
        $foreignModel->save(false);
    }

    public function isPrimaryKey(array $keys): bool
    {
        $pks = $this->primaryKey();
        if (count($keys) === count($pks)) {
            return count(array_intersect($keys, $pks)) === count($pks);
        }

        return false;
    }

    public function getAttributeLabel(string $attribute): string
    {
        $labels = $this->attributeLabels();
        if (isset($labels[$attribute])) {
            return $labels[$attribute];
        } elseif (strpos($attribute, '.')) {
            $attributeParts = explode('.', $attribute);
            $neededAttribute = array_pop($attributeParts);

            $relatedModel = $this;
            foreach ($attributeParts as $relationName) {
                if ($relatedModel->isRelationPopulated($relationName) && $relatedModel->$relationName instanceof self) {
                    $relatedModel = $relatedModel->$relationName;
                } else {
                    try {
                        $relation = $relatedModel->getRelation($relationName);
                    } catch (InvalidArgumentException $e) {
                        return $this->generateAttributeLabel($attribute);
                    }
                    /* @var $modelClass ActiveRecordInterface */
                    $modelClass = $relation->modelClass;
                    $relatedModel = new $modelClass($this->db);
                }
            }

            $labels = $relatedModel->attributeLabels();
            if (isset($labels[$neededAttribute])) {
                return $labels[$neededAttribute];
            }
        }

        return $this->generateAttributeLabel($attribute);
    }

    public function getAttributeHint(string $attribute): string
    {
        $hints = $this->attributeHints();
        if (isset($hints[$attribute])) {
            return $hints[$attribute];
        } elseif (strpos($attribute, '.')) {
            $attributeParts = explode('.', $attribute);
            $neededAttribute = array_pop($attributeParts);

            $relatedModel = $this;
            foreach ($attributeParts as $relationName) {
                if ($relatedModel->isRelationPopulated($relationName) && $relatedModel->$relationName instanceof self) {
                    $relatedModel = $relatedModel->$relationName;
                } else {
                    try {
                        $relation = $relatedModel->getRelation($relationName);
                    } catch (InvalidArgumentException $e) {
                        return '';
                    }
                    /* @var $modelClass ActiveRecordInterface */
                    $modelClass = $relation->modelClass;
                    $relatedModel = new $modelClass($this->db);
                }
            }

            $hints = $relatedModel->attributeHints();
            if (isset($hints[$neededAttribute])) {
                return $hints[$neededAttribute];
            }
        }

        return '';
    }

    public function fields(): array
    {
        $fields = array_keys($this->_attributes);

        return array_combine($fields, $fields);
    }

    public function extraFields(): array
    {
        $fields = array_keys($this->getRelatedRecords());

        return array_combine($fields, $fields);
    }

    public function offsetUnset(mixed $offset): void
    {
        if (property_exists($this, $offset)) {
            $this->$offset = null;
        } else {
            unset($this->$offset);
        }
    }

    private function resetDependentRelations(string $attribute): void
    {
        foreach ($this->_relationsDependencies[$attribute] as $relation) {
            unset($this->_related[$relation]);
        }
        unset($this->_relationsDependencies[$attribute]);
    }

    private function setRelationDependencies(string $name, ActiveQueryInterface $relation): void
    {
        if (empty($relation->via) && $relation->link) {
            foreach ($relation->link as $attribute) {
                $this->_relationsDependencies[$attribute][$name] = $name;
            }
        } elseif ($relation->via instanceof ActiveQueryInterface) {
            $this->setRelationDependencies($name, $relation->via);
        } elseif (is_array($relation->via)) {
            [, $viaQuery] = $relation->via;
            $this->setRelationDependencies($name, $viaQuery);
        }
    }

    public function getDb(): ConnectionInterface
    {
        return $this->db ??= getDI('db')->get();
    }
}
