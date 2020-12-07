<?php

declare(strict_types=1);

namespace Rabbit\ActiveRecord;

use Throwable;
use Rabbit\DB\Query;
use Rabbit\DB\DBHelper;
use Rabbit\DB\Exception;
use ReflectionException;
use Rabbit\DB\Expression;
use Rabbit\DB\JsonExpression;
use Rabbit\Base\Helper\JsonHelper;
use Rabbit\Base\Core\UserException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\StaleObjectException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\InvalidArgumentException;

/**
 * Class ARHelper
 * @package Rabbit\ActiveRecord
 */
class ARHelper
{
    /**
     * @param $model
     * @param array $array_columns
     * @return int
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public static function saveSeveral(BaseActiveRecord $model, array $array_columns, bool $withUpdate = true): int
    {
        if (empty($array_columns)) {
            return 0;
        }
        $conn = $model::getDb();
        $sql = '';
        $params = [];
        $i = 0;
        if (!ArrayHelper::isIndexed($array_columns)) {
            $array_columns = [$array_columns];
        }
        $keys = $model::primaryKey();

        $schema = $conn->getSchema();
        $tableSchema = $schema->getTableSchema($model::tableName());
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];

        foreach ($array_columns as $item) {
            $table = clone $model;
            $table->load($item, '');
            //关联模型
            foreach ($table->getRelations() as $child => [$key, $val, $delete]) {
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if (!isset($item[$key][0])) {
                        $item[$key] = [$item[$key]];
                    }
                    foreach ($val as $c_attr => $p_attr) {
                        foreach ($item[$key] as &$param) {
                            $param[$c_attr] = $table->{$p_attr};
                        }
                    }
                    if ($delete) {
                        if (is_array($delete)) {
                            self::delete($child_model, $delete);
                        } elseif (is_callable($delete)) {
                            call_user_func($delete, $child_model, $item[$key]);
                        }
                    }
                    if (self::saveSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            $names = array();
            $placeholders = array();
            $table->isNewRecord = false;
            if (!$table->validate()) {
                throw new UserException(implode(PHP_EOL, $table->getFirstErrors()));
            }
            $tableArray = $table->toArray();
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key]) && (!isset($item[$key]) || $tableArray[$key] === null)) {
                        $tableArray[$key] = $item[$key];
                    }
                }
            }
            foreach ($tableArray as $name => $value) {
                if (!$i) {
                    $names[] = $conn->quoteColumnName($name);
                    $withUpdate && ($updates[] = $conn->quoteColumnName($name) . "=values(" . $conn->quoteColumnName($name) . ")");
                }
                $value = isset($columnSchemas[$name]) ? $columnSchemas[$name]->dbTypecast($value) : $value;
                if ($value instanceof Expression) {
                    $placeholders[] = $value->expression;
                    foreach ($value->params as $n => $v) {
                        $params[$n] = $v;
                    }
                } elseif ($value instanceof JsonExpression) {
                    $placeholders[] = '?';
                    $params[] = is_string($value->getValue()) ? $value->getValue() : JsonHelper::encode($value);
                } else {
                    $placeholders[] = '?';
                    $params[] = $value;
                }
            }
            if (!$i) {
                $sql = 'INSERT INTO ' . $conn->quoteTableName($table::tableName())
                    . ' (' . implode(', ', $names) . ') VALUES ('
                    . implode(', ', $placeholders) . ')';
            } else {
                $sql .= ',(' . implode(', ', $placeholders) . ')';
            }
            $i++;
        }
        $withUpdate && $updates && $sql .= " on duplicate key update " . implode(', ', $updates);
        $result = $conn->createCommand($sql, $params)->execute();
        if (is_array($result)) {
            return end($result);
        }
        return $result;
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $array_columns
     * @return int
     * @throws NotSupportedException
     * @throws InvalidConfigException
     * @throws ReflectionException
     */
    public static function deleteSeveral(BaseActiveRecord $model, array $array_columns): int
    {
        if (empty($array_columns)) {
            return 0;
        }
        $result = false;
        $keys = $model::primaryKey();
        $condition = [];
        if (ArrayHelper::isAssociative($array_columns)) {
            $array_columns = [$array_columns];
        }
        foreach ($array_columns as $item) {
            $model->load($item, '');
            $model->isNewRecord = false;
            foreach ($model->getRelations() as $child => [$key]) {
                if (isset($item[$key])) {
                    $child_model = new $child();
                    if (self::deleteSeveral($child_model, $item[$key]) === false) {
                        return 0;
                    }
                }
            }
            if ($keys) {
                foreach ($keys as $key) {
                    if (isset($item[$key])) {
                        $condition[$key][] = $item[$key];
                    }
                }
            }
        }
        if ($condition) {
            $result = $model::deleteAll($condition);
        }
        return (int)$result;
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $batch
     * @return array
     * @throws Exception
     * @throws NotSupportedException
     * @throws Throwable
     */
    public static function create(BaseActiveRecord $model, array $body, bool $batch = true): array
    {
        if (!ArrayHelper::isIndexed($body)) {
            $body = [$body];
        }
        if (!$batch) {
            $result = [];
            foreach ($body as $params) {
                $res = self::createSeveral(clone $model, $params);
                $result[] = $res;
            }
        } else {
            $result = self::saveSeveral($model, $body, false);
        }
        return is_array($result) ? $result : [$result];
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $useOrm
     * @param bool $batch
     * @return array
     * @throws Exception
     * @throws NotSupportedException
     * @throws Throwable
     */
    public static function update(BaseActiveRecord $model, array $body, bool $useOrm = false, bool $batch = true): array
    {
        if (isset($body['edit']) && $body['edit']) {
            $result = $useOrm ? $model::getDb()->createCommandExt(['update', $body['edit'], ArrayHelper::getValue($body, 'where', [])])->execute() :
                $model->updateAll($body['edit'], DBHelper::Search((new Query()),  ArrayHelper::getValue($body, 'where', []))->where);
            if ($result === false) {
                throw new Exception('Failed to update the object for unknown reason.');
            }
        } else {
            if (!ArrayHelper::isIndexed($body)) {
                $body = [$body];
            }
            if (!$batch) {
                $result = [];
                $exists = self::findExists($model, $body);
                foreach ($body as $params) {
                    $res = self::updateSeveral(clone $model, $params, self::checkExist($model, $params, $exists));
                    $result[] = $res;
                }
            } else {
                $result = self::saveSeveral($model, $body);
            }
        }
        return is_array($result) ? $result : [$result];
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param bool $useOrm
     * @return int
     * @throws Exception
     * @throws InvalidConfigException
     * @throws NotSupportedException
     * @throws ReflectionException
     */
    public static function delete(BaseActiveRecord $model, array $body, bool $useOrm = false): int
    {
        if (ArrayHelper::isIndexed($body)) {
            $result = self::deleteSeveral($model, $body);
        } else {
            $result = $useOrm ? $model::getDb()->createCommandExt(['delete', [$model::tableName(), $body]])->execute() :
                $model::deleteAll(DBHelper::Search((new Query()), $body)->where);
        }
        if ($result === false) {
            throw new Exception('Failed to delete the object for unknown reason.');
        }
        return $result;
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     * @throws InvalidConfigException
     * @throws NotSupportedException
     * @throws ReflectionException
     * @throws StaleObjectException
     */
    private static function createSeveral(BaseActiveRecord $model, array $body): array
    {
        $model->load($body, '');
        if ($model->save()) {
            $result = self::insertRealation($model, $body);
        } elseif (!$model->hasErrors()) {
            throw new Exception('Failed to create the object for unknown reason.');
        } else {
            throw new Exception(implode(PHP_EOL, $model->getFirstErrors()));
        }
        return $result;
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     * @throws InvalidConfigException
     * @throws NotSupportedException
     * @throws ReflectionException
     * @throws StaleObjectException
     */
    private static function insertRealation(BaseActiveRecord $model, array $body): array
    {
        $result = [];
        //关联模型
        foreach ($model->getRelations() as $child => [$key, $val]) {
            if (isset($body[$key])) {
                if (ArrayHelper::isAssociative($body[$key])) {
                    $body[$key] = [$body[$key]];
                }
                foreach ($body[$key] as $params) {
                    if ($val) {
                        foreach ($val as $c_attr => $p_attr) {
                            $params[$c_attr] = $model->{$p_attr};
                        }
                    }
                    $child_model = new $child();
                    $res = self::createSeveral($child_model, $params);
                    $result[$key][] = $res;
                }
            }
        }
        $res = $model->toArray();
        foreach ($result as $key => $val) {
            $res[$key] = $val;
        }
        return $res;
    }

    /**
     * @param $model
     * @param array $body
     * @param array $condition
     * @return array
     */
    private static function findExists(BaseActiveRecord $model, array $body, array $condition = []): array
    {
        $keys = $model::primaryKey();
        if (ArrayHelper::isAssociative($body)) {
            $body = [$body];
        }
        foreach ($keys as $key) {
            foreach ($body as $item) {
                if (array_key_exists($key, $item)) {
                    $condition[$key][] = $item[$key];
                }
            }
        }
        if ($condition !== [] && count($keys) === count($condition)) {
            return $model::find()->where($condition)->asArray()->all();
        }
        return [];
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @param array|null $exist
     * @return array
     * @throws Exception
     * @throws NotSupportedException
     * @throws Throwable
     */
    public static function updateSeveral(BaseActiveRecord $model, array $body, ?array $exist): array
    {
        $model->setOldAttributes($exist);
        $model->load($body, '');
        if ($model->save() === false && !$model->hasErrors()) {
            throw new Exception('Failed to update the object for unknown reason.');
        } else {
            $result = self::saveRealation($model, $body);
        }
        return $result;
    }

    /**
     * @param BaseActiveRecord $model
     * @param array $body
     * @return array
     * @throws Exception
     * @throws NotSupportedException
     * @throws Throwable
     */
    protected static function saveRealation(BaseActiveRecord $model, array $body): array
    {
        $result = [];
        //关联模型
        foreach ($model->getRelations() as $child => [$key, $val, $delete]) {
            if (isset($body[$key])) {
                $child_model = new $child();
                if (isset($params['edit']) && $params['edit']) {
                    $result[$key] = self::update($child_model, $params);
                } else {
                    if (ArrayHelper::isAssociative($body[$key])) {
                        $params = [$body[$key]];
                    } else {
                        $params = $body[$key];
                    }
                    if ($delete) {
                        if (is_array($delete)) {
                            self::delete($child_model, $delete);
                        } elseif (is_callable($delete)) {
                            call_user_func($delete, $child_model, $params);
                        }
                    }
                    $exists = self::findExists($child_model, $params);
                    foreach ($params as $param) {
                        if ($val) {
                            $child_model = new $child();
                            foreach ($val as $c_attr => $p_attr) {
                                $param[$c_attr] = $model->{$p_attr};
                            }
                            $result[$key][] = self::updateSeveral(
                                $child_model,
                                $param,
                                self::checkExist(
                                    $param,
                                    $exists,
                                    $child_model::primaryKey()
                                )
                            );
                        }
                    }
                }
            }
        }
        $res = $model->toArray();
        foreach ($result as $key => $val) {
            $res[$key] = $val;
        }
        return $res;
    }

    /**
     * @param array $body
     * @param array $exists
     * @param array $conditions
     * @return array|null
     */
    public static function checkExist(array $body, array $exists, array $conditions = []): ?array
    {
        if (!$exists) {
            return null;
        }
        $existCount = 0;
        foreach ($exists as $exist) {
            if (empty($conditions))
                foreach ($conditions as $key) {
                    if (isset($body[$key]) && $body[$key] == $exist[$key]) {
                        $existCount++;
                    }
                    if ($existCount === count($conditions)) {
                        return $exist;
                    }
                }
        }
        return null;
    }
}
