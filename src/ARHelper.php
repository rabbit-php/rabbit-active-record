<?php

declare(strict_types=1);

namespace Rabbit\ActiveRecord;

use Rabbit\DB\Query;
use Rabbit\DB\DBHelper;
use Rabbit\DB\Exception;
use Rabbit\DB\Expression;
use Rabbit\DB\JsonExpression;
use Rabbit\Base\Helper\JsonHelper;
use Rabbit\Base\Core\UserException;
use Rabbit\Base\Helper\ArrayHelper;

/**
 * Class ARHelper
 * @package Rabbit\ActiveRecord
 */
class ARHelper
{
    public static function saveSeveral(BaseActiveRecord $model, array $arrayColumns, bool $withUpdate = true): int
    {
        if (empty($arrayColumns)) {
            return 0;
        }
        $conn = $model::getDb();
        $sql = '';
        $params = [];
        $i = 0;
        if (!ArrayHelper::isIndexed($arrayColumns)) {
            $arrayColumns = [$arrayColumns];
        }
        $keys = $model::primaryKey();

        $schema = $conn->getSchema();
        $tableSchema = $schema->getTableSchema($model::tableName());
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];

        foreach ($arrayColumns as $item) {
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
        return $conn->createCommand($sql, $params)->execute();
    }

    public static function updateSeveral(BaseActiveRecord $model, array $arrayColumns, array $when = null): int
    {
        $updateSql = '';
        if (!ArrayHelper::isIndexed($arrayColumns)) {
            $arrayColumns = [$arrayColumns];
        }
        $firstRow = current($arrayColumns);
        $updateColumn = array_keys($firstRow);
        $conn = $model::getDb();
        $schema = $conn->getSchema();
        $tableSchema = $schema->getTableSchema($model::tableName());
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];
        $referenceColumns = $when ?? $model::primaryKey();
        foreach ($referenceColumns as $column) {
            unset($updateColumn[array_search($column, $updateColumn)]);
        }
        $updateSql = "UPDATE " .  $model::tableName() . " SET ";
        $sets = [];
        $bindings = [];
        $wheres = [];
        foreach ($updateColumn as $uColumn) {
            $setSql = "`" . $uColumn . "` = CASE ";
            foreach ($arrayColumns as $data) {
                $refSql = '';
                foreach ($referenceColumns as $i => $ref) {
                    $refSql .= " `" . $ref . "` = ? and";
                    $value = isset($columnSchemas[$ref]) ? $columnSchemas[$ref]->dbTypecast($data[$ref]) : $data[$ref];
                    if ($value instanceof Expression) {
                        foreach ($value->params as  $v) {
                            $wheres[$i][] = $bindings[] = $v;
                        }
                    } elseif ($value instanceof JsonExpression) {
                        $wheres[$i][] = $bindings[] = is_string($value->getValue()) ? $value->getValue() : JsonHelper::encode($value);
                    } else {
                        $wheres[$i][] = $bindings[] = $value;
                    }
                }
                $refSql = rtrim($refSql, 'and');
                $setSql .= "WHEN $refSql THEN ? ";
                $bindings[] = $data[$uColumn];
            }
            $setSql .= "ELSE `" . $uColumn . "` END ";
            $sets[] = $setSql;
        }
        $updateSql .= implode(', ', $sets);
        $updateSql = rtrim($updateSql, ", ") . " where (" . implode(',', $referenceColumns) . ") in (";

        for ($i = 0; $i < count(reset($wheres)); $i++) {
            $pal = [];
            $tmp = array_column($wheres, $i);
            foreach ($tmp as $v) {
                $pal[] = '?';
                $bindings[] = $v;
            }
            $updateSql .= '(' . implode(',', $pal) . '),';
        }
        $updateSql = rtrim($updateSql, ',');
        $updateSql .= ')';
        return $conn->createCommand($updateSql, $bindings)->execute();
    }

    public static function deleteSeveral(BaseActiveRecord $model, array $arrayColumns): int
    {
        if (empty($arrayColumns)) {
            return 0;
        }
        $result = false;
        $keys = $model::primaryKey();
        $condition = [];
        if (ArrayHelper::isAssociative($arrayColumns)) {
            $arrayColumns = [$arrayColumns];
        }
        foreach ($arrayColumns as $item) {
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

    public static function create(BaseActiveRecord $model, array $body, bool $batch = true): array
    {
        if (!ArrayHelper::isIndexed($body)) {
            $body = [$body];
        }
        if (!$batch) {
            $result = [];
            foreach ($body as $params) {
                $res = self::createModel(clone $model, $params);
                $result[] = $res;
            }
        } else {
            $result = self::saveSeveral($model, $body, false);
        }
        return is_array($result) ? $result : [$result];
    }

    public static function update(BaseActiveRecord $model, array $body, bool $onlyUpdate = false, array $when = null, bool $batch = true): array
    {
        if (isset($body['edit']) && $body['edit']) {
            $result = $model->updateAll($body['edit'], DBHelper::Search((new Query()),  ArrayHelper::getValue($body, 'where', []))->where);
            if ($result === false) {
                throw new Exception('Failed to update the object for unknown reason.');
            }
        } else {
            if (!ArrayHelper::isIndexed($body)) {
                $body = [$body];
            }
            if ($onlyUpdate) {
                $result = self::updateSeveral($model, $when);
            } elseif (!$batch) {
                $result = [];
                $exists = self::findExists($model, $body);
                foreach ($body as $params) {
                    $res = self::updateModel(clone $model, $params, self::checkExist($params, $exists, $model::primaryKey()));
                    $result[] = $res;
                }
            } else {
                $result = self::saveSeveral($model, $body);
            }
        }
        return is_array($result) ? $result : [$result];
    }

    public static function delete(BaseActiveRecord $model, array $body): int
    {
        if (ArrayHelper::isIndexed($body)) {
            $result = self::deleteSeveral($model, $body);
        } else {
            $result = $model::deleteAll(DBHelper::Search((new Query()), $body)->where);
        }
        if ($result === false) {
            throw new Exception('Failed to delete the object for unknown reason.');
        }
        return $result;
    }

    private static function createModel(BaseActiveRecord $model, array $body): array
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
                    $res = self::createModel($child_model, $params);
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

    public static function updateModel(BaseActiveRecord $model, array $body, ?array $exist): array
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
                            $result[$key][] = self::updateModel(
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
