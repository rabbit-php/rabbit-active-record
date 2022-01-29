<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\ActiveRecord;

use ArrayAccess;
use ArrayIterator;
use IteratorAggregate;
use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\Inflector;
use Rabbit\Model\Model as ModelModel;
use ReflectionClass;
use Respect\Validation\Validatable;

class Model extends ModelModel implements IteratorAggregate, ArrayAccess, Arrayable
{
    use ArrayableTrait;

    const SCENARIO_DEFAULT = 'default';

    private array $_errors = [];

    private string $_scenario = self::SCENARIO_DEFAULT;

    public function rules(): array
    {
        return [];
    }

    public function formName(): string
    {
        $reflector = new ReflectionClass($this);
        if ($reflector->isAnonymous()) {
            throw new InvalidConfigException('The "formName()" method should be explicitly defined for anonymous models');
        }
        return $reflector->getShortName();
    }

    public function attributes(): array
    {
        $class = new ReflectionClass($this);
        $names = [];
        foreach ($class->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            if (!$property->isStatic()) {
                $names[] = $property->getName();
            }
        }

        return $names;
    }

    public function attributeLabels(): array
    {
        return [];
    }

    public function attributeHints(): array
    {
        return [];
    }

    public function validate(array $attributeNames = null, bool $throwAble = true, bool $firstReturn = false, bool $clearErrors = true): bool
    {
        if ($clearErrors) {
            $this->clearErrors();
        }

        foreach ($this->rules() as $rule) {
            list($properties, $validator) = $rule;
            foreach ($properties as $property) {
                if ($validator instanceof Validatable) {
                    if (!$validator->validate($this->$property)) {
                        $this->addError($property, $validator->reportError($property)->getMessage());
                    }
                } elseif (is_callable($validator)) {
                    $this->$property = call_user_func($validator, $this->$property);
                } else {
                    $this->$property === null && $this->$property = $validator;
                }
            }
        }

        return !$this->hasErrors();
    }

    public function getAttributeLabel(string $attribute): string
    {
        $labels = $this->attributeLabels();
        return $labels[$attribute] ?? $this->generateAttributeLabel($attribute);
    }

    public function getAttributeHint(string $attribute): string
    {
        $hints = $this->attributeHints();
        return $hints[$attribute] ?? '';
    }

    public function getErrorSummary(bool $showAllErrors): array
    {
        $lines = [];
        $errors = $showAllErrors ? $this->getErrors() : $this->getFirstErrors();
        foreach ($errors as $es) {
            $lines = [...(array)$es, ...$lines];
        }
        return $lines;
    }

    public function generateAttributeLabel(string $name): string
    {
        return Inflector::camel2words($name, true);
    }

    public function getAttributes(array $names = null, array $except = []): array
    {
        $values = [];
        if ($names === null) {
            $names = $this->attributes();
        }
        foreach ($names as $name) {
            $values[$name] = $this->$name;
        }
        foreach ($except as $name) {
            unset($values[$name]);
        }

        return $values;
    }

    public function setAttributes(array $values, bool $safeOnly = true): void
    {
        if (is_array($values)) {
            $attributes = array_flip($this->attributes());
            foreach ($values as $name => $value) {
                if (isset($attributes[$name])) {
                    $this->$name = $value;
                } elseif ($safeOnly) {
                    $this->onUnsafeAttribute($name, $value);
                }
            }
        }
    }

    public function onUnsafeAttribute(string $name, $value): void
    {
        if (config('debug')) {
            App::debug("Failed to set unsafe attribute '$name' in '" . get_class($this) . "'.");
        }
    }

    public function getScenario(): string
    {
        return $this->_scenario;
    }

    public function load(array $data, string $formName = ''): bool
    {
        $scope = $formName ?? $this->formName();
        if ($scope === '' && !empty($data)) {
            $this->setAttributes($data);

            return true;
        } elseif (isset($data[$scope])) {
            $this->setAttributes($data[$scope]);

            return true;
        }

        return false;
    }

    public static function loadMultiple(array $models, array $data, string $formName = null): bool
    {
        if ($formName === null) {
            /* @var $first Model|false */
            $first = reset($models);
            if ($first === false) {
                return false;
            }
            $formName = $first->formName();
        }

        $success = false;
        foreach ($models as $i => $model) {
            /* @var $model Model */
            if ($formName == '') {
                if (!empty($data[$i]) && $model->load($data[$i], '')) {
                    $success = true;
                }
            } elseif (!empty($data[$formName][$i]) && $model->load($data[$formName][$i], '')) {
                $success = true;
            }
        }

        return $success;
    }

    public static function validateMultiple(array $models, array $attributeNames = null, bool $clearErrors = true): bool
    {
        $valid = true;
        /* @var $model Model */
        foreach ($models as $model) {
            $valid = $model->validate($attributeNames, $clearErrors) && $valid;
        }

        return $valid;
    }

    public function fields(): array
    {
        $fields = $this->attributes();

        return array_combine($fields, $fields);
    }

    public function getIterator(): ArrayIterator
    {
        $attributes = $this->getAttributes();
        return new ArrayIterator($attributes);
    }

    public function offsetExists(mixed $offset): bool
    {
        return isset($this->$offset);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->$offset;
    }

    public function offsetSet(mixed $offset, mixed $item): void
    {
        $this->$offset = $item;
    }

    public function offsetUnset(mixed $offset): void
    {
        $this->$offset = null;
    }
}
