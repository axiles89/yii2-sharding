<?php
/**
 * HelperCoordinator.php
 *
 * @package axiles89\sharding
 * @date: 16.03.2016 16:53
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;

use yii\base\Exception;
use yii\base\InvalidParamException;
use yii\db\Query;

/**
 * Класс для получения значения ключей шарда из условия запроса
 * Class HelperCoordinator
 * @package axiles89\sharding
 */
class HelperCoordinator
{
    /**
     * Префикс параметров
     */
    const PARAM_PREFIX = ':qp';

    /**
     * @var
     */
    private static $instance;

    /**
     * @var array
     */
    protected $conditionBuilders = [
        'NOT' => 'buildNotCondition',
        'AND' => 'buildAndCondition',
        'OR' => 'buildAndCondition',
        'BETWEEN' => 'buildBetweenCondition',
        'NOT BETWEEN' => 'buildBetweenCondition',
        'IN' => 'buildInCondition',
        'NOT IN' => 'buildInCondition',
        'LIKE' => 'buildLikeCondition',
        'NOT LIKE' => 'buildLikeCondition',
        'OR LIKE' => 'buildLikeCondition',
        'OR NOT LIKE' => 'buildLikeCondition',
        'EXISTS' => 'buildExistsCondition',
        'NOT EXISTS' => 'buildExistsCondition',
    ];

    /**
     *
     */
    private function __construct()
    {
    }

    /**
     * @return mixed
     */
    public static function getInstance()
    {
        if (!isset(static::$instance)) {
            static::$instance = new self;
        }
        return static::$instance;
    }

    /**
     * @param $where
     * @param $params
     * @param $column
     * @return mixed
     */
    public function getData($where, $params, $column)
    {
        $result = $this->buildWhere($where, $params);

        $sql = strtr($result['where'], $result['params']);

        return ParserSql::getInstance()->setSql($sql)->setColumn($column)->execute();
    }

    /**
     * @param $condition
     * @param $params
     * @return array
     */
    public function buildWhere($condition, $params)
    {
        $where = $this->buildCondition($condition, $params);

        $result = [
            'params' => $params,
            'where' => $where
        ];

        return $result;
    }

    /**
     * Построитель условий (выбор нужного метода или по хешу, если условие задано как массив)
     * @param $condition
     * @param $params
     * @return string
     */
    public function buildCondition($condition, &$params)
    {
        if (!is_array($condition)) {
            return (string)$condition;
        } elseif (empty($condition)) {
            return '';
        }

        if (isset($condition[0])) {
            $operator = strtoupper($condition[0]);

            if (isset($this->conditionBuilders[$operator])) {
                $method = $this->conditionBuilders[$operator];
            } else {
                $method = 'buildSimpleCondition';
            }
            array_shift($condition);
            return $this->{$method($operator, $condition, $params)};
        } else {
            return $this->buildHashCondition($condition, $params);
        }
    }


    /**
     * Построитель по хешу (['id' => 123, 'parent_id' => [13,4,45]])
     * @param $condition
     * @param $params
     * @return string
     * @throws Exception
     */
    public function buildHashCondition($condition, &$params)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value)) {
                $parts[] = $this->buildInCondition('IN', [$column, $value], $params);
            } else {
                if ($value === null) {
                    $parts[] = "$column IS NULL";
                } elseif ($value instanceof Expression) {
                    $parts[] = "$column=" . $value->expression;
                    foreach ($value->params as $n => $v) {
                        $params[$n] = $v;
                    }
                } else {
                    $phName = self::PARAM_PREFIX . count($params);
                    $parts[] = "$column=$phName";
                    $params[$phName] = $value;
                }
            }
        }

        return count($parts) === 1 ? $parts[0] : '(' . implode(') AND (', $parts) . ')';
    }

    /**
     * Построитель запросов по условию типа ['>=', 'id', 3]
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws InvalidParamException
     */
    public function buildSimpleCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        list($column, $value) = $operands;

        if ($value === null) {
            return "$column $operator NULL";
        } elseif ($value instanceof Expression) {
            foreach ($value->params as $n => $v) {
                $params[$n] = $v;
            }
            return "$column $operator {$value->expression}";
        } elseif ($value instanceof Query) {
            return "";
        } else {
            $phName = self::PARAM_PREFIX . count($params);
            $params[$phName] = $value;
            return "$column $operator $phName";
        }
    }

    /**
     * Построитель условий and
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     */
    public function buildAndCondition($operator, $operands, &$params)
    {
        $parts = [];

        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand, $params);
            }
            if ($operand !== '') {
                $parts[] = $operand;
            }
        }

        if (!empty($parts)) {
            return '(' . implode(") $operator (", $parts) . ')';
        } else {
            return '';
        }
    }

    /**
     * Построитель not
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws InvalidParamException
     */
    public function buildNotCondition($operator, $operands, &$params)
    {
        if (count($operands) != 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }

        $operand = reset($operands);

        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        }

        if ($operand === '') {
            return '';
        }

        return "$operator ($operand)";
    }

    /**
     * Построитель between (['BETWEEN', 'id', 199, 202])
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws InvalidParamException
     */
    public function buildBetweenCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new InvalidParamException("Operator '$operator' requires three operands.");
        }

        list($column, $value1, $value2) = $operands;

        if ($value1 instanceof Expression) {
            foreach ($value1->params as $n => $v) {
                $params[$n] = $v;
            }
            $phName1 = $value1->expression;
        } else {
            $phName1 = self::PARAM_PREFIX . count($params);
            $params[$phName1] = $value1;
        }
        if ($value2 instanceof Expression) {
            foreach ($value2->params as $n => $v) {
                $params[$n] = $v;
            }
            $phName2 = $value2->expression;
        } else {
            $phName2 = self::PARAM_PREFIX . count($params);
            $params[$phName2] = $value2;
        }

        return "$column $operator $phName1 AND $phName2";
    }

    /**
     * Построитель in (['in', 'id', [100,101]])
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws Exception
     */
    public function buildInCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $values) = $operands;

        if ($values === [] || $column === []) {
            return $operator === 'IN' ? '0=1' : '';
        }

        if ($values instanceof Query) {
            return '';
        }

        $values = (array)$values;

        //todo in по 2 колонкам. Проверить целесообразность
        if (count($column) > 1) {
            return '';
        }

        if (is_array($column)) {
            $column = reset($column);
        }

        foreach ($values as $i => $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                $values[$i] = 'NULL';
            } elseif ($value instanceof Expression) {
                $values[$i] = $value->expression;
                foreach ($value->params as $n => $v) {
                    $params[$n] = $v;
                }
            } else {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = $value;
                $values[$i] = $phName;
            }
        }

        if (count($values) > 1) {
            return "$column $operator (" . implode(', ', $values) . ')';
        } else {
            $operator = $operator === 'IN' ? '=' : '<>';
            return $column . $operator . reset($values);
        }
    }

    /**
     * Построитель like и not like (['NOT LIKE', 'title', ['dima', 'fdfdf']])
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws InvalidParamException
     */
    public function buildLikeCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        $escape = isset($operands[2]) ? $operands[2] : ['%' => '\%', '_' => '\_', '\\' => '\\\\'];
        unset($operands[2]);

        // Поддержка not и объединений or и and
        if (!preg_match('/^(AND |OR |)(((NOT |))I?LIKE)/', $operator, $matches)) {
            throw new InvalidParamException("Invalid operator '$operator'.");
        }

        $andor = ' ' . (!empty($matches[1]) ? $matches[1] : 'AND ');
        $not = !empty($matches[3]);
        $operator = $matches[2];

        list($column, $values) = $operands;

        if (!is_array($values)) {
            $values = [$values];
        }

        if (empty($values)) {
            return $not ? '' : '0=1';
        }

        $parts = [];
        foreach ($values as $value) {
            if ($value instanceof Expression) {
                foreach ($value->params as $n => $v) {
                    $params[$n] = $v;
                }
                $phName = $value->expression;
            } else {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = empty($escape) ? $value : ('%' . strtr($value, $escape) . '%');
            }
            $parts[] = "$column $operator $phName";
        }

        return implode($andor, $parts);
    }


    /**
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     */
    public function buildExistsCondition($operator, $operands, &$params)
    {
        return '';
    }
}