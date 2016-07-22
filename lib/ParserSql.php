<?php
/**
 * ParserSql.php
 *
 * @package axiles89\sharding
 * @date: 22.03.2016 22:51
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;

use yii\base\InvalidParamException;

/**
 * Class ParserSql
 * @package axiles89\sharding
 */
class ParserSql
{
    /**
     * Выражения для получения значения равенства
     */
    const REG_AND = '/$:params\s*=\s*([a-zA-Z_0-9]+)/i';
    /**
     * Выражение для получения данных in
     */
    const REG_IN = '/$:params\s*IN\s*\(([a-zA-Z_0-9,\s]+)\)/i';
    /**
     * Выражение для получения данных between
     */
    const REG_BETWEEN = '/$:params\s*BETWEEN\s*([0-9]+) AND ([0-9]+)/i';

    /**
     * @var array
     */
    private $typeParser = [
        'parserParamsAnd',
        'parserParamsIn',
        'parserParamsBetween'
    ];

    /**
     * @var
     */
    private static $instance;
    /**
     * @var
     */
    private $sql;
    /**
     * @var
     */
    private $column;

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
     * @param mixed $sql
     * @return $this
     */
    public function setSql($sql)
    {
        $this->sql = $sql;
        return $this;
    }

    /**
     * @param mixed $column
     * @return $this
     */
    public function setColumn($column)
    {
        $this->column = $column;
        return $this;
    }

    /**
     * @return array
     * @throws InvalidParamException
     */
    public function execute()
    {
        if (!$this->column) {
            throw new InvalidParamException('Set the column for parser.');
        }

        $result = [];

        foreach ($this->typeParser as $method) {
            $this->$method($result);
        }

        return $result;
    }

    /**
     * Находим значения равенства в sql
     * @param $result
     */
    private function parserParamsAnd(&$result)
    {
        if (preg_match_all(str_replace("$:params", $this->column, self::REG_AND), $this->sql, $matches)) {

            /*
             *  Array
             *  (
             *       [0] => 100
             *   )
             */
            list(, $data) = $matches;
            $result = array_merge($result, $data);
        }
    }

    /**
     * Находим значений in в sql
     * @param $result
     */
    private function parserParamsIn(&$result)
    {
        if (preg_match_all(str_replace("$:params", $this->column, self::REG_IN), $this->sql, $matches)) {
            /*
             *  Array
             *  (
             *       [0] => 100, 101
             *   )
             */
            list(, $data) = $matches;
            foreach ($data as $value) {
                $arrData = array_map("trim", explode(",", $value));;
                $result = array_merge($result, $arrData);
            }
        }
    }

    /**
     * Находим значений between в sql
     * @param $result
     */
    private function parserParamsBetween(&$result)
    {
        if (preg_match_all(str_replace("$:params", $this->column, self::REG_BETWEEN), $this->sql, $matches, PREG_SET_ORDER)) {
            /*
             *   [0] => id BETWEEN 199 AND 202
             *   [1] => 199
             *   [2] => 202
             */
            foreach ($matches as $value) {
                if ($value[2] >= $value[1]) {
                    for ($i = $value[1]; $i <= $value[2]; $i++) {
                        $result[] = $i;
                    }
                }
            }
            $result = array_unique($result);
        }
    }
}