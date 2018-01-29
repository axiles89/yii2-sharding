<?php
/**
 * QueryBuilder.php
 *
 * @package axiles89\sharding
 * @date: 09.03.2016 14:40
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;

use yii\base\Object;

/**
 * Построитель запросов
 * Class QueryBuilder
 * @package axiles89\sharding
 */
class QueryBuilder extends Object
{
    /**
     * Массив названий компонентов db
     * @var array
     */
    private $db;

    /**
     * @param array $dbComponent
     * @param array $config
     */
    public function __construct(array $dbComponent, array $config = [])
    {
        $this->db = $dbComponent;
        parent::__construct($config);
    }

    /**
     * Построение запросов delete для всех нужных компонентов db
     * @param $table
     * @param string $condition
     * @param array $params
     * @return array
     *
     *      [0] => Array
     *          (
     *              [db1] => DELETE FROM `region` WHERE `id`=:qp0
     *          )
     *
     *      [1] => Array
     *          (
     *              [:qp0] => 555
     *          )
     *
     * @throws \yii\base\InvalidConfigException
     */
    public function delete($table, $condition = '', array $params = [])
    {
        $result = [
            0 => [],
            1 => []
        ];

        $pr = $params;
        foreach ($this->db as $key => $value) {
            $result[0][$value] = \Yii::$app->get($value)->getQueryBuilder()->delete($table, $condition, $pr);
            $result[1] = $pr;
            $pr = [];
        }

        return $result;
    }

    /**
     * Построение запросов update для всех нужных компонентов db
     * @param $table
     * @param $columns
     * @param string $condition
     * @param array $params
     * @return array
     *
     *      [0] => Array
     *          (
     *              [db1] => UPDATE `region` SET `description`=:qp0 WHERE `id`=:qp1
     *          )
     *
     *      [1] => Array
     *          (
     *              [:qp0] => bbbb
     *              [:qp1] => 555
     *          )
     *
     * @throws \yii\base\InvalidConfigException
     */
    public function update($table, $columns, $condition = '', array $params = [])
    {
        $result = [
            0 => [],
            1 => []
        ];

        $pr = $params;
        foreach ($this->db as $key => $value) {
            $result[0][$value] = \Yii::$app->get($value)->getQueryBuilder()->update($table, $columns, $condition, $pr);
            $result[1] = $pr;
            $pr = [];
        }

        return $result;
    }

    /**
     * Построитель запросов select
     * @param $query
     * @param array $params
     * @return array
     *
     *      [0] => Array
     *          (
     *              [db1] => SELECT id FROM `region` WHERE `id`=:qp0
     *          )
     *
     *      [1] => Array
     *          (
     *              [:qp0] => 555
     *          )
     *
     * @throws \yii\base\InvalidConfigException
     */
    public function build($query, array $params = [])
    {
        $result = [
            0 => [],
            1 => []
        ];

        foreach ($this->db as $key => $value) {
            $db = \Yii::$app->get($value);
            list($sql, $pr) = $db->getQueryBuilder()->build($query, $params);
            $result[0][$value] = $sql;
            $result[1] = $pr;
        }

        return $result;
    }
}