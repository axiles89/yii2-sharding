<?php
/**
 * Connection.php
 *
 * @package axiles89\sharding
 * @date: 04.03.2016 19:56
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;


use yii\base\Component;
use yii\base\InvalidConfigException;

/**
 * Class Connection
 * @package axiles89\sharding
 */
class Connection extends Component
{
    /**
     * @var
     */
    public $shard;

    /**
     * @throws InvalidConfigException
     */
    public function init() {
        parent::init();

        foreach ($this->shard as $value) {
            if (!isset($value['db']) or !$value['db']) {
                throw new InvalidConfigException("Please set db for shard");
            }
            if (!isset($value['coordinator']) or !$value['coordinator']) {
                throw new InvalidConfigException("Please set coordinator for shard");
            }
        }
    }

    /**
     * Создание команды на выполнение запросов
     * createCommand('db1', ['db1' => 'SELECT ...']);
     * @param $db
     * @param array $sql
     * @param array $params
     * @return $this
     */
    public function createCommand($db, $sql = [], $params = []) {
        $command = new Command([
            'db' => (!is_array($db)) ? [$db] : $db,
            'sql' => $sql,
        ]);

        return $command->bindValues($params);
    }

    /**
     * Закрытие соединений со всеми db
     * @throws InvalidConfigException
     */
    public function close() {
        foreach ($this->shard as  $valueShard) {
            foreach ($valueShard['db'] as $db) {
                \Yii::$app->get($db)->close();
            }
        }
    }

    /**
     * @param $db
     * @return QueryBuilder
     */
    public function getQueryBuilder($db) {
        return new QueryBuilder((!is_array($db)) ? [$db]: $db);
    }
}