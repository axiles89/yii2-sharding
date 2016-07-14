<?php
/**
 * Command.php
 *
 * @package axiles89\sharding
 * @date: 09.03.2016 18:04
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;


use yii\base\Component;

/**
 * Class Command
 * @package axiles89\sharding
 */
class Command extends Component
{
    /**
     * @var
     */
    public $db;
    /**
     * @var
     */
    public $sql;
    /**
     * @var array
     */
    private $_pendingParams = [];
    /**
     * @var array
     */
    public $params = [];
    /**
     * @var
     */
    public $pdoStatement;
    /**
     * @var int
     */
    public $fetchMode = \PDO::FETCH_ASSOC;

    /**
     * Получение всех данных
     * @param null $fetchMode
     * @return array
     */
    public function queryAll($fetchMode = null)
    {
        return $this->queryInternal('fetchAll', $fetchMode);
    }

    /**
     * Извлечение следующей строки из результирующего набора (первой)
     * @param null $fetchMode
     * @return array
     */
    public function queryOne($fetchMode = null)
    {
        return $this->queryInternal('fetch', $fetchMode);
    }

    /**
     * Подстановка параметров и получение готового sql запроса для каждого шарда
     * @return array
     * @throws \yii\base\InvalidConfigException
     */
    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->sql;
        }

        $params = [];
        $sql = [];

        foreach ($this->params as $dbName => $paramsDb) {
            foreach ($paramsDb as $name => $value) {
                if (is_string($name) && strncmp(':', $name, 1)) {
                    $name = ':' . $name;
                }

                $db = \Yii::$app->get($dbName);

                if (is_string($value)) {
                    $params[$name] = $db->quoteValue($value);
                } elseif (is_bool($value)) {
                    $params[$name] = ($value ? 'TRUE' : 'FALSE');
                } elseif ($value === null) {
                    $params[$name] = 'NULL';
                } elseif (!is_object($value) && !is_resource($value)) {
                    $params[$name] = $value;
                }
            }

            $sql[$dbName] = strtr($this->sql[$dbName], $params);
        }

        return $sql;
    }

    /**
     * Подготовка pdo компонентов для каждого шарда с его запросом
     * @param null $forRead
     * @throws \Exception
     */
    public function prepare($forRead = null)
    {
        $sql = $this->getSql();

        try {
            foreach ($this->db as $dbName) {
                $db = \Yii::$app->get($dbName);

                // Получаем pdo объект для нужного компонента db
                if ($forRead || $forRead === null && $db->getSchema()->isReadQuery($sql)) {
                    $pdo = $db->getSlavePdo();
                } else {
                    $pdo = $db->getMasterPdo();
                }

                $this->pdoStatement[$dbName] = $pdo->prepare($sql[$dbName]);
            }

            $this->bindPendingParams();

        } catch (\Exception $e) {
            $message = $e->getMessage() . "\nFailed to prepare SQL: " . var_export($sql, true);
            $errorInfo = $e instanceof \PDOException ? $e->errorInfo : null;
            throw new \Exception($message . $errorInfo, (int)$e->getCode(), $e);
        }
    }

    /**
     * Получение значения первого столбца следующей строки
     * @return int
     */
    public function queryScalar()
    {
        $data = $this->queryInternal('fetchColumn', 0);

        $result = 0;
        foreach ($data as $count) {
            $result += $count;
        }

        return $result;
    }

    /**
     * Исполнение запросов select
     * @param $method
     * @param null $fetchMode
     * @return array
     * @throws \Exception
     * @throws \yii\base\InvalidConfigException
     */
    protected function queryInternal($method, $fetchMode = null)
    {
        $token = $this->getRawSql();
        $this->prepare(true);

        $result = [];
        foreach ($this->pdoStatement as $dbName => $pdo) {
            try {
                \Yii::beginProfile($token[$dbName], 'axiles89\sharding\Command::query');
                $pdo->execute();

                if ($fetchMode === null) {
                    $fetchMode = $this->fetchMode;
                }
                $data = call_user_func_array([$pdo, $method], (array)$fetchMode);

                $pdo->closeCursor();

                if ($data) {
                    $result = array_merge($result, (is_array($data)) ? $data : [$data]);
                }
                \Yii::endProfile($token[$dbName], 'axiles89\sharding\Command::query');
            } catch (\Exception $e) {
                \Yii::endProfile($token[$dbName], 'axiles89\sharding\Command::query');
                throw \Yii::$app->get($dbName)->getSchema()->convertException($e, $token[$dbName]);
            }

        }

        return $result;
    }

    /**
     * Исполнение запросов типа insert, delete, update
     * @return int
     * @throws \yii\base\InvalidConfigException
     */
    public function execute()
    {
        $sql = $this->getSql();
        $token = $this->getRawSql();

        if (!$sql) {
            return 0;
        }

        $this->prepare(false);

        $n = 0;

        foreach ($this->pdoStatement as $dbName => $pdo) {
            try {
                \Yii::beginProfile($token[$dbName], __METHOD__);
                $pdo->execute();
                $n += $pdo->rowCount();
                \Yii::endProfile($token[$dbName], __METHOD__);
            } catch (\Exception $e) {
                \Yii::endProfile($token[$dbName], __METHOD__);
                throw \Yii::$app->get($dbName)->getSchema()->convertException($e, $token[$dbName]);
            }
        }

        return $n;
    }

    /**
     * Привязка параметров для дальнейшей привязки к конкретным объектам pdo
     * @param $values - массив параметров запроса, который построил builder ([:qp0] => 555)
     * @return $this
     * @throws \yii\base\InvalidConfigException
     */
    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }

        foreach ($this->db as $dbComponent) {
            $schema = \Yii::$app->get($dbComponent)->getSchema();

            foreach ($values as $name => $value) {
                if (is_array($value)) {
                    $this->_pendingParams[$dbComponent][$name] = $value;
                    $this->params[$dbComponent][$name] = $value[0];
                } else {
                    $type = $schema->getPdoType($value);
                    $this->_pendingParams[$dbComponent][$name] = [$value, $type];
                    $this->params[$dbComponent][$name] = $value;
                }
            }
        }

        return $this;
    }

    /**
     * Привязка параметров к объекту pdo каждого шарда
     */
    protected function bindPendingParams()
    {
        foreach ($this->_pendingParams as $dbName => $params) {
            foreach ($params as $name => $value) {
                $this->pdoStatement[$dbName]->bindValue($name, $value[0], $value[1]);
            }
            $this->_pendingParams[$dbName] = [];
        }
    }

    /**
     * @return mixed
     */
    public function getSql()
    {
        return $this->sql;
    }

}