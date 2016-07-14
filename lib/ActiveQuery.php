<?php
/**
 * ActiveQuery.php
 *
 * @package axiles89\sharding
 * @date: 01.03.2016 17:22
 * @author: Kyshnerev Dmitriy <dimkysh@mail.ru>
 */

namespace axiles89\sharding;


use yii\base\Component;
use yii\base\Exception;
use yii\base\InvalidCallException;
use yii\base\InvalidConfigException;
use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;
use yii\db\Expression;
use yii\db\Query;
use yii\db\QueryTrait;

/**
 * Class ActiveQuery
 * @package axiles89\sharding
 */
class ActiveQuery extends Query implements ActiveQueryInterface
{
    use QueryTrait;
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * @var
     */
    public $select;
    /**
     * @var
     */
    public $selectOption;
    /**
     * @var
     */
    public $distinct;
    /**
     * @var
     */
    public $from;
    /**
     * @var
     */
    public $join;
    /**
     * @var
     */
    public $groupBy;
    /**
     * @var
     */
    public $having;
    /**
     * @var
     */
    public $union;
    /**
     * @var array
     */
    public $params = [];

    /**
     * @param array $modelClass
     * @param array $config
     */
    public function __construct($modelClass, $config = [])
    {
        $this->modelClass = $modelClass;
        parent::__construct($config);
    }

    /**
     * Создание команды с нужным зпросов для нужных шардов
     * @param $shardDb
     * @return mixed
     * @throws InvalidConfigException
     */
    public function createCommand($shardDb = null)
    {
        $modelClass = $this->modelClass;
        $db = $modelClass::getDb();

        if ($shardDb === null) {
            if (!isset($db->shard[$modelClass::shardingType()])) {
                throw new InvalidConfigException('The sharding component for this Active Record model not found');
            }

            // Получаем нужные шарды
            $valueKey = HelperCoordinator::getInstance()->getData($this->where, $this->params, $modelClass::shardingColumn());
            $shardType = $db->shard[$modelClass::shardingType()];
            $coordinator = \Yii::$app->{$shardType['coordinator']};
            $shardDb = $coordinator->getShard($shardType['db'], $valueKey);

            if (!$shardDb) {
                $shardDb = $shardType['db'];
            }
        }

        // Строим запрос для каждого шарда
        list($sql, $params) = $db->getQueryBuilder($shardDb)->build($this);
        $command = $db->createCommand($shardDb, $sql, $params);

        return $command;
    }

    /**
     * @param $params
     * @return $this
     */
    public function addParams($params)
    {
        if (!empty($params)) {
            if (empty($this->params)) {
                $this->params = $params;
            } else {
                foreach ($params as $name => $value) {
                    if (is_int($name)) {
                        $this->params[] = $value;
                    } else {
                        $this->params[$name] = $value;
                    }
                }
            }
        }
        return $this;
    }

    /**
     * @param $columns
     * @param null $option
     * @return $this
     */
    public function select($columns, $option = null)
    {
        if (!is_array($columns)) {
            // ('id', 'title') => ['id' => '', 'title' => '']
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->select = $columns;
        $this->selectOption = $option;
        return $this;
    }

    /**
     * @param $sql
     * @param bool|false $all
     * @return $this
     */
    public function union($sql, $all = false)
    {
        $this->union[] = ['query' => $sql, 'all' => $all];
        return $this;
    }

    /**
     * @param $type
     * @param $table
     * @param string $on
     * @return $this
     */
    public function join($type, $table, $on = '', $params = [])
    {
        $this->join[] = [$type, $table, $on];
        return $this;
    }

    /**
     * @param $table
     * @param string $on
     * @return $this
     */
    public function innerJoin($table, $on = '', $params = [])
    {
        $this->join[] = ['INNER JOIN', $table, $on];
        return $this;
    }

    /**
     * @param $table
     * @param string $on
     * @return $this
     */
    public function leftJoin($table, $on = '', $params = [])
    {
        $this->join[] = ['LEFT JOIN', $table, $on];
        return $this;
    }

    /**
     * @param $table
     * @param string $on
     * @return $this
     */
    public function rightJoin($table, $on = '', $params = [])
    {
        $this->join[] = ['RIGHT JOIN', $table, $on];
        return $this;
    }

    /**
     * @param $columns
     * @return $this
     */
    public function groupBy($columns)
    {
        if (!is_array($columns)) {
            $columns = array_map('trim', explode(',', trim($columns)));
        }

        $this->groupBy = $columns;
        return $this;
    }

    /**
     * @param bool|true $value
     * @return $this
     */
    public function distinct($value = true)
    {
        $this->distinct = $value;
        return $this;
    }

    /**
     * @param $columns
     * @return $this
     */
    public function addGroupBy($columns)
    {
        if (!is_array($columns)) {
            $columns = array_map('trim', explode(',', trim($columns)));
        }
        if ($this->groupBy === null) {
            $this->groupBy = $columns;
        } else {
            $this->groupBy = array_merge($this->groupBy, $columns);
        }
        return $this;
    }

    /**
     * @param $condition
     * @param array $params
     * @return $this
     */
    public function having($condition, $params = [])
    {
        $this->having = $condition;
        $this->addParams($params);
        return $this;
    }

    /**
     * @param $condition
     * @param array $params
     * @return $this
     */
    public function andHaving($condition, $params = [])
    {
        if ($this->having === null) {
            $this->having = $condition;
        } else {
            $this->having = ['and', $this->having, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * @param $condition
     * @param array $params
     * @return $this
     */
    public function orHaving($condition, $params = [])
    {
        if ($this->having === null) {
            $this->having = $condition;
        } else {
            $this->having = ['or', $this->having, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * Метод необходим для создания объекта запроса для QueryBuilder
     * @param $builder
     * @return Query
     * @throws Exception
     */
    public function prepare($builder)
    {
        if (empty($this->from)) {
            $modelClass = $this->modelClass;
            $tableName = $modelClass::tableName();
            $this->from = [$tableName];
        }

        if (empty($this->select) && !empty($this->join)) {
            foreach ((array)$this->from as $alias => $table) {
                if (is_string($alias)) {
                    $this->select = ["$alias.*"];
                } elseif (is_string($table)) {
                    if (preg_match('/^(.*?)\s+({{\w+}}|\w+)$/', $table, $matches)) {
                        $alias = $matches[2];
                    } else {
                        $alias = $table;
                    }

                    $this->select = ["$alias.*"];
                }
                break;
            }
        }

        if ($this->primaryModel === null) {
            // eager loading
            $query = Query::create($this);
        } else {
            // lazy loading of a relation
            $where = $this->where;

            if ($this->via instanceof self) {
                // via junction table
                $viaModels = $this->via->findJunctionRows([$this->primaryModel]);
                $this->filterByModels($viaModels);
            } elseif (is_array($this->via)) {
                // via relation
                /* @var $viaQuery ActiveQuery */
                list($viaName, $viaQuery) = $this->via;
                if ($viaQuery->multiple) {
                    $viaModels = $viaQuery->all();
                    $this->primaryModel->populateRelation($viaName, $viaModels);
                } else {
                    $model = $viaQuery->one();
                    $this->primaryModel->populateRelation($viaName, $model);
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }

            $query = Query::create($this);
            $this->where = $where;
        }

        return $query;
    }

    /**
     * Создание модели из результатов выборки
     * @param $rows
     * @return array|\yii\db\ActiveRecord[]
     */
    public function populate($rows)
    {
        if (empty($rows)) {
            return [];
        }

        $models = $this->createModels($rows);
        $models = $this->removeDuplicatedModels($models);

        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }
        if (!$this->asArray) {
            foreach ($models as $model) {
                $model->afterFind();
            }
        }

        return $models;
    }

    /**
     * Удаление дубликатов по первичному ключу
     * @param $models
     * @return array
     * @throws InvalidCallException
     * @throws InvalidConfigException
     */
    private function removeDuplicatedModels($models)
    {
        $hash = [];
        /* @var $class ActiveRecord */
        $class = $this->modelClass;
        $pks = $class::primaryKey();

        if (count($pks) > 1) {
            // composite primary key
            foreach ($models as $i => $model) {
                $key = [];
                foreach ($pks as $pk) {
                    if (!isset($model[$pk])) {
                        // do not continue if the primary key is not part of the result set
                        break 2;
                    }
                    $key[] = $model[$pk];
                }
                $key = serialize($key);
                if (isset($hash[$key])) {
                    unset($models[$i]);
                } else {
                    $hash[$key] = true;
                }
            }
        } elseif (empty($pks)) {
            throw new InvalidCallException("Primary key of '{$class}' can not be empty.");
        } else {
            // single column primary key
            $pk = reset($pks);
            foreach ($models as $i => $model) {
                if (!isset($model[$pk])) {
                    // do not continue if the primary key is not part of the result set
                    break;
                }
                $key = $model[$pk];
                if (isset($hash[$key])) {
                    unset($models[$i]);
                } elseif ($key !== null) {
                    $hash[$key] = true;
                }
            }
        }

        return array_values($models);
    }

    /**
     * @param $tableName
     * @param $link
     * @param callable|null $callable
     * @return $this
     */
    public function viaTable($tableName, $link, callable $callable = null)
    {
        $relation = new ActiveQuery(get_class($this->primaryModel), [
            'from' => [$tableName],
            'link' => $link,
            'multiple' => true,
            'asArray' => true,
        ]);
        $this->via = $relation;
        if ($callable !== null) {
            call_user_func($callable, $relation);
        }

        return $this;
    }

    /**
     * @param null $db - название компонентов нужных db компонентов ['db1', 'db2']
     * @return array|\yii\db\ActiveRecord[]
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function all($db = null)
    {
        $command = $this->createCommand($db);

        if ((empty($this->groupBy) && empty($this->having) && empty($this->union)) || (isset($command->db) and count($command->db) < 2)) {
            $rows = $this->createCommand($db)->queryAll();;
            return $this->populate($rows);
        } else {
            throw new Exception('This query uses more than one database');
        }
    }

    /**
     * @param null $db - название компонентов нужных db компонентов ['db1', 'db2']
     * @return null
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function one($db = null)
    {
        $command = $this->createCommand($db);

        if ((empty($this->groupBy) && empty($this->having) && empty($this->union)) || (isset($command->db) and count($command->db) < 2)) {
            $row = $command->queryOne();

            if ($row) {
                $models = $this->populate([$row]);
                return reset($models) ?: null;
            } else {
                return null;
            }
        } else {
            throw new Exception('This query uses more than one database');
        }
    }

    /**
     * @param string $q
     * @param null $db - название компонентов нужных db компонентов ['db1', 'db2']
     * @return mixed
     * @throws Exception
     */
    public function count($q = '*', $db = null)
    {
        return $this->queryScalar("COUNT($q)", $db);
    }

    /**
     * @param null $db - название компонентов нужных db компонентов ['db1', 'db2']
     * @return bool
     * @throws InvalidConfigException
     */
    public function exists($db = null)
    {
        $select = $this->select;
        $this->select = [new Expression('1')];
        $command = $this->createCommand($db);

        return ($command->queryScalar()) ? true : false;
    }

    /**
     * Получение значения первой колонки результата выборки (для скалярных запросов)
     * @param $selectExpression
     * @param $db - название компонентов нужных db компонентов ['db1', 'db2']
     * @return mixed
     * @throws Exception
     * @throws InvalidConfigException
     */
    protected function queryScalar($selectExpression, $db)
    {
        $this->select = [$selectExpression];
        $this->limit = null;
        $this->offset = null;
        $command = $this->createCommand($db);

        if ((empty($this->groupBy) && empty($this->having) && empty($this->union) && !$this->distinct) || (isset($command->db) and count($command->db) < 2)) {
            return $command->queryScalar();
        } else {
            throw new Exception('This query uses more than one database');
        }
    }

}