(ns importdata.dbutil.dbmanipulate
  (:require [clojure.java.jdbc :as j]
            [importdata.dbutil.dbconnection :as conn]))

(defmacro query
  "
    param:
    sql string 需要执行的sql和参数 exp (\"select * from table where id = ?\" \"10\")
    fun key 对结果进行处理的参数 exp :row-fn :cost
  "
  [sql & fun]
  `(j/query conn/db-connection
            [~@sql]
            ~@fun))

(defmacro insert!
  "
    param:
    table key 表名 exp :table
    row-map map 插入行的map exp {:id \"1\" :name \"hello\"}
  "
  [table & row-map]
  `(j/insert! conn/db-connection ~table
              ~@row-map))

(defmacro update!
  "
    param:
    table key 表名 exp :table
    update-map map 跟新的字段和值的map exp {:id 10}
    condition vec [\"string\" string or number] 更新字段的限定条件 exp [\"id = ?\" 20]
  "
  [table update-map condition]
  `(j/update! conn/db-connection ~table
              ~update-map
              ~condition))

(defmacro delete!
  "
    param:
    table key 表名 exp :table
    condition vec [string string or number] 删除行的限定条件 exp [\"id = ?\" 1]
  "
  [table condition]
  `(j/delete! conn/db-connection ~table
              ~condition))
