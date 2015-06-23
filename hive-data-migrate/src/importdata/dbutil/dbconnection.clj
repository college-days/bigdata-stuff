(ns importdata.dbutil.dbconnection)

(def db-name "newsrecommend")
(def db-user "zjh")
(def db-host "localhost")
(def db-port "5432")
(def db-password "cleantha")
(def db-connection
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname (str "//" db-host ":" db-port "/" db-name)
   :user db-user
   :password db-password})
