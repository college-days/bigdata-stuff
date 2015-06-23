(ns importdata.data
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clj-time.local :as l]
            [clojure.java.jdbc :as jdbc]
            [importdata.dbutil.dbmanipulate :as db])
  (:import (java.text SimpleDateFormat)
           (java.util Date)
           (java.sql Timestamp)))

(defn get-time
  "trans timestamp to time
  example: (get-time '1394463205')
  139446320 -> 2014-03-10 22:53"
  [timestamp]
  (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
           (Timestamp. (* (Integer/parseInt timestamp) 1000))))

(get-time "1394678871")

(defn get-unix-timestamp-from-psql-timestamp
  "从psql的timestamp类型字段中获得时间对象，然后转为unix时间戳，从而计算时间差"
  [timestamp]
  (/ (.getTime timestamp) 1000))

(defn get-distinct-userid
  []
  (map :user_id
       (db/query
        ["select distinct user_id from news order by user_id asc"])))

(defn importnews
  []
  (map (fn [result]
         (let [visit (:visit_timestamp result)
               visit_timestamp (get-unix-timestamp-from-psql-timestamp visit)
               visit_time (get-time (str (get-unix-timestamp-from-psql-timestamp visit)))
               post (:post_timestamp result)
               post_timestamp (get-unix-timestamp-from-psql-timestamp post)
               post_time (get-time (str (get-unix-timestamp-from-psql-timestamp post)))]
           (str (:user_id result) "\t" (:news_id result) "\t" visit_timestamp "\t" visit_time "\t" post_timestamp "\t" post_time "\t" (:news_title result) "\t" (:news_content result))))
       (db/query
        ["select * from news"])))

(defn importoldestvisit
  []
  (map (fn [result]
         (let [visit (:visit_timestamp result)
               visit_timestamp (get-unix-timestamp-from-psql-timestamp visit)
               visit_time (get-time (str (get-unix-timestamp-from-psql-timestamp visit)))]
           (str (:news_id result) "\t" visit_timestamp "\t" visit_time)))
       (db/query
        ["select * from oldestvisit"])))

(defn importoneday
  []
  (map (fn [result]
         (str (:user_id result) "\t" (:news_id result)))
       (db/query
        ["select * from oneday"])))

(defn importnewscity
  []
  (map (fn [result]
         (str (:news_id result) "\t" (:cities result)))
       (db/query
        ["select * from newscity"])))

(defn importvisitcount
  []
  (map (fn [result]
         (str (:user_id result) "\t" (:news_id result) "\t" (:cnt result)))
       (db/query
        ["select user_id, news_id, count(*) as cnt from news group by user_id, news_id"])))

(defn importdistinct-newscontent
  []
  (map (fn [result]
         (str (:news_content result)))
       (db/query
        ["select distinct news_content from news"])))

(defn importdistinct-newstitle
  []
  (map (fn [result]
         (str (:news_title result)))
       (db/query
        ["select distinct news_title from news"])))

(defn importnews-for-specific-user
  [userid]
  (reduce (fn [old new]
            (str old "   " new))
          ;;""
          (map :news_content
               (db/query
                ["select news_content from news where user_id = ?" userid]))))

(defn importnewstitle-for-specific-user
  [userid]
  (reduce (fn [old new]
            (str old "   " new))
          (map :news_title
               (db/query
                ["select news_title from news where user_id = ?" userid]))))

(defn importnews-for-users
  []
  (map (fn [userid]
         (let [newscontents (importnews-for-specific-user userid)]
           (str userid "\t" newscontents)))
       (get-distinct-userid)))

(defn importnewstitle-for-users
  []
  (map (fn [userid]
         (let [newstitles (importnewstitle-for-specific-user userid)]
           (str userid "\t" newstitles)))
       (get-distinct-userid)))

(defn write-file
  [contents filename]
  (with-open [w (clojure.java.io/writer filename :append true)]
    (doall
     (map (fn [content]
            (println content)
            (.write w (str content "\n")))
          contents))))

(reduce (fn [money year]
          (* 1.0325 (+ 1.4 money)))
        0
        (range 0 40))

(loop  [year 1 result 1.4]
  (if (= 41 year)
    result
    (recur (inc year) (+ 1.4 (* result 1.0325)))))
