(ns newsrecommend.data.preprocessdata
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clj-time.local :as l]
            [clojure.java.jdbc :as jdbc]
            [newsrecommend.dbutil.dbmanipulate :as db])
  (:import (java.text SimpleDateFormat)
           (java.util Date)
           (java.sql Timestamp)))

(defn initdata
  "read data from disk"
  [filename]
  (clojure.string/split (slurp filename) #"\n"))

(defn initdata-forreal
  "read data from disk"
  [filename]
  (clojure.string/split (slurp filename) #"\r\n"))

(defn parsedata
  "parse each line data to list"
  [newscontent]
  (clojure.string/split newscontent #"\t"))

;;deprecated
(defn get-time
  "trans timestamp to time
  example: (get-time '1394463205')
  139446320 -> 2014-03-10 22:53"
  [timestamp]
  (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm")
           (Timestamp. (* (Integer/parseInt timestamp) 1000))))

;;deprecated
(defn get-unix-timestamp
  "trans time to unix timestamp
  example: (get-unix-timestamp '2014-03-10 14:53')
  2014-03-10 14:53 -> 139443438000"
  [time]
  (/ (.getTime (.parse (SimpleDateFormat. "yyyy-MM-dd hh:mm") time)) 1000))

(defn get-unix-timestamp-from-psql-timestamp
  "从psql的timestamp类型字段中获得时间对象，然后转为unix时间戳，从而计算时间差"
  [timestamp]
  (/ (.getTime timestamp) 1000))

(defn get-interval
  "从psql的timestamp类型字段中获得两个时间，然后计算以秒为单位的时间间隔"
  [endtime starttime]
  (- (get-unix-timestamp-from-psql-timestamp endtime)
     (get-unix-timestamp-from-psql-timestamp starttime)))

(defn get-interval-days
  "获得以天为单位的时间差，返回的是间隔天数，1表示当天，2表示第二天，3表示第三天"
  [endtime starttime]
  (let [interval (get-interval endtime starttime)
        interval-days (/ interval (* 24. 3600))
        interval-left (int interval-days)
        interval-right (- interval-days interval-left)]
    (if (> interval-right 0)
      (+ interval-left 1)
      interval-left)))

(defn is-in-interval?
  "统计新闻点击次数随时间变化情况，当天、1天后、2天后…n天后点击数，此函数作用是判断浏览时间和发布时间的时间差是否在第n天中，传入的day参数0值表示当天，1值表示第二天"
  [endtime starttime day]
  (let [interval (get-interval endtime starttime)]
    (and (< interval (* (+ day 1) (* 24 3600))) (> interval (* day (* 24 3600))))))

(defn is-interval-greater-than-days?
  "查看时间差是否超过目标天数，输入分别是从psql中获得的timestamp类型字段的内容，以及以天为单位的时间差阈值"
  [endtime starttime days]
  (let [interval (get-interval endtime starttime)]
    (> interval (* days (* 24 3600)))))

(defn get-psql-timestamp-from-time
  "trans time to psql timestamp
  example: (get-psql-timestamp-from-time '2014年03月13日11:32')"
  [time]
  (Timestamp. (.getTime (.parse (SimpleDateFormat. "yyyy-MM-dd hh:mm")
                                (clojure.string/replace
                                 (clojure.string/replace time #"[年|月]" "-")
                                 #"日" " ")))))

(defn get-psql-timestamp-from-simpletime
  "trans time to psql timestamp
  example: (get-psql-timestamp-from-simpletime '2014年03月10日')"
  [time]
  (Timestamp. (.getTime (.parse (SimpleDateFormat. "yyyy-MM-dd")
                                (clojure.string/replace
                                 (clojure.string/replace time #"[年|月]" "-")
                                 #"日" "")))))

(defn get-psql-timestamp-from-timestamp
  "trans timestamp to psql timestamp
  example: (get-psql-timestamp-from-timestamp '1394463205')"
  [timestamp]
  (Timestamp. (* (Integer/parseInt timestamp) 1000)))

(def news-id-post-time-map (atom {}))

;;how to update the global map
;;(swap! news-id-post-time-map assoc 333 123)
;;(get @news-id-post-time-map 333)

;;deprecated
(defn get-psql-timestamp-from-newsid
  "因为会有发布日期为NULL的情况，所以需要以最早点击的时间作为新闻的发布时间"
  [news_id visit_time]
  (let [mem-visit_time (get @news-id-post-time-map news_id)]
    (if mem-visit_time
      mem-visit_time
      (do (swap! news-id-post-time-map assoc news_id visit_time)
          visit_time))))

;;deprecated
(defn get-psql-timestamp-from-newsid-smalleast
  "上面那个函数有一个问题就是只是按照记录出现的次数来，而不是按照时间的大小来，所以需要判断一下时间戳大小，不然最后计算时间差的时候会有负值"
  [news_id visit_time]
  (let [mem-visit_time (get @news-id-post-time-map news_id)]
    (if mem-visit_time
      (let [mem-unix-stamp (get-unix-timestamp-from-psql-timestamp mem-visit_time)
            new-unix-stamp (get-unix-timestamp-from-psql-timestamp visit_time)]
        (if (< new-unix-stamp mem-unix-stamp)
          (do (swap! news-id-post-time-map assoc news_id visit_time)
              visit_time)
          mem-visit_time))
      (do (swap! news-id-post-time-map assoc news_id visit_time)
          visit_time))))

(defn get-oldestvisit-from-psql
  "根据newsid从psql中获得最早的浏览时间"
  [newsid]
  (db/query
   ["select visit_timestamp from oldestvisit where news_id = ?" newsid]))

;;(empty? (get-oldestvisit-from-psql 100508882))
;;(get-unix-timestamp-from-psql-timestamp (:visit_timestamp (nth (get-oldestvisit-from-psql 100508882) 0)))

(defn insert-or-update-oldestvisit-into-psql
  "插入或者更新给定新闻id的最早浏览时间"
  [newsid visit_time]
  (let [lastrecord (get-oldestvisit-from-psql newsid)]
    (if (empty? lastrecord)
      (db/insert! :oldestvisit
                  {:news_id newsid
                   :visit_timestamp visit_time})
      (let [last-visit (get-unix-timestamp-from-psql-timestamp (:visit_timestamp (nth lastrecord 0)))
            new-visit (get-unix-timestamp-from-psql-timestamp visit_time)]
        (if (< new-visit last-visit)
          (db/update! :oldestvisit
                      {
                       :visit_timestamp visit_time}
                      ["news_id = ?" newsid]))))))

;;(insert-or-update-oldestvisit-into-psql 100508882 (get-psql-timestamp-from-timestamp "1393853381"))

(defn setup-oldestvisit
  "为所有发布日期为NULL的新闻按最早的浏览时间近似作为发布日期，此函数是用来存储最早浏览日期的psql数据表"
  [user_id news_id visit_timestamp news_title news_content post_time]
  (let [news_id (Integer/parseInt news_id)
        visit_timestamp (get-psql-timestamp-from-timestamp visit_timestamp)]
    (if (= "NULL" post_time)
      (insert-or-update-oldestvisit-into-psql news_id visit_timestamp))))

(defn create!
  "创建新闻
  example: (create! \"5218791\" \"100648802\" \"1394463205\" \"马航代表与乘客家属见面\" \"3月9日，马来西亚航空公司代表在北京与马航客机失联事件的乘客家属见面。沈伯韩/新华社（手机拍摄）1/4\" \"2014年03月09日13:00\")"
  [user_id news_id visit_timestamp news_title news_content post_time]
  (let [user_id (Integer/parseInt user_id)
        news_id (Integer/parseInt news_id)
        visit_timestamp (get-psql-timestamp-from-timestamp visit_timestamp)
        post_timestamp (if (= "NULL" post_time)
                         (:visit_timestamp (nth (get-oldestvisit-from-psql news_id) 0))
                         (if (re-find #":" post_time)
                           (get-psql-timestamp-from-time post_time)
                           (get-psql-timestamp-from-simpletime post_time)))]
    (println user_id "\t" news_id "\t" visit_timestamp "\t" post_timestamp "\t" news_title "\t" news_content)
    (db/insert! :news
                {:user_id user_id
                 :news_id news_id
                 :visit_timestamp visit_timestamp
                 :post_timestamp post_timestamp
                 :news_title news_title
                 :news_content news_content})))

;;(:visit_timestamp (nth (get-oldestvisit-from-psql 100508882) 0))

(defn get-visit-less-post-news
  "从interval这张表中得到浏览时间小于发布日时间的所有新闻id"
  []
  (map :news_id
       (db/query
        ["select distinct news_id from interval where interval < 0"])))

;;(get-visit-less-post-news)

(defn get-oldestvisit-for-news
  "根据给定新闻id从news表中获得所有和这个新闻id关联的浏览日期时间，并且获得最早的浏览日期"
  [newsid]
  (get-psql-timestamp-from-timestamp
   (str (apply min (map get-unix-timestamp-from-psql-timestamp
                        (map :visit_timestamp
                             (db/query
                              ["select visit_timestamp from news where news_id = ?" newsid])))))))

;;(get-oldestvisit-for-news 100648389)

;;(map (fn [newsid]
;;       (get-oldestvisit-for-news newsid))
;;     (get-visit-less-post-news))

(defn fix-visit-older-than-post
  "因为原始的数据信息中存在噪声，有一些记录中的浏览时间会在发布时间之前，所以现在要将发布时间也换成最早的浏览时间，和发布时间为NULL时一样处理"
  []
  (doall
   (map (fn [newsid]
          (let [oldestvisit (get-oldestvisit-for-news newsid)]
            (println "newsid: " newsid)
            (db/update! :news
                        {:post_timestamp oldestvisit}
                        ["news_id=?" newsid])))
        (get-visit-less-post-news))))

;;samples to use create! function
;;(map (fn [eachline]
;;       (apply create! (parsedata eachline)))
;;     (initdata "data/test.txt"))
;;(apply create! (parsedata (nth (initdata "data/test.txt") 3)))

