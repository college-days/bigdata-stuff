(ns newsrecommend.data.stepone
  (:require [clojure.java.jdbc :as jdbc]
            [newsrecommend.dbutil.dbmanipulate :as db]
            [newsrecommend.data.preprocessdata :as process]
            [newsrecommend.plot.plotchart :as pp])
  (:import (java.text SimpleDateFormat)
           (java.util Date)
           (java.sql Timestamp)))

(defn get-distinct-user
  "select all distinct userid
  return a list of distinct userid"
  []
  (map :user_id
       (db/query
        ["select distinct user_id from news"])))

;;get distinct userid
;;(class (first (get-distinct-user)))
;;(count (get-distinct-user))

(defn get-distinct-news
  "select all distinct newsid
  return a list of distinct newsid"
  []
  (map :news_id
       (db/query
        ["select distinct news_id from news"])))

;;get distance newsid
;;(class (first (get-distinct-news)))
;;(count (get-distinct-news))

(defn find-news-by-userid
  "
  按照userid搜索news
  param:
    userid news的userid 增加了type hint进行限定
  "
  [^Integer userid]
  (db/query["select * from news where user_id = ?" userid]))

(defn click-old-news-user
  "查找会点击已经点击过新闻的用户"
  [userid]
  (let [news-list (map :news_id (find-news-by-userid userid))]
  (if (not= (count news-list) (count (set news-list)))
    userid)))

(defn get-click-old-news-with-days
  "判断用户是否点击非当天的历史新闻，传入参数是给定的用户id以及时间差阈值"
  [userid days]
  (let [query-result (db/query
                      ["select id, news_id, visit_timestamp, post_timestamp from news where user_id = ?" userid])]
    (filter identity (map (fn [record]
                            (let [visit (:visit_timestamp record)
                                  post (:post_timestamp record)
                                  id (:id record)
                                  news_id (:news_id record)]
                              (if (process/is-interval-greater-than-days? visit post days)
                                (db/insert! :oneday
                                            {:user_id userid
                                             :news_id news_id}))))
                          query-result))))

(defn setup-interval-days
  "为给定新闻id的所有记录添加以天为单位的时间差"
  [newsid]
  (map (fn [record]
         (let [userid (:user_id record)
               newsid (:news_id record)
               visit (:visit_timestamp record)
               post (:post_timestamp record)
               interval (process/get-interval-days visit post)]
           (db/insert! :interval
                       {:user_id userid
                        :news_id newsid
                        :interval interval})))
       (db/query
        ["select user_id, news_id, visit_timestamp, post_timestamp from news where news_id = ?" newsid])))

(defn setup-interval-days-all-news
  []
  (map (fn [newsid]
         (setup-interval-days newsid))
       (get-distinct-news)))

(defn get-clickcount-in-someday
  "统计新闻点击次数随时间变化情况，当天、1天后、2天后…n天后点击数，这里传入的就是某一天，得到的是这一天的新闻点击数，其中传入1表示当天，2表示第二天"
  [day]
  (:count
   (nth
    (db/query
     ["select count(*) from interval where interval = ?" day]) 0)))

;;(get-clickcount-in-someday 1)

(defn get-trendchart-within-days
  "统计新闻点击次数随时间变化情况，当天、1天后、2天后…n天后点击数，这里传入的就是截止某一天的点击量变化趋势"
  [days]
  (let [day-seq (range 1 (+ days 1))
        clickcounts (map (fn [day]
                           (get-clickcount-in-someday day))
                         day-seq)]
    (pp/plot-trend-chart day-seq clickcounts)))

;;(get-trendchart-within-days 3)

;;(get-trendchart-within-days 1000)

;;(setup-interval-days 100648598)

;;(map (fn [record]
;;       (let [visit (:visit_timestamp record)
;;             post (:post_timestamp record)]
;;         (process/is-in-interval? visit post 2)))
;;     (db/query
;;      ["select visit_timestamp, post_timestamp from news where user_id = ?" 5218791]))
;;
;;(map (fn [record]
;;       (let [visit (:visit_timestamp record)
;;             post (:post_timestamp record)]
;;         (process/get-interval-days visit post)))
;;     (db/query
;;      ["select visit_timestamp, post_timestamp from news where user_id = ?" 5218791]))
;;(get-click-old-news-with-days 5218791 1)
;;(doall (map (fn [record]
;;              (let [visit (:visit_timestamp record)
;;                    post (:post_timestamp record)]
;;                (println (process/is-interval-greater-than-days? visit post 1))))
;;            (db/query
;;             ["select id, news_id, visit_timestamp, post_timestamp from news where user_id = ?" 5218791])))

