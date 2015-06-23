(ns newsrecommend.data.steptwo
  (:require [clojure.java.jdbc :as jdbc]
            [newsrecommend.dbutil.dbmanipulate :as db]
            [newsrecommend.data.preprocessdata :as process]
            [newsrecommend.data.stepone :as stepone]
            [newsrecommend.data.city :as city])
  (:import (java.text SimpleDateFormat)
           (java.util Date)
           (java.sql Timestamp)
           (java.util.regex Pattern)))

(defn get-nth-last-news
  "为用户获得倒数第n次浏览过的新闻，如果传入的nth参数是1，就是倒数第二次的浏览记录，是按照浏览时间来排序的，后面选取最近浏览过的当做训练数据集的正例"
  [userid index]
  (let [visit_time (process/get-psql-timestamp-from-timestamp
                    (str (nth (sort > (map process/get-unix-timestamp-from-psql-timestamp
                                           (map :visit_timestamp
                                                (db/query
                                                 ["select visit_timestamp from news where user_id = ?" userid])))) index)))]
    (nth (db/query
          ["select * from news where visit_timestamp = ? and user_id = ?" visit_time userid]) 0)))

;;(get-nth-last-news 5218791 1)

(defn generate-train-positive
  "生成训练用的正例，也就是所有用户倒数第n次点击过的记录，共10000条，其中选取哪一条记录作为训练数据是由参数决定，传入1即为倒数第二条"
  [index]
  (doall
   (map (fn [userid]
          (let [positive (get-nth-last-news userid index)]
            (println positive)
            (db/insert! :trainpositive
                        positive)))
        (stepone/get-distinct-user))))

(defn is-user-notvisited-news?
  "判断给定用户是否浏览过了给定的新闻，如果已经浏览过了就是false，如果没有浏览过就是true"
  [userid newsid]
  (empty?
   (db/query
    ["select * from news where user_id = ? and news_id = ?" userid newsid])))

(defn get-news-info
  "根据新闻id来获得新闻的发布时间，新闻标题，新闻内容，是要插入到数据库中作为训练的负例的"
  [newsid]
  (let [first-record (nth (db/query
                           ["select * from news where news_id = ?" newsid]) 0)]
    {:post_timestamp (:post_timestamp first-record)
     :news_title (:news_title first-record)
     :news_content (:news_content first-record)}))

;;(println (get-news-info 100648598))

(defn get-not-visit-for-user
  "为给定用户获得所有没有浏览过的新闻id，但是为所有用户获得未浏览的新闻记录计算量太大了，这一步暂时搁置"
  [userid]
  (map (fn [newsid]
         (if (is-user-notvisited-news? userid newsid)
           (let [negative (merge {:user_id userid :news_id newsid}
                                 (get-news-info newsid))]
             (println (db/insert! :trainnegative
                                  negative)))))
       (stepone/get-distinct-news)))

(defn generate-train-negative
  "为所有用户找到未浏览过的新闻作为训练负例"
  []
  (doall
   (map (fn [userid]
          (get-not-visit-for-user userid))
        (stepone/get-distinct-user))))

(defn news-contains-city
  "得到新闻内容中包含的城市信息，分别将包含的城市的序号以x-x-x的形式存进psql中"
  [newsid]
  (let [news-info (get-news-info newsid)
        news-content (:news_content news-info)]
    (clojure.string/join "-"
                         (filter identity
                                 (map (fn [city-pair]
                                        (if (re-find (re-pattern (nth city-pair 0)) news-content)
                                          (nth city-pair 1)))
                                      city/city-map)))))

(defn get-city-for-news
  "为所有新闻统计其新闻内容中包含的城市名称"
  []
  (doall
   (map (fn [newsid]
          (let [cities (news-contains-city newsid)]
            (println
             (db/insert! :newscity
                         {:news_id newsid
                          :cities cities}))))
        (stepone/get-distinct-news))))

;;(news-contains-city 100648598)
;;(news-contains-city 100650277)
;;(if (re-find (re-pattern "香港") (:news_content (get-news-info 100648598)))
;;  :true
;;  :false)
;;(if (re-find (re-pattern "北京") (:news_content (get-news-info 100648598)))
;;  :true
;;  :false)
;;(get city/city-map "浙江")

