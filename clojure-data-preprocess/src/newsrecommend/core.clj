(ns newsrecommend.core
  (:require [newsrecommend.data.preprocessdata :as preprocess]
            [newsrecommend.data.stepone :as stepone]
            [newsrecommend.data.steptwo :as steptwo]
            [newsrecommend.data.algorithm :as algo])
  (:gen-class))

(defn setup-data
  "将文件中的数据放入psql中"
  []
  (doall (map (fn [eachline]
                (apply preprocess/create! (preprocess/parsedata eachline)))
              (preprocess/initdata "data/data.txt")))
  (println "setupdate finished"))

(defn setup-posttime
  "为所有发布如期为NULL的新闻按最早的浏览时间近似作为发布日期，此函数是用来构造存储这个最早浏览日期的psql数据表"
  []
  (doall (map (fn [eachline]
                (println (apply preprocess/setup-oldestvisit (preprocess/parsedata eachline))))
              (preprocess/initdata "data/data.txt"))))

(defn get-click-old-news-user
  "获得会重复点击新闻的用户"
  []
    (let [click-old-users (filter identity (map stepone/click-old-news-user (stepone/get-distinct-user)))]
    (println click-old-users)
    (println (count click-old-users))))

(defn get-click-old-news-oneday
  "获取会点击一天以前旧新闻的用户"
  []
  (doall (map (fn [userid]
                ;;(println userid)
                (println (stepone/get-click-old-news-with-days userid 1)))
              (stepone/get-distinct-user))))

(defn setup-interval
  "为所有新闻用户记录添加以天为单位的浏览时间与发布时间间隔"
  []
  (doall
   (map (fn [newsid]
          (println (stepone/setup-interval-days newsid)))
        (stepone/get-distinct-news))))

(defn draw-click-trend
  "画出点击量随日期推移的变化趋势"
  [days]
  (stepone/get-trendchart-within-days days))

(defn steup-trainpositive
  "设置训练用的正例，传入1表示选取用户的倒数第二次浏览记录作为训练正例，传入0表示倒数第一条"
  [index]
  (steptwo/generate-train-positive index))

(defn setup-trainnegative
  "设置训练用的负例，负例是所有用户没有浏览过的新闻对应该使用10000*6183-116225条记录"
  []
  (steptwo/generate-train-negative))

(defn setup-news-cities
  []
  (steptwo/get-city-for-news))

(defn -main
  "just for fun"
  [& args]
  ;;(setup-posttime)
  ;;(setup-data)
  ;;(preprocess/fix-visit-older-than-post)
  ;;(setup-interval)
  ;;(draw-click-trend 50)
  ;;(steup-trainpositive 1)
  ;;(doall (steptwo/get-not-visit-for-user 5218791))
  ;;(algo/rand-square-mat 10000 6183)
  ;;(println (algo/big-vector-mul (algo/big-vector 10000) (algo/big-vector 10000)))
  ;;(setup-news-cities)
  (println "cleantha"))
