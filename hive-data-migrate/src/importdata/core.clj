(ns importdata.core
  (:require [importdata.data :as data])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;;(println (data/importnews))
  ;;(data/write-file (data/importnews) "./newsdata.txt")
  ;;(data/write-file (data/importoldestvisit) "./oldestvisit.txt")
  ;;(data/write-file (data/importoneday) "./oneday.txt")
  ;;(data/write-file (data/importnewscity) "./newscity.txt")
  ;;(data/write-file (data/importvisitcount) "./visitcount.txt")
  ;;(data/write-file (data/importdistinct-newscontent) "./distinctnewscontent.txt")
  ;;(data/write-file (data/importdistinct-newstitle) "./distinctnewstitle.txt")
  ;;(println (data/get-distinct-userid))
  ;;(println (data/importnews-for-specific-user 75))
  ;;(println (data/importnews-for-users))
  ;;(println (count (data/get-distinct-userid)))
  ;;(println (count (data/importnews-for-users)))
  ;;为每一个用户生成一个其看过的所有新闻内容的整合用来进行用户兴趣提取和相似度计算
  ;;(data/write-file (data/importnews-for-users) "./newscontentsforusers")
  ;;(data/write-file (data/importnews-for-users) "./newscontentsforusers-sideeffect")
  ;;(println (data/importnewstitle-for-specific-user 75))
  ;;为每一个用户生成一个其看过的所有新闻标题的整合用来进行用户兴趣提取和相似度计算
  (data/write-file (data/importnewstitle-for-users) "./newstitleforusers"))
