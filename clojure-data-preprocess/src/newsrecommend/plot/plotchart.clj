(ns newsrecommend.plot.plotchart
  (:use [clojure.core.matrix :only [transpose]]
        [incanter.charts :only [scatter-plot xy-plot add-lines add-points]]
        [incanter.stats :only [linear-model quantile-normal]]
        [incanter.core :only [view sel sqrt matrix]])
  (:require [clatrix.core :as cl]))

(def X (cl/matrix [1 2 3 4 5
                   6 7 8 9 10]))

(def Y (cl/matrix [112 90 88 79 70
                   69 50 47 35 12]))

(def linear-samp-scatter
  (scatter-plot X Y))

(defn plot-scatter []
  (view linear-samp-scatter))

(def samp-linear-model
  (linear-model Y X))

(defn plot-model []
  (view linear-samp-scatter))

(defn plot-trend-chart
  [x-axis y-axis]
  (view (add-points (xy-plot x-axis y-axis) x-axis y-axis)))

;;(plot-trend-chart X Y)

