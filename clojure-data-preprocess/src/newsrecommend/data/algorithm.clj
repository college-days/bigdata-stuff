(ns newsrecommend.data.algorithm
  (:use [incanter.core]))

(defn rand-square-mat
  "Generates a random matrix of size n x n 因为需要产生一个10000*6183大小的矩阵，jvm内存溢出了，内存里存不下这么大的矩阵，只能改用mahout了"
  [rows cols]
  ;; this won't work
  (matrix (partition
           cols (repeatedly
                 (* rows cols) #(rand-int 100)))))

(defn big-vector
  [length]
  (matrix (repeatedly length #(rand-int 100))))

(defn big-vector-mul
  [vector1 vector2]
  (mmult (trans vector1) vector2))

;;(big-vector 10)
;;(big-vector-mul (big-vector 10) (big-vector 10))
;;(rand-square-mat 4 3)




