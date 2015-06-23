package org.give.playspark

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zjh on 14-10-4.
 * spark-submit --class org.give.newsspark.SingularDecomp --master local target/news-spark-1.0-SNAPSHOT.jar hdfs://localhost:9000/user/zjh/svd.txt hdfs://localhost:9000/user/zjh/matrixu.txt hdfs://localhost:9000/user/zjh/matrixs.txt hdfs://localhost:9000/user/zjh/matrixv.txt
 */
object SingularDecomp {
    def main(args: Array[String]){
        def zeros(numRows: Int, numCols: Int): Matrix =
            new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))

        def ones(numRows: Int, numCols: Int): Matrix =
            new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))

        def index(i: Int, j: Int, numRows: Int): Int = i + numRows * j

        def eye(n: Int): Matrix = {
            val identity = zeros(n, n)
            val identityArray = identity.toArray
            val rows = identity.numRows
            val cols = identity.numCols
            var i = 0
            while (i < n){
                //identity.update(i, i, 1.0)
                identityArray(index(i, i, rows)) = 1.0
                i += 1
            }
            //identity
            new DenseMatrix(rows, cols, identityArray)
        }

        def diag(vector: Vector): Matrix = {
            val n = vector.size
            val matrix = eye(n)
            val matrixArray = matrix.toArray
            val values = vector.toArray
            var i = 0
            while (i < n) {
                //matrix.update(i, i, values(i))
                matrixArray(index(i, i, n)) = values(i)
                i += 1
            }
            //matrix
            new DenseMatrix(n, n, matrixArray)
        }


        val sc = new SparkContext(new SparkConf().setAppName("Singular Decomposition"))

        //spark-submit --class org.give.newsspark.SingularDecomp --master local target/news-spark-1.0-SNAPSHOT.jar hdfs://localhost:9000/user/zjh/svd.txt
        /*
        svd data
        0	0	1
        0	2	3
        1	2	5
        2	1	1
        2	2	1
        */
        val inputData = sc.textFile(args(0)).map{ line =>
            val parts = line.split("\t")
            (parts(0).toLong, parts(1).toInt, parts(2).toDouble)
        }

        val nCol = inputData.map(_._2).distinct().count().toInt

        val pre = inputData.groupBy(_._1).collect.sortWith(_._1 < _._1)
        val preRDD = sc.parallelize(pre)

        val dataRows = preRDD.map[(Long, Vector)]{ row =>
            val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
            (row._1, new SparseVector(nCol, indices.toArray, values.toArray))
        }

        val svd = new RowMatrix(dataRows.map(_._2).persist()).computeSVD(nCol, computeU = true)

        val U: RowMatrix = svd.U // The U factor is a RowMatrix.
        val s: Vector = svd.s // The singular values are stored in a local dense vector.
        val V: Matrix = svd.V // The V factor is a local dense matrix.

        val transV = svd.V.toArray.grouped(svd.V.numRows).toList.transpose
        val matrixU = new RowMatrix(U.rows)
        val matrixV = new DenseMatrix(V.numRows, V.numCols, transV.reduce(List.concat(_, _)).toArray)
        val matrixS = diag(svd.s)

        //just validate the decomposition result
        val origin = matrixU.multiply(matrixS).multiply(matrixV).rows.collect
        //println(origin)
        //origin
        println(origin.mkString(" "))
        /*
        origin => [1.0000000000000007,1.1102230246251565E-16,3.0000000000000018] [7.771561172376096E-16,-4.440892098500626E-16,5.0000000000000036] [5.551115123125783E-16,1.0,1.0000000000000004]
        just like the data in disk
        1 0 3
        0 0 5
        0 1 1
         */

        //makeRDD的第二个参数可以指定输出文件的个数，slices其实是说在集群中人物的分片个数，之前mr和spark输出结果多个结果就是因为最终还是由多个node来计算得到的结果，如果需要一个结果文件，那么最后一个人物只能由一个node来reduce
        svd.U.rows.map(row => row.toArray).zip(dataRows.map(_._1))
            .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with row index
            .saveAsTextFile(args(1))
        sc.makeRDD(svd.s.toArray, 1).saveAsTextFile(args(2))
        sc.makeRDD(transV, 1).zipWithIndex()
            .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with column index
            .saveAsTextFile(args(3))
    }
}
