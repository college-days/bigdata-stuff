import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg._
import java.util.Arrays
import scala.collection.mutable.ArrayBuffer

//inspired https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/linalg/Matrices.scala

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

eye(3)

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

diag(Vectors.dense(1, 2, 3))
diag(Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
diag(new SparseVector(3, Array(0, 2), Array(1.0, 3.0)))

//http://spark.apache.org/docs/1.0.2/mllib-basics.html
//spark中的sparse vector, 第一个参数表示向量的长度，后面一个参数是一个数组表示存在非零值的下标，最后一个参数也是一个数组，依次表示这些非零值位置的非零数值

/* content in svd.txt
0.5 1.0
2.0 3.0
4.0 5.0
*/

val originSvd = sc.textFile("hdfs://localhost:9000/svd.txt")
val rows = originSvd.map {
    line =>
    val values = line.split(' ').map(_.toDouble)
    //values
    Vectors.dense(values)
}

val mat = new RowMatrix(rows)
val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(mat.numCols().toInt, computeU = true)
val U: RowMatrix = svd.U // The U factor is a RowMatrix.
val s: Vector = svd.s // The singular values are stored in a local dense vector.
val V: Matrix = svd.V // The V factor is a local dense matrix.

U.rows.collect //=> to get arrays of the RDD

val matrixU = new RowMatrix(U.rows)
val matrixV = V
val matrixS = diag(svd.s)

matrixU.multiply(matrixS).multiply(matrixV).rows.collect

val ttt = ones(3, 3)
ttt.numRows
ttt.numCols
val tttArray = ttt.toArray
tttArray(1) = 3.0
new DenseMatrix(3, 3, tttArray)

/*
Array(1.0, 3.0, 2.0, 1.0, 1.0, 0.0, 1.0, 1.0, 1.0)

new DenseMatrix(3, 3, tttArray)

array to matrix => 是按照优先补列位置的顺序构造矩阵的

1.0  1.0  1.0
3.0  1.0  1.0
2.0  0.0  1.0
*/

val ttt = (0,0,1.0)
ttt._1 //=>0
ttt._2 //=>0
ttt._3 //=>1.0

//from https://gist.github.com/vrilleup/9e0613175fab101ac7cd

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.{SparkConf, SparkContext}
 
// To use the latest sparse SVD implementation, please build your spark-assembly after this
// change: https://github.com/apache/spark/pull/1378
 
// Input tsv with 3 fields: rowIndex(Long), columnIndex(Long), weight(Double), indices start with 0
// Assume the number of rows is larger than the number of columns, and the number of columns is
// smaller than Int.MaxValue
// sc is a SparkContext defined in the job
val inputData = sc.textFile("hdfs://localhost:9000/user/zjh/svd.txt").map{ line =>
  val parts = line.split("\t")
  (parts(0).toLong, parts(1).toInt, parts(2).toDouble)
}
 
// Number of columns
val nCol = inputData.map(_._2).distinct().count().toInt
 
inputData.groupBy(_._1) //=> Array[(Long, Iterable[(Long, Int, Double)])] = Array((0,ArrayBuffer((0,0,1.0), (0,2,3.0))), (2,ArrayBuffer((2,1,1.0), (2,2,1.0))), (1,ArrayBuffer((1,2,5.0))))

ArrayBuffer((0,0,1.0), (0,2,3.0)).map(e => (e._2, e._3)) //=> ArrayBuffer((0,1.0), (2,3.0))
ArrayBuffer((0,0,1.0), (0,2,3.0)).map(e => (e._2, e._3)).unzip //=> (ArrayBuffer(0, 2),ArrayBuffer(1.0, 3.0))

// Construct rows of the RowMatrix
val dataRows = inputData.groupBy(_._1).map[(Long, Vector)]{ row =>
  val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
  (row._1, new SparseVector(nCol, indices.toArray, values.toArray))
}
 
val pre = inputData.groupBy(_._1).collect.sortWith(_._1 < _._1)
val preRDD = sc.parallelize(pre)

val dataRows = preRDD.map[(Long, Vector)]{ row =>
  val (indices, values) = row._2.map(e => (e._2, e._3)).unzip
  (row._1, new SparseVector(nCol, indices.toArray, values.toArray))
}

// Compute 20 largest singular values and corresponding singular vectors
val svd = new RowMatrix(dataRows.map(_._2).persist()).computeSVD(nCol, computeU = true)
 
val U: RowMatrix = svd.U // The U factor is a RowMatrix.
val s: Vector = svd.s // The singular values are stored in a local dense vector.
val V: Matrix = svd.V // The V factor is a local dense matrix.

// Write results to hdfs
val V = svd.V.toArray.grouped(svd.V.numRows).toList.transpose
V.map(list => list.toArray)
V.map(list => list.toArray).toArray

//http://alvinalexander.com/scala/how-merge-scala-lists-concatenate

V.reduce(List.concat(_, _))
V.reduce(List.concat(_, _)).toArray
new DenseMatrix(3, 3, V.reduce(List.concat(_, _)).toArray)

val matrixU = new RowMatrix(U.rows)
//val matrixV = V
val matrixS = diag(svd.s)

matrixU.multiply(matrixS).multiply(new DenseMatrix(3, 3, V.reduce(List.concat(_, _)).toArray)
).rows.collect

sc.makeRDD(V, 1).zipWithIndex()
  .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with column index
  .saveAsTextFile("hdfs://...output/right_singular_vectors")
 
svd.U.rows.map(row => row.toArray).zip(dataRows.map(_._1))
  .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with row index
  .saveAsTextFile("hdfs://...output/left_singular_vectors")
 
sc.makeRDD(svd.s.toArray, 1)
  .saveAsTextFile("hdfs://...output/singular_values")
