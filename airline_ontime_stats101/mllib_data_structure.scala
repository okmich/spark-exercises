// Data structures and Linear Algebra
//==============================
//Why Linear Algebra
//Linear algebra operation

//Matrix
//A matrix of dimension n,m has n rows and m columns
//Transpose a matrix M with dimension n,m will result in another matrix with dimension m,n
//A scalar is a constant in the world of linear
//a scalar multiplication is an element-wise multiplication of a matrix
//identity matrix is a matrix such that all element M(1,j) where i=j is 1 otherwise is 0
//Any multiplication of a matrix A with an identity matrix will always result in A 
//the inverse of a matrix A is such that result of dividing the matrix by an indentity matrix


//Vector
//Generally is a matrix with either 1 column or 1 row.
//A row vector of size n is a matrix such that the dimension is n x 1
//A column vector of size n is a matrix such that the dimension is 1 x n



//ML vs MlLib
//Mllib is based on RDD while Ml is based on DataFrame



//Sparse and Dense Vectors
//A dense vector contains its relevant values
//A sparse vector is a vectors that contains fews of its relevant values with other as zeros
import org.apache.spark.mllib.linalg.Vectors
val sparseVector = Vectors.sparse(100, Seq((3, 4.2), (7, 2.1)))
val sparseVector2 = Vectors.sparse(100, Array(3,7), Array(4.2, 2.1))


val denseVector = Vectors.dense(Array(1.0, 2.0))
val seq : Seq[Double] = (1 to 20).map(_.toDouble)
val denseVector = Vectors.dense(0.0, seq : _*)

//Sparse and Dense Matrices
//A dense matrix contains its relevant values
//A sparse matrix is a matrix that contains fews of its relevant values with other as zeros

import org.apache.spark.ml.linalg.{Matrices, SparseMatrix, DenseMatrix}
val sparseMat = Matrices.sparse(3, 3, Array(0, 3 , 6, 9),  Array(1 , 3), Array(4.2, 2.1)))
val sparseMatr2 = Matrices.sparse(100, Array(3,7), Array(4.2, 2.1))


val denseMatrix = Matrices.dense(3, 3, Array(1,4, 2, 3,4,5,6,7,8))

val seq : Seq[Double] = (1 to 20).map(_.toDouble)
val denseVector = Matrices.dense(0.0, seq : _*)

//DISTRIBUTED STRUCTURE
//vectors are not distributed
//Spark ML has distributed matrix
	-	RowMatrix
	- 	IndexedRowMatrix
	- 	CoordinatedMatrix
	- 	BlockMatrix
