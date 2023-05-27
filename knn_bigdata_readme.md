the spark project done by us is Knn in  scala language
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
versions used:
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
java -version
      openjdk version "11.0.16" 2022-07-19
      OpenJDK Runtime Environment (build 11.0.16+8-post-Ubuntu-0ubuntu122.04)
      OpenJDK 64-Bit Server VM (build 11.0.16+8-post-Ubuntu-0ubuntu122.04, mixed mode, sharing)

used spark version 3.4.0 hadoop 3
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-> The code for the project :

object knn_bigdata{
  def main(args: Array[String]){
	val rdd=sc.textFile("/home/mithra/Downloads/BD_project_dataset.txt")
	val s="Iris-setosa"
	val s1="Iris-versicolor"
	val s2="Iris-virginica"
	val p = rdd.map(x=>
	 if(x.contains(s)){
	(x,s)}
	else if(x.contains(s1)){
	(x,s1)}
	else{
	 (x,s2)}
	 )
	p.collect
	val d = p.map { case (x : String, y: String) =>
	if (x.contains(s)){
	(x.replace(s, ""), y)}
	else if (x.contains(s1)){
	(x.replace(s1, ""), y)}
	else{
	(x.replace(s2, ""), y)}
	}
	d.collect
	val j = d.map(x => x._1)
	j.collect
	val splitRDD = j.map(x => x.split(" "))
	splitRDD.collect
	val doubleRDD = splitRDD.map(x => x.map(_.toDouble))
	doubleRDD.collect
	var test1 = List(0.3,2.3,3.2,4.2)
	val rem=rdd.map(x=>x.replace(x.substring(0,16),""))
	rem.collect
	var achuth = List[Double]()
	import scala.math
	var sum : Double=0
	var n=0
	val data: Array[Array[Double]] = doubleRDD.collect()
	import java.io._
	for(x<-data){
	sum=0;
	n=0;
	for(y<-x){
	sum=sum+(y-test1(n))*(y-test1(n))
	n=n+1
	 }
	var dist : Double =math.sqrt(sum)
	achuth=achuth:+dist
	val file = new File("/home/mithra/Downloads/distances1")
	val bw = new BufferedWriter(new FileWriter(file))
	bw.write(achuth.mkString(","))
	bw.close()
	}
	val w=sc.textFile("/home/mithra/Downloads/distances1")
	w.collect
	val sanj = w.map(x => x.split(","))
	sanj.collect
	val varu = sanj.flatMap(x => x.map(_.toDouble))
	varu.collect
	rem.collect
	val remrepair=rem.repartition(3)
	val varurepair=varu.repartition(3)
	val comrepair=remrepair.zip(varurepair)
	val sortedarray = comrepair.repartition(4).sortBy(_._2)
	sortedarray.collect
	import scala.io.StdIn
	val k=30
	val topel=sortedarray.take(k)
	val toprdd=sc.parallelize(topel)
	val newtoprdd=toprdd.map(x=>(x._1,1))
	val reducedRDD = newtoprdd.reduceByKey(_+_)
	val redsort=reducedRDD.sortBy(_._2)
	var max = Int.MinValue
	for(x <-redsort.collect()){
	if(x._2 > max){
	max = x._2
	}
	}
	max
	for(x<-redsort.collect()){
	if(x._2 == max){
	val h = x._1
	print(h)
	}
	}	
	
	sc.stop()
    }
}
 
The above code is saved in a scala file and named as knn_bigdata.scala which is named  as the object defined in the code 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-> Explaination of code step by step
1)Set the correct path to your dataset file by modifying the line:
	val rdd = sc.textFile("/home/mithra/Downloads/BD_project_dataset.txt")
2)Customize the class labels you want to classify. In this code, the class labels are set as follows:
	val s = "Iris-setosa"
	val s1 = "Iris-versicolor"
	val s2 = "Iris-virginica"
3)Run the code using Apache Spark.
4)The code performs the following steps:
  1.Reads the dataset and maps each data point with its corresponding class label.
  2.Processes the data to remove the class label from each data point.
  3.Splits the data points into an RDD (Resilient Distributed Dataset).
  4.Converts the data points into a double RDD for numerical computations.
  5.Calculates the distances between the input data point and all other data points using the Euclidean distance formula.
  6.Writes the distances to a file (/home/mithra/Downloads/distances1).
  7.Reads the distances from the file and processes them.
  8.Repartitions and sorts the data based on the distances.
  9.Takes the top K nearest neighbors.
 10.Performs a reduce operation to count the occurrences of each class label.
 11.Finds the most frequent class label.
 12.Prints the predicted class label for the input data point.
5.Adjust the value of k based on your needs:
 val k = 30
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-> Steps to execute the above code in spark-shell

1) save the above code in the .scala file named as knn_bigdata.scala
2) execute command spark-shell on the terminal which runs the sparksession
3) load the file using :load command in the spark which is (:load knn_bigdata.scala)
4) it will compile the object knn_bigdata
5) run the file by executing = knn_bigdata.main(Array()) 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-> The series of commands after saving the code are
1)spark-shell
2):load knn_bigdata.scala
3) knn_bigdata.main(Array())



