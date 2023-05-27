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
    

