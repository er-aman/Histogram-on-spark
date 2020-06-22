import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main ( args: Array[ String ] ): Unit = {
    val conf = new SparkConf().setAppName("Histogram")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    var graph = sc.textFile(args(0)).map(line => {val a = line.split(","); var adjacent = new ListBuffer[Long]()
      for (i <- 1 to (a.length - 1))
        adjacent += a(i).toLong
      var adjacent_lis = adjacent.toList;(a(0).toLong, a(0).toLong, adjacent_lis)
    })
    var second_graph = graph.map(g => (g._1,g));

    for(i <- 1 to 5){
      graph = graph.flatMap(map => map match{ case (i, j, kl) => (i,j) :: kl.map(a => (a,j) ) } )
        .reduceByKey((x, y) => (if (x >= y) y else x)).join(second_graph).map(l => (l._2._2._2, l._2._1, l._2._2._3))
    }
    val size = graph.map(g => (g._2, 1))
    val res = size.reduceByKey((m, v) => (m + v))
      .collect()
      .foreach(println)
    sc.stop()
  }
}
