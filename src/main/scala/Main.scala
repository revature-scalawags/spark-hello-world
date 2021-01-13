import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("hello world").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    helloSQL(spark)
    spark.stop()
  }

  def helloSQL(spark: SparkSession) {
    import spark.implicits._
    val jsonfile = spark.read.option("multiline", "true").json("/datalake/people.json").cache()
    jsonfile.show()
    jsonfile.printSchema()
    jsonfile.select($"name", $"age" + 10).show()
    jsonfile.select(jsonfile("name.first"), jsonfile("age") + 20).show()
    jsonfile.groupBy("eyeColor").count().show()
    jsonfile.select(functions.round($"age", -1)).show()

    // What is the average age by eye color for people with first names of length < 6
    val weirdQuery = jsonfile.filter(functions.length($"name.first") < 6)
      .groupBy("eyeColor")
      .agg(functions.avg("age"))

    weirdQuery.show()
    weirdQuery.explain(true)

    val peopleDataSet = jsonfile.as[Person]
    peopleDataSet.filter(person => person.name.first.length < 6).show()

    val weirdQuery2 = peopleDataSet.filter(_.name.first.length < 6)
      .map(person => s"${person.name.first} ${person.name.last}")

    weirdQuery2.select(weirdQuery2("value").alias("Full Name")).show()
    
    jsonfile.write.partitionBy("eyeColor").parquet("/datawarehouse/people.parquet")
    val peopleParquet = spark.read.parquet("/datawarehouse/people.parquet")
    peopleParquet.show()

    spark.createDataset(List(Name("Mehrab", "Rahman"), Name("Rahman", "Mehrab")))
      .createOrReplaceTempView("names")
    
    println(spark.sql("SELECT * FROM names").rdd.toDebugString)
  }

  def helloWorld(spark: SparkSession) {
    import spark.implicits._
    val file = spark.read.textFile("/tmp/logfile.txt").cache()
    val split = file.map(line => line.split(","))
    split.foreach(i => println(i))
  }

  case class Person(_id: String, index: Long, age: Long, eyeColor: String, name: Name, phone: String, address: String) {}
  case class Name(first: String, last: String) {}
}