package eg.edu.alexu.id40
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.desc
/**
 * @author ${zezomido78}
 */
object App {

  
  def main(args : Array[String]) {
    println( "Starting spark..." )
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")
    val SQL = true
    val spark = SparkSession
      .builder()
      .appName("BigData Lab3")
      .config(conf)
      .getOrCreate()

    try {


      val inputfile: String = "nasa_19950801.tsv"

      val input = spark.read.format("csv")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("nasa_19950801.tsv")
        input.createOrReplaceGlobalTempView("log_lines")

      val t1 = System.nanoTime
      val command: String = "comparison"
      println("With SQL ? : "+SQL)
      command match {
        case "count-all" =>
        // TODO count total number of records in the file
          val out = input.count()
          println(s"Input file has ${out} records ")
        case "code-filter" =>
            val out = input.filter("response == 200").count()
            println(s"Input file has ${out} records with response code = 200")
        // TODO Filter the file by response code, args(2), and print the total number of matching lines
        case "time-filter" =>
          val out = input.filter("time BETWEEN 807274014 AND 807283738").count()
          println(s"input file has ${out} records  between 807274014 AND 807283738")
        // TODO Filter by time range [from = args(2), to = args(3)], and print the total number of matching lines
        case "count-by-code" =>
              val out = input.groupBy("response").count().show()
        // TODO Group the lines by response code and count the number of records per group
        case "sum-bytes-by-code" =>
              input.groupBy("response").sum("bytes").show()
        // TODO Group the lines by response code and sum the total bytes per group
        case "avg-bytes-by-code" =>
        // TODO Group the liens by response code and calculate the average bytes per group
          input.groupBy("response").avg("bytes").show()
        case "top-host" =>
          val row = input.groupBy("host").count().orderBy(desc("count")).first()
          val host_name = row.getAs[String](0)
          val count = row.getAs[Long](1)
          println(s"The top host is ${host_name} with number of records= ${count}")
        // TODO print the host the largest number of lines and print the number of lines
        case "comparison" =>
        // TODO Given a specific time, calculate the number of lines per response code for the
        // entries that happened before that time, and once more for the lines that happened at or after
        // that time. Print them side-by-side in a tabular form.
          val df1 = input.filter("time < 807295758")
            .groupBy("response")
            .count()
            .withColumnRenamed("count","count_before")
          val df2 = input.filter("time >= 807295758")
                    .groupBy("response")
                    .count()
                    .withColumnRenamed("count","count_after")
           val dfTotal = df1.join(df2,"response")
            dfTotal.show()
      }
      val t2 = System.nanoTime
      println(s"Command '${command}' on file '${inputfile}' finished in ${(t2-t1)*1E-9} seconds")
    } finally {
      spark.stop
    }
  }

}
