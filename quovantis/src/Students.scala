
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j._

object Students {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Student Analysis")
      .master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp").getOrCreate()

    /**
     * Getting the datasets from local
     */

    val marks = spark.read.option("inferSchema", "true").option("header", "true").csv("C:/Users/kanik/OneDrive/Desktop/quovantis/datasets/marks.csv")
    val students = spark.read.option("inferSchema", "true").option("header", "true").csv("C:/Users/kanik/OneDrive/Desktop/quovantis/datasets/students.csv")

    /**
     * Creating transformed dataset with student percentage and their respective grades
     */
    val results = marks.join(students, Seq("student_id"), "left")
      .withColumn("total (out of 400)", round((col("english") + col("hindi") + col("maths") + col("science")), 2))
      .withColumn("percentage", round(((col("total (out of 400)") / 400) * 100), 2))
      .withColumn("grade", when(col("percentage") > lit(90.0), lit('A'))
        .when(col("percentage").between(lit(80.0), lit(90.0)), lit('B'))
        .when(col("percentage").between(lit(70.0), lit(80.0)), lit('C'))
        .when(col("percentage").between(lit(40.0), lit(70.0)), lit('D'))
        .otherwise(lit('F'))).select("student_id", "name", "class", "section", "percentage", "grade").cache()

    /**
     *  1. Saving "total of each student and grade associated to csv"
     */

    results.select("student_id", "name", "percentage", "grade").coalesce(1)
      .write.format("csv").mode("overwrite").option("header", "true").save("C:/Users/kanik/OneDrive/Desktop/quovantis/output/query_1")

    /**
     *   2. No of students passing and failing
     */

    results.withColumn("result", when(col("grade") === lit('F'), "failed").otherwise("passed")).groupBy("class", "section", "result").count().coalesce(1)
      .write.format("csv").mode("overwrite").option("header", "true").save("C:/Users/kanik/OneDrive/Desktop/quovantis/output/query_2")

    /**
     * 3. Class wise topper names
     */

    val dr = Window.partitionBy("class", "section").orderBy(desc("percentage"))

    results.withColumn("rank", dense_rank().over(dr)).where("rank = 1").select("class", "section", "name").coalesce(1)
      .write.format("csv").mode("overwrite").option("header", "true").save("C:/Users/kanik/OneDrive/Desktop/quovantis/output/query_3")

    /**
     *  4. Average percentage of students
     */

    results.agg(round(avg("percentage"), 2)).coalesce(1)
      .write.format("csv").mode("overwrite").option("header", "true").save("C:/Users/kanik/OneDrive/Desktop/quovantis/output/query_4")

    /**
     *  5. Names of Students failing in class 5-C
     *
     */

    results.filter(col("section") === 'C' && col("grade") === 'F').select("name").coalesce(1)
      .write.format("csv").mode("overwrite").option("header", "true").save("C:/Users/kanik/OneDrive/Desktop/quovantis/output/query_5")
  }
}