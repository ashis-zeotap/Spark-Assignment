import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object Driver {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val schemaText = spark.sparkContext.wholeTextFiles("./src/main/resources/outputSchema.avsc").collect()(0)._2

    val inputPath1 = "test_data/sample1/part-00000-a4794630-9584-4152-b5ed-595fd7608322.avro"
    val inputPath2 = "test_data/sample2/part-00000-64940e90-dcc3-4d23-9a38-c7e1e1d24820.avro"
    val inputPath3 = "test_data/sample3/part-00000-ec4ac14a-4d9b-4675-b2a5-224784c22950.avro"
    val inputPath4 = "test_data/sample4/part-00000-a3521293-47b8-4b2c-983a-2b973319fd51.avro"
    val inputPath5 = "test_data/sample4/part-00001-a3521293-47b8-4b2c-983a-2b973319fd51.avro"

    val priority_matrix = spark.read.json("src/main/scala/priority.json")

    val df1 = spark.read.option("avroSchema", schemaText).avro(inputPath1)
    val df2 = spark.read.option("avroSchema", schemaText).avro(inputPath2)
    val df3 = spark.read.option("avroSchema", schemaText).avro(inputPath3)
    val df4 = spark.read.option("avroSchema", schemaText).avro(inputPath4)
    val df5 = spark.read.option("avroSchema", schemaText).avro(inputPath5)

    val df_4 = df1.toDF()
    val df_212 = df2.union(df3).toDF()
    val df_316 = df4.union(df5).toDF()

    val merge = df_4.join(df_212.select("Identifier"), "Identifier")
    var mergeFinal = merge.join(df_316.select("Identifier"), "Identifier")

    // Datapartner wise count for Age records, Gender records, ZipCode records
    val dpAge = mergeFinal.groupBy("Age_dpid").count()
    val dpGender = mergeFinal.groupBy("Gender_dpid").count()
    val dpZipCode = mergeFinal.groupBy("ZipCode_dpid").count()

    println(dpAge)
    print(dpGender)
    println(dpZipCode)

    // Applying a UDF for bucketing the ages into groups
    def age_groups = udf((age: Int) =>
      if (age <= 18) 18
      else if (age > 18 && age <= 25) 25
      else if (age > 25 && age <= 35) 35
      else if (age > 35 && age <= 45) 45
      else if (age > 45 && age <= 55) 55
      else if (age > 55 && age <= 65) 65
      else 75
    )

    val resultant_df = mergeFinal.withColumn("Age", age_groups(col("Age")))
    val ageCount = resultant_df.groupBy("Age").count()
    val genderCount = resultant_df.groupBy("Gender").count()

    println(ageCount)
    println(genderCount)
  }
}
