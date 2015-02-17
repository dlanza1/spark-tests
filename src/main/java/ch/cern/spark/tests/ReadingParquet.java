package ch.cern.spark.tests;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;


public class ReadingParquet {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkTest");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaSQLContext sqlContext = new JavaSQLContext(sc);
		
		JavaSchemaRDD parquetFile = sqlContext.parquetFile(
				"hdfs://itrac925.cern.ch:8020/user/lhclog/data_numeric_small_reduce/516003e7-33ad-452c-b195-a850073a5dae.parquet");
		
		//Parquet files can also be registered as tables and then used in SQL statements.
		parquetFile.registerTempTable("parquetFile");
		
		JavaSchemaRDD values = sqlContext.sql("SELECT value FROM parquetFile LIMIT 10");
		
		List<String> stringsWithValues = values.map(new Function<Row, String>() {
		  public String call(Row row) {
		    return "Value: " + row.getString(0);
		  }
		}).collect();
		
		for (String string : stringsWithValues) {
			System.out.println(string);
		}
		
	}
	
}