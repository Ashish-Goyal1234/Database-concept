package base;

import org.apache.spark.sql.SparkSession;

public interface SparkSessionWrapper {
	
	SparkSession spark = SparkSession.builder().master("local").appName("create a parquet and read from it").getOrCreate();

}
