package parquetFileConcept;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.testng.Assert;
import org.testng.annotations.Test;

import base.BaseClass;
import base.SparkSessionWrapper;

public class CreatingParquetFile extends BaseClass implements SparkSessionWrapper {

	String path = readProperty().getProperty("parquetpath") + File.separator;

	public Boolean checkIfFileExists(String folderPath) {
		File file = new File(folderPath);
		Boolean status = false;
		try {
			status = file.exists();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return status;
	}

	public enum parquetType {
		stage_, prod_
	}
	
	@Test(priority=0, description = "Removing a Parquet file")
	public void removingAParquetFile(){
		File file = new File(path);
		try{
			if(file.exists() && file.isDirectory()){
				FileUtils.cleanDirectory(file);
			}
			
		}catch(Exception e){
			
		}
	}
	

	@Test(priority=1, description = "Creating a Parquet file for employees table")
	public void creatingParquetFile() {
		spark.sparkContext().setLogLevel("Error");
		try {
			if(!checkIfFileExists(path+parquetType.stage_+"ListOFEmployees"+".parquet")){
				Dataset<Row> query = spark.read().format("jdbc")
						.option("username", "ashish")
						.option("password","pass@123")
						.option("url", "jdbc:mysql://localhost:3306")
						.option("query","select * from classicmodels.employees").load();
				if(query != null){
					query.write().parquet(path+parquetType.stage_+"ListOFEmployees"+".parquet");
				}
			}
		} catch (Exception e) {
				e.printStackTrace();
				Assert.fail("Failed to create parquet" + e);
		}
	}
	

	@Test(priority = 2)
	public void readingParquetFile(){
		spark.sparkContext().setLogLevel("Error");
		Dataset<Row> readParquet = spark.read().parquet(path+parquetType.stage_+"ListOFEmployees"+".parquet");
	
		readParquet.show();  //  Show the table.
	//	readParquet.printSchema();   // Shows the schema of the table.
	//	readParquet.count();   // Shows number of rows present in parquet file.
	//	readParquet.columns();  //Used to return nu,ber of columns present in parquet file.
		/*
		    Syntax :
		    String[] columns = readParquet.columns();
		    for(String cc : columns){
		    		System.out.println("Columns : " + cc);
		    }
		 */
	//	 readParquet.filter("jobTitle == 'Sales Rep'").show(false);//Used to filter out data in parquet file. Syntax columnName == value
	//	 readParquet.filter(readParquet.col("firstName").startsWith("A")).show();
		 readParquet.drop("email").show(); // used to tempropry drop the column
	}

}
