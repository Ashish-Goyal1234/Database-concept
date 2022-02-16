package base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class BaseClass {

	public static String path;
	public static Properties prop;
	public static String propFile;
	public static InputStream input;

	static {
		path = System.getProperty("user.dir");
		prop = new Properties();
		propFile = path + File.separator + "src" + File.separator + "test" + File.separator + "resources"
				+ File.separator + "database.properties";
		try {
			input = new FileInputStream(propFile);
		} catch (FileNotFoundException f) {
			f.printStackTrace();
		}
		try {
			prop.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Reading Properties file
	public static Properties readProperty() {
		try {
			input = new FileInputStream(propFile);
			prop.load(input);
		} catch (Exception e) {
			e.printStackTrace();
			e.getMessage();
		}
		return prop;
	}

	// Fetching a value from other than default properties file.
	public static Properties readProperty(String fileName) {
		try {
			String fpath = path + File.separator + "src" + File.separator + "test" + File.separator + "resources"
					+ File.separator + fileName + ".properties";
			input = new FileInputStream(fpath);
			prop.load(input);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return prop;
	}

	// Fetching property value for different property file directly.
	public static String readProperty(String fileName, String property) {
		String propertValue = null;
		try {
			propertValue = readProperty(fileName).getProperty(property);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return propertValue;
	}
	
	// Get Integer value from properties file
	public static int readPropertyInteger(String prop) {
		int propertyValue = 0;
		try{
			propertyValue = Integer.parseInt(readProperty().getProperty(prop));
		}catch(Exception e){
			e.printStackTrace();
		}
		return propertyValue;
	}
	
	
	// Method is used to connect my sql
	public static Connection getMySQLConnection(String env) throws SQLException{
		Connection con = null;
		String hostName = null;
		if(env.equalsIgnoreCase("STAGE")){
			hostName = readProperty().getProperty("mySqlHostName-STAGE");
		}else if(env.equalsIgnoreCase("Prod")){
			hostName = readProperty().getProperty("mySqlHostName-PROD");
		}else if(env.equalsIgnoreCase("QA")){
			hostName = readProperty().getProperty("mySqlHostName-QA");
		}else if(env.equalsIgnoreCase("dev")){
			hostName = readProperty().getProperty("mySqlHostName-DEV");
		}
		try{
	        Class.forName("com.mysql.jdbc.Driver");
	        System.out.println("Driver loaded");
	         con = DriverManager.getConnection(hostName, readProperty().getProperty("username"), readProperty().getProperty("password"));
	        System.out.println("Successfully connected to mysql database");
		}catch(Exception e){
			e.printStackTrace();
			con.close();
		}
		return con;
	}
	
	

}
