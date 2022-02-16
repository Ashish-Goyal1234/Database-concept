package mysqlConcept;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import base.BaseClass;

public class connectingMysql extends BaseClass {

	static Connection con = null;
	static String username = readProperty().getProperty("username");
	static String password = readProperty().getProperty("password");

	
	// Way 01
	@Test(priority=0, description = "command is to verify databses")
	    public void testShowDatabases() throws ClassNotFoundException {
		 ResultSet resultset= null;
		 Statement stmt = null;
			try{
	        Class.forName("com.mysql.cj.jdbc.Driver");
	        System.out.println("Driver loaded");
	         con = DriverManager.getConnection("jdbc:mysql://localhost:3306", username, password);
	        System.out.println("******************** Connected to MYSQL DB ***********************");
	         stmt = con.createStatement();
	         resultset = stmt.executeQuery("show databases;");
	        if(stmt.execute("show databases;")){
	        	resultset = stmt.getResultSet();
	        }
	        while (resultset.next()) {
                System.out.println(resultset.getString("Database"));
            }
        }
        catch (SQLException ex){
            // handle any errors
            ex.printStackTrace();
        }
        finally {
            // release resources
            if (resultset != null) {
                try {
                    resultset.close();
                } catch (SQLException sqlEx) { }
                resultset = null;
            }
 
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { }
                stmt = null;
            }
 
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException sqlEx) { }
                con = null;
            }
        }
    }
	
	
	//way 02
	@Test(priority=2)
	public static void showlistOfEmployes(){
		try{
		Connection con = getMySQLConnection("STAGE");
		Statement stmt = con.createStatement();
		ResultSet resultset = stmt.executeQuery("select * from classicmodels.employees;");
		 if(stmt.execute("select * from classicmodels.employees;")){
	        	resultset = stmt.getResultSet();
	        }
	        while (resultset.next()) {
             System.out.println(resultset.getString("employeeNumber") +" "+ resultset.getString("lastName") );
         }
     }catch(Exception e){
			e.printStackTrace();
		}
	}
	
	@Test(priority=1)
	public static void showtables(){
		try{
		Connection con = getMySQLConnection("STAGE");
		Statement stmt = con.createStatement();
		ResultSet resultset = stmt.executeQuery("show tables from classicmodels;");
		 if(stmt.execute("show tables from classicmodels;")){
	        	resultset = stmt.getResultSet();
	        }
	        while (resultset.next()) {
             System.out.println(resultset.getString("Tables_in_classicmodels"));
         }
     }catch(Exception e){
			e.printStackTrace();
		}
	}
	
	        
}
