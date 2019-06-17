package com.n4mative.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import org.springframework.core.env.Environment;

public class MySqlConnection {

	private static Connection connection = null;
	private static Object obj = new Object();
	private static String database = "";
	private Environment env;
	public MySqlConnection() {
		
	}
	
	public MySqlConnection(String dbname) {
		synchronized (obj) {
			try {
				if (!connection.isValid(5) || connection == null || !database.equals(dbname)) {
				
					database = dbname;
					connection = getMySQLConnection();
					//LOG.info("MySQL connection created");
					//System.out.println("MySQL connection created");
				} 
			} catch (Exception e) {
				//LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public static Connection getConnection(String dbname)
			throws IOException {
		//if (connection == null || !database.equals(dbname)) {
			synchronized (obj) {
				//database = dbname;
				new MySqlConnection(dbname);
			}
		//}
		return connection;
	}
	
	private Connection getMySQLConnection() {
		
		System.out.println("mysql.host: " + env.getProperty("mysql.host"));
	       //String url = "jdbc:mysql://192.168.1.71:3306/"+ database;
	    String url = "jdbc:mysql://" + env.getProperty("mysql.host") + "/"+ database;
	        
    	//String url = "jdbc:mysql://192.168.1.71:3306/"+ database;
        String username = "liamapp";
        String password = "Gauch022$";
        System.out.println("Connecting database...");
        
        try {
        
        	connection = DriverManager.getConnection(url, username, password);
        	if(connection != null) {
        		System.out.println("MySQL Database connected!");
        	} else {
        		System.out.println("MySQL Database NOT connected!");
        	}

        } catch(Exception e) {
        	e.printStackTrace();
        }
        return connection;
    }
}
