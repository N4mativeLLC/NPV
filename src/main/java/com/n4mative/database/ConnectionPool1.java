package com.n4mative.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

public class ConnectionPool1 {
	
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    //public static final String URL = "jdbc:mysql://localhost/kodejava";
    //public static final String USERNAME = "kodejava";
    //public static final String PASSWORD = "kodejava123";
    
	public static final String URL = "jdbc:mysql://192.168.1.71:3306/";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "hadoop";
    
   // String url = "jdbc:mysql://192.168.1.71:3306/"+ database;
    //String username = "root";
   // String password = "hadoop";

    //private GenericObjectPool connectionPool = null;
    private ObjectPool<PoolableConnection> connectionPool = null;
    private static Object obj = new Object();
    private static DataSource dataSource = null;
    public static Class driverClass;
    
    public ConnectionPool1() {
    	
    }
    
    public ConnectionPool1(String dbname) {
		synchronized (obj) {
			try {
				if (connectionPool == null || dataSource == null) {
				
					//database = dbname;
					dataSource = setUp(dbname);
					//LOG.info("MySQL connection created");
					//System.out.println("MySQL connection created");
				} 
			} catch (Exception e) {
				//LOG.error(e.getMessage());
				e.printStackTrace();
			}
		}
	}
    
    private DataSource setUp(String database) throws Exception {
        // Load JDBC Driver class.
        //Class.forName(ConnectionPool.DRIVER).newInstance();
    	
		registerJDBCDriver(MYSQL_DRIVER);

		// 2. Create the Connection Factory (DriverManagerConnectionFactory)
		ConnectionFactory connectionFactory = 
			getConnFactory(URL+database, USERNAME, PASSWORD);
		
		// 3. Instantiate the Factory of Pooled Objects
		PoolableConnectionFactory poolfactory = new PoolableConnectionFactory(
				connectionFactory, null);

        // Creates an instance of GenericObjectPool that holds our
        // pool of connections object.
        connectionPool = new GenericObjectPool<PoolableConnection>(poolfactory);

        //connectionPool = new GenericObjectPool(null);
        //connectionPool.set

        // Creates a connection factory object which will be use by
        // the pool to create the connection object. We passes the
        // JDBC url info, username and password.
        //ConnectionFactory cf = new DriverManagerConnectionFactory(
        //        ConnectionPool.URL,
        //        ConnectionPool.USERNAME,
        //        ConnectionPool.PASSWORD);

        // Creates a PoolableConnectionFactory that will wraps the
        // connection object created by the ConnectionFactory to add
        // object pooling functionality.
       // PoolableConnectionFactory pcf =
        //        new PoolableConnectionFactory(cf, null);
        return new PoolingDataSource(connectionPool);
    }

    private ConnectionFactory getConnFactory(String connectionURI,
    		
    		            String user, String password) {
    		
    		        ConnectionFactory driverManagerConnectionFactory = new DriverManagerConnectionFactory(
    		
    		                connectionURI, user, password);
    		
    		        return driverManagerConnectionFactory;
    		
    		    }

    
    private ObjectPool<PoolableConnection> getConnectionPool() {
        return connectionPool;
    }

    public static Connection getConnection(String dbName) throws IOException, SQLException {

		synchronized (obj) {
			new ConnectionPool(dbName);
		}
		return dataSource.getConnection();
    }
    
    public Connection getConnection1() {
    	try {
    		return dataSource.getConnection();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
		return null;
    }
    
    private void registerJDBCDriver(String driver) {
		try {
			driverClass = Class.forName(driver);
		} catch (ClassNotFoundException e) {
			System.err.println("There was not able to find the driver class");
		}
	}
    /*
    public static void main(String[] args) throws Exception {
        ConnectionPool demo = new ConnectionPool("LIAM");
        //DataSource dataSource = demo.setUp("LIAM");
        //demo.printStatus();
        
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = dataSource.getConnection();
            demo.printStatus();

            stmt = conn.prepareStatement("SELECT * FROM heirarchy where id = 1");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                System.out.println("name: " + rs.getString("name"));
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        demo.printStatus();
    }
*/
    /**
     * Prints connection pool status.
     */
    private void printStatus() {
        System.out.println("Max   : " + getConnectionPool().getNumIdle() + "; " +
                "Active: " + getConnectionPool().getNumActive() + "; " +
                "Idle  : " + getConnectionPool().getNumIdle());
    }
    
}
