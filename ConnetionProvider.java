package test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * Singleton class to get db connection
 *
 */
public class ConnectionProvider {
	
	private static final Logger log = Logger.getLogger(ConnectionProvider.class.getName());
	
	private DataSource dataSource;
	private String schema;
	
	public DataSource getDataSource() {
		return dataSource;
	}

	public String getSchema() {
		return schema;
	}

	private static class ConnectionHolder {
		private static final ConnectionProvider INSTANCE = new ConnectionProvider();
	}
	
	private ConnectionProvider() {
		
		Properties jdbcp = new Properties();
		String jndiName = "";
		try {
			jdbcp.load(ConnectionProvider.class.getClassLoader().getResourceAsStream("/jdbc.properties"));
			jndiName = jdbcp.getProperty("jdbc.jndiName");
			schema = jdbcp.getProperty("jdbc.schema");
			
		} catch (IOException e) {
			log.error("Cannot load jdbc.properties, exception :" , e);
		}

		try {
			Hashtable<String, String> ht = new Hashtable<String, String>();
			ht.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
			ht.put(Context.PROVIDER_URL, "t3://localhost:7002");
			InitialContext ctx = new InitialContext(ht);
			dataSource = (DataSource) ctx.lookup(jndiName);
		} catch (Exception e) {
			log.error("Cannot get connection, exception :" , e);
			try {
				Hashtable<String, String> ht2 = new Hashtable<String, String>();
				ht2.put(Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
				ht2.put(Context.PROVIDER_URL, "t3://localhost:7001");
				InitialContext ctx2 = new InitialContext(ht2);
				dataSource = (DataSource) ctx2.lookup(jndiName); 
			} catch(Exception ex){
				log.error("Exception :" , e);
			}
		}
		
	}
	
	
	public static final ConnectionProvider getInstance(){
		return ConnectionHolder.INSTANCE;
	}
	
	
	
	public static Connection getConnection() throws SQLException {
		Connection conn = null;
		ConnectionProvider connectionProvider = ConnectionProvider.getInstance();
		if(connectionProvider.getDataSource() != null) {
			conn = connectionProvider.getDataSource().getConnection();
			conn.createStatement().execute("alter session set current_schema=" + connectionProvider.getSchema()); 
		}
		return conn;
	}
	
	public static ResultSet query(PreparedStatement psmt, Object... args) throws SQLException {
		
		int index = 0;
		for(Object o : args) {
			index ++;
			if(o instanceof String) {
				psmt.setString(index, (String) o);
			} else if (o instanceof Integer) {
				psmt.setInt(index, (Integer) o);
			} else if (o instanceof Long) {
				psmt.setLong(index, (Long) o);
			}else if (o instanceof Float) {
				psmt.setFloat(index, (Float) o);
			} else if (o instanceof Double) {
				psmt.setDouble(index, (Double) o);
			} else if (o instanceof BigDecimal) {
				psmt.setBigDecimal(index, (BigDecimal) o);
			}
		}
		
        ResultSet res = psmt.executeQuery();
        
        return res;
    }

	public static void closeConnection(Statement smt, Connection conn) {
		try{
			if(smt != null) smt.close();
		} catch(SQLException e) {
			log.error("Exception:", e);
		}
		try{
			if(conn != null ) conn.close();
		} catch(SQLException e) {
			log.error("Exception:", e);
		}
	}
	
}
