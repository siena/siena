package siena.base.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import junit.framework.TestResult;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Database;

import siena.PersistenceManager;
import siena.Query;
import siena.base.test.model.Discovery4Search;
import siena.jdbc.JdbcPersistenceManager;
import siena.jdbc.PostgresqlPersistenceManager;
import siena.jdbc.ddl.DdlGenerator;

public class JdbcMultiThreadTest extends BaseMultiThreadTest {
	private static JdbcPersistenceManager pm;
	

	@Override
	public PersistenceManager createPersistenceManager(List<Class<?>> classes) throws Exception {
		if(pm == null){
			Properties p = new Properties();
			
			String driver   = "com.mysql.jdbc.Driver";
			String username = "siena";
			String password = "siena";
			String url      = "jdbc:mysql://localhost/siena";
			
			p.setProperty("driver",   driver);
			p.setProperty("user",     username);
			p.setProperty("password", password);
			p.setProperty("url",      url);
	
			Class.forName(driver);
			BasicDataSource dataSource = new BasicDataSource();
			dataSource = new BasicDataSource();
			dataSource.setUrl(url);
			dataSource.setUsername(username);
			dataSource.setPassword(password);
			dataSource.setMaxWait(2000); // 2 seconds max for wait a connection.
			
			DdlGenerator generator = new DdlGenerator();
			for (Class<?> clazz : classes) {
				generator.addTable(clazz);
			}
	
			// get the Database model
			Database database = generator.getDatabase();
	
			Platform platform = PlatformFactory.createNewPlatformInstance("mysql");
			Class.forName("com.mysql.jdbc.Driver");
			Connection connection = DriverManager.getConnection(url, username, password);
			
			System.out.println(platform.getAlterTablesSql(connection, database));
			
			// this will perform the database changes
			platform.alterTables(connection, database, true);
	
			connection.close();
			
			pm = new JdbcPersistenceManager();
			pm.init(p);
		}
		return pm;
	}

	@Override
	public void testMultiThreadSimple() {
		// TODO Auto-generated method stub
		super.testMultiThreadSimple();
	}

	
}
