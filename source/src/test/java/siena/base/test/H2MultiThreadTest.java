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
import siena.jdbc.H2PersistenceManager;
import siena.jdbc.JdbcPersistenceManager;
import siena.jdbc.PostgresqlPersistenceManager;
import siena.jdbc.ddl.DdlGenerator;

public class H2MultiThreadTest extends BaseMultiThreadTest {
	private static JdbcPersistenceManager pm;
	

	@Override
	public PersistenceManager createPersistenceManager(List<Class<?>> classes) throws Exception {
		if(pm==null){
			Properties p = new Properties();
			
			String driver   = "org.h2.Driver";
			String url      = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
			String username = "sa";
			String password = "";
			
			p.setProperty("driver",   driver);
			p.setProperty("url",      url);
			p.setProperty("user",     username);
			p.setProperty("password", password);

			DdlGenerator generator = new DdlGenerator();
			for (Class<?> clazz : classes) {
				generator.addTable(clazz);
			}
	
			// get the Database model
			Database database = generator.getDatabase();
	
			Platform platform = PlatformFactory.createNewPlatformInstance("mysql");
			Class.forName(driver);
			//JdbcDataSource ds = new JdbcDataSource();
			//ds.setURL(url);
			//Connection connection = ds.getConnection();
			Connection connection = DriverManager.getConnection(url, username, password);
			
			System.out.println(platform.getAlterTablesSql(connection, database));
			
			// this will perform the database changes
			platform.alterTables(connection, database, true);
	
			connection.close();
			
			pm = new H2PersistenceManager();
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
