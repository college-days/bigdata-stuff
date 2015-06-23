import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class HiveUtil {
	
	private static Connection connection = null;
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	public static Connection getConnection() {
		if (connection == null) {
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			try {
				connection = DriverManager.getConnection(
						"jdbc:hive://192.168.1.103:10003/default", "hive", "");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return connection;
	}
}
