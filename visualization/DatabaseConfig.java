import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConfig {
    
    // Hive数据库连接配置
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_URL = "jdbc:hive2://localhost:10000/crime_analysis";
    private static final String HIVE_USER = "hive";
    private static final String HIVE_PASSWORD = "";
    
    // MySQL数据库连接配置
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/crime_analysis";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";
    
    static {
        try {
            // 加载数据库驱动
            Class.forName(HIVE_DRIVER);
            Class.forName(MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load database drivers: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 获取Hive数据库连接
     */
    public static Connection getHiveConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", HIVE_USER);
        properties.setProperty("password", HIVE_PASSWORD);
        
        return DriverManager.getConnection(HIVE_URL, properties);
    }
    
    /**
     * 获取MySQL数据库连接
     */
    public static Connection getMySQLConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", MYSQL_USER);
        properties.setProperty("password", MYSQL_PASSWORD);
        properties.setProperty("useSSL", "false");
        
        return DriverManager.getConnection(MYSQL_URL, properties);
    }
    
    /**
     * 关闭数据库连接
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Failed to close connection: " + e.getMessage());
            }
        }
    }
}