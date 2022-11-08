import com.clickhouse.jdbc.*;
import java.sql.*;
import java.util.*;

public class ReadExample {
    public static void main(String[] args) throws Exception {

        String url = "jdbc:ch://localhost:9200";
        Properties properties = new Properties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Connection connection = dataSource.getConnection("default", null);
        Statement statement = connection.createStatement();

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("Executing: SELECT id, name, mail FROM demo.contacts LIMIT 10");
        System.out.println("------------------------------------------------------------------------------");
        
        ResultSet resultSet = statement.executeQuery("SELECT id, name, mail FROM demo.contacts LIMIT 10");
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columns = resultSetMetaData.getColumnCount();
        while (resultSet.next()) {
            for (int c = 1; c <= columns; c++) {
                System.out.print(resultSetMetaData.getColumnName(c) + ":" + resultSet.getString(c)
                        + (c < columns ? ", " : "\n"));
            }
        }
        
        System.out.println("------------------------------------------------------------------------------\n");
    }
}