import com.clickhouse.jdbc.*;
import java.sql.*;
import java.util.*;

public class ReadWriteExample {
    public static void main(String[] args) throws Exception {

        String url = "jdbc:ch://localhost:9200";
        Properties properties = new Properties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("SELECT: Total count of helloworld.my_first_table");
        System.out.println("------------------------------------------------------------------------------");
        try (Connection connection = dataSource.getConnection("default", null);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement
                        .executeQuery("select count(*) AS count from helloworld.my_first_table")) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columns = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                for (int c = 1; c <= columns; c++) {
                    System.out.print(resultSetMetaData.getColumnName(c) + ":" + resultSet.getString(c)
                            + (c < columns ? ", " : "\n"));
                }
            }
        }
        System.out.println("------------------------------------------------------------------------------\n");

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("INSERTION IN PROGRESS");
        System.out.println("------------------------------------------------------------------------------");
        String sql = "INSERT INTO helloworld.my_first_table (user_id, message, timestamp, metric) VALUES " +
                "(101, 'Hello, ClickHouse!', now(), -1.0), " +
                "(102, 'Insert a lot of rows per batch', yesterday(), 1.41421 ), " +
                "(102, 'Sort your data based on your commonly-used queries', today(), 2.718), " +
                "(101, 'Granules are the smallest chunks of data read', now() + 5, 3.14159)";
        try (Connection connection = dataSource.getConnection("default", null)) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        }

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("SELECT: Total count of helloworld.my_first_table");
        System.out.println("------------------------------------------------------------------------------");
        try (

                Connection connection = dataSource.getConnection("default", null);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement
                        .executeQuery("select count(*) AS count from helloworld.my_first_table")) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columns = resultSetMetaData.getColumnCount();
            while (resultSet.next()) {
                for (int c = 1; c <= columns; c++) {
                    System.out.print(resultSetMetaData.getColumnName(c) + ":" + resultSet.getString(c)
                            + (c < columns ? ", " : "\n"));
                }
            }
        }
        System.out.println("------------------------------------------------------------------------------\n");
    }
}