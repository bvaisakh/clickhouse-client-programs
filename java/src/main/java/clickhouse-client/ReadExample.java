import com.clickhouse.jdbc.*;
import java.sql.*;
import java.util.*;

public class ReadExample {
    public static void main(String[] args) throws Exception {

        String url = "jdbc:ch://localhost:9200";
        Properties properties = new Properties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);

        System.out.println("------------------------------------------------------------------------------");
        System.out.println("Executing: select * from helloworld.my_first_table limit 10");
        System.out.println("------------------------------------------------------------------------------");
        try (Connection connection = dataSource.getConnection("default", null);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select * from helloworld.my_first_table limit 10")) {
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