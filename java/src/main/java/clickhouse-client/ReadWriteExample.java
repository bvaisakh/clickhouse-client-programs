import com.clickhouse.jdbc.*;
import java.sql.*;
import java.util.*;

public class ReadWriteExample {
    public static void main(String[] args) throws Exception {

        String url = "jdbc:ch://localhost:9200";
        Properties properties = new Properties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Connection connection = dataSource.getConnection("default", null);

        System.out.println("------------------------------------------------------------------------------");
        System.out.println(
                "INSERT INTO demo.contacts (id, name, mail) VALUES (4, 'Ahamed Sinan', 'ahamed.sinan@chistadata.com')");
        System.out.println("------------------------------------------------------------------------------");

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement
                .executeQuery("SELECT COUNT(*) FROM demo.contacts");
        if (resultSet.next()) {
            System.out.println("ROW COUNT BEFORE INSERT: " + resultSet.getString(1));
        }

        System.out.println("INSERTION IN PROGRESS");
        String sql = "INSERT INTO demo.contacts (id, name, mail) VALUES (4, 'Ahamed Sinan', 'ahamed.sinan@chistadata.com')";
        statement.execute(sql);

        resultSet = statement
                .executeQuery("SELECT COUNT(*) FROM demo.contacts");
        if (resultSet.next()) {
            System.out.println("ROW COUNT AFTER INSERT: " + resultSet.getString(1));
        }
        System.out.println("------------------------------------------------------------------------------\n");
    }
}