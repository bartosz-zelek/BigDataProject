import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class StatesWithHighestDelays {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://127.0.0.1:10000/default";
    private static final String user = "root";
    private static final String password = "";

    public StatesWithHighestDelays() {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static ResultSet executeQuery(String sql) {
        try {
            Connection connection = getConnection();
            if (connection == null) {
                throw new RuntimeException("Failed to establish a connection.");
            }

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            return resultSet;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void executeWithNoResult(String sql) {
        try {
            Connection connection = getConnection();
            if (connection == null) {
                throw new RuntimeException("Failed to establish a connection.");
            }

            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // properly formatted string with big query
        String sql = "CREATE OR REPLACE VIEW TOP_3_STATES_PER_MONTH as\n" +
                "SELECT\n" +
                "    YEAR,\n" +
                "    MONTH,\n" +
                "    STATE,\n" +
                "    AVG(TOTAL_DELAY) AS AVG_DELAY\n" +
                "FROM\n" +
                "    AIRPORTS_LANDINGS_DELAYS\n" +
                "JOIN\n" +
                "    AIRPORTS\n" +
                "ON\n" +
                "    AIRPORTS_LANDINGS_DELAYS.AIRPORT = AIRPORTS.IATA\n" +
                "GROUP BY\n" +
                "    YEAR,\n" +
                "    MONTH,\n" +
                "    STATE\n" +
                "DISTRIBUTE BY -- DISTRIBUTE BY - rozdystrybuuje dane do różnych reducerów w zależności od wartości kolumn YEAR, MONTH\n" +
                "    YEAR, MONTH\n" +
                "SORT BY\n" +
                "    YEAR, MONTH, AVG_DELAY DESC\n";
        executeWithNoResult(sql);

        sql = "SELECT\n" +
                "    YEAR,\n" +
                "    MONTH,\n" +
                "    STATE,\n" +
                "    AVG_DELAY\n" +
                "FROM\n" +
                "    (\n" +
                "        SELECT\n" +
                "            YEAR,\n" +
                "            MONTH,\n" +
                "            STATE,\n" +
                "            AVG_DELAY,\n" +
                "            ROW_NUMBER() OVER (PARTITION BY YEAR, MONTH ORDER BY AVG_DELAY DESC) AS RANK\n" +
                "        FROM\n" +
                "            TOP_3_STATES_PER_MONTH\n" +
                "    ) temp\n" +
                "WHERE\n" +
                "    RANK <= 3\n" +
                "ORDER BY\n" +
                "    YEAR,\n" +
                "    MONTH,\n" +
                "    AVG_DELAY DESC";

        try (ResultSet resultSet = executeQuery(sql)) {
            if (resultSet != null) {
                System.out.println("Results:");
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " " + resultSet.getString(3) + " " + resultSet.getString(4));
                }
            } else {
                System.out.println("No results.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
