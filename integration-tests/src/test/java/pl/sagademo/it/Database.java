package pl.sagademo.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class Database {

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public Database(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public String jdbcUrl(String database) {
        return "jdbc:postgresql://%s:%d/%s".formatted(host, port, database);
    }

    public long count(String database, String sql, Object... parameters) {
        return query(database, sql, parameters, resultSet -> resultSet.getLong(1));
    }

    public String string(String database, String sql, Object... parameters) {
        return query(database, sql, parameters, resultSet -> resultSet.getString(1));
    }

    public void execute(String database, String sql, Object... parameters) {
        try (Connection connection = open(database);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, parameters);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQL failed on " + database + ": " + sql, e);
        }
    }

    private <T> T query(String database, String sql, Object[] parameters, RowReader<T> reader) {
        try (Connection connection = open(database);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, parameters);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return reader.read(resultSet);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQL failed on " + database + ": " + sql, e);
        }
    }

    private Connection open(String database) throws SQLException {
        return DriverManager.getConnection(jdbcUrl(database), user, password);
    }

    private static void bind(PreparedStatement statement, Object[] parameters) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
    }

    @FunctionalInterface
    private interface RowReader<T> {
        T read(ResultSet resultSet) throws SQLException;
    }
}
