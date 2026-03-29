package com.balaj.scheduler.db;

import com.balaj.scheduler.config.QuartzConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.h2.tools.RunScript;

public final class DatabaseSchemaInitializer {
    private static final String SCHEMA_RESOURCE = "/db/quartz_tables_h2.sql";
    private static final String JOB_DETAILS_TABLE = "QRTZ_JOB_DETAILS";
    private static final String DEPENDENCY_TABLE = "JOB_DEPENDENCIES";
    private static final String DEPENDENCY_SCHEMA_SQL = """
            CREATE TABLE IF NOT EXISTS JOB_DEPENDENCIES (
              ID IDENTITY PRIMARY KEY,
              PARENT_JOB_NAME VARCHAR(200) NOT NULL,
              PARENT_JOB_GROUP VARCHAR(200) NOT NULL,
              CHILD_JOB_NAME VARCHAR(200) NOT NULL,
              CHILD_JOB_GROUP VARCHAR(200) NOT NULL,
              TRIGGER_TYPE VARCHAR(100) NOT NULL,
              ACTIVE BOOLEAN NOT NULL DEFAULT TRUE,
              CREATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UPDATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT UK_JOB_DEPENDENCIES UNIQUE (
                PARENT_JOB_NAME, PARENT_JOB_GROUP, CHILD_JOB_NAME, CHILD_JOB_GROUP, TRIGGER_TYPE
              )
            )
            """;
    private static final String RUN_AUDIT_TABLE = "JOB_RUN_AUDIT";
    private static final String RUN_AUDIT_SCHEMA_SQL = """
            CREATE TABLE IF NOT EXISTS JOB_RUN_AUDIT (
              ID IDENTITY PRIMARY KEY,
              FIRE_INSTANCE_ID VARCHAR(255) NOT NULL,
              CORRELATION_ID VARCHAR(255) NOT NULL,
              ROOT_JOB_NAME VARCHAR(200) NOT NULL,
              ROOT_JOB_GROUP VARCHAR(200) NOT NULL,
              PARENT_JOB_NAME VARCHAR(200),
              PARENT_JOB_GROUP VARCHAR(200),
              JOB_NAME VARCHAR(200) NOT NULL,
              JOB_GROUP VARCHAR(200) NOT NULL,
              TRIGGER_NAME VARCHAR(200),
              TRIGGER_GROUP VARCHAR(200),
              SCHEDULED_FIRE_TIME TIMESTAMP,
              ACTUAL_FIRE_TIME TIMESTAMP,
              COMPLETED_AT TIMESTAMP,
              STATUS VARCHAR(50) NOT NULL,
              ERROR_MESSAGE VARCHAR(1000),
              CREATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              UPDATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT UK_JOB_RUN_AUDIT_FIRE_INSTANCE UNIQUE (FIRE_INSTANCE_ID)
            )
            """;
    private static final String RUN_AUDIT_VIEW = """
            CREATE VIEW IF NOT EXISTS JOB_RUN_AUDIT_VIEW AS
            SELECT
              CORRELATION_ID,
              ROOT_JOB_GROUP,
              ROOT_JOB_NAME,
              PARENT_JOB_GROUP,
              PARENT_JOB_NAME,
              JOB_GROUP,
              JOB_NAME,
              STATUS,
              FIRE_INSTANCE_ID,
              TRIGGER_GROUP,
              TRIGGER_NAME,
              SCHEDULED_FIRE_TIME,
              ACTUAL_FIRE_TIME,
              COMPLETED_AT,
              ERROR_MESSAGE
            FROM JOB_RUN_AUDIT
            """;

    private DatabaseSchemaInitializer() {
    }

    public static void ensureSchema(Properties quartzProperties) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection(
                QuartzConfiguration.getJdbcUrl(quartzProperties),
                QuartzConfiguration.getJdbcUser(quartzProperties),
                QuartzConfiguration.getJdbcPassword(quartzProperties))) {
            if (!tableExists(connection, JOB_DETAILS_TABLE)) {
                try (InputStream inputStream = DatabaseSchemaInitializer.class.getResourceAsStream(SCHEMA_RESOURCE)) {
                    if (inputStream == null) {
                        throw new IOException("Missing schema resource: " + SCHEMA_RESOURCE);
                    }
                    try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                        RunScript.execute(connection, reader);
                    }
                }
            }

            if (!tableExists(connection, DEPENDENCY_TABLE)) {
                try (PreparedStatement statement = connection.prepareStatement(DEPENDENCY_SCHEMA_SQL)) {
                    statement.execute();
                }
            }

            if (!tableExists(connection, RUN_AUDIT_TABLE)) {
                try (PreparedStatement statement = connection.prepareStatement(RUN_AUDIT_SCHEMA_SQL)) {
                    statement.execute();
                }
            }

            try (PreparedStatement statement = connection.prepareStatement(RUN_AUDIT_VIEW)) {
                statement.execute();
            }
        }
    }

    private static boolean tableExists(Connection connection, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt(1) > 0;
            }
        }
    }
}
