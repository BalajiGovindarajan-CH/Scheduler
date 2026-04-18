package com.balaj.scheduler.dependency;

import com.balaj.scheduler.config.QuartzConfiguration;
import com.balaj.scheduler.listener.TriggerType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.quartz.JobKey;

public final class JobDependencyRepository {
    private static final String SELECT_ACTIVE_DEPENDENCIES = """
            SELECT ID, PARENT_JOB_NAME, PARENT_JOB_GROUP, CHILD_JOB_NAME, CHILD_JOB_GROUP, TRIGGER_TYPE, ACTIVE
            FROM JOB_DEPENDENCIES
            WHERE PARENT_JOB_NAME = ? AND PARENT_JOB_GROUP = ? AND ACTIVE = TRUE
            ORDER BY ID
            """;
    private static final String INSERT_DEPENDENCY = """
            MERGE INTO JOB_DEPENDENCIES (
              PARENT_JOB_NAME, PARENT_JOB_GROUP, CHILD_JOB_NAME, CHILD_JOB_GROUP, TRIGGER_TYPE, ACTIVE, UPDATED_AT
            ) KEY (PARENT_JOB_NAME, PARENT_JOB_GROUP, CHILD_JOB_NAME, CHILD_JOB_GROUP, TRIGGER_TYPE)
            VALUES (?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
            """;
    private static final String EXISTS_ACTIVE_DEPENDENCY = """
            SELECT 1
            FROM JOB_DEPENDENCIES
            WHERE PARENT_JOB_NAME = ? AND PARENT_JOB_GROUP = ? AND ACTIVE = TRUE
            FETCH FIRST 1 ROW ONLY
            """;
    private static final String SELECT_ACTIVE_PARENT_DEPENDENCIES = """
            SELECT ID, PARENT_JOB_NAME, PARENT_JOB_GROUP, CHILD_JOB_NAME, CHILD_JOB_GROUP, TRIGGER_TYPE, ACTIVE
            FROM JOB_DEPENDENCIES
            WHERE CHILD_JOB_NAME = ? AND CHILD_JOB_GROUP = ? AND ACTIVE = TRUE
            ORDER BY ID
            """;

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    public JobDependencyRepository(Properties quartzProperties) {
        this.jdbcUrl = QuartzConfiguration.getJdbcUrl(quartzProperties);
        this.jdbcUser = QuartzConfiguration.getJdbcUser(quartzProperties);
        this.jdbcPassword = QuartzConfiguration.getJdbcPassword(quartzProperties);
    }

    public List<JobDependency> findActiveDependencies(JobKey parentJobKey) throws SQLException {
        List<JobDependency> dependencies = new ArrayList<>();
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(SELECT_ACTIVE_DEPENDENCIES)) {
            statement.setString(1, parentJobKey.getName());
            statement.setString(2, parentJobKey.getGroup());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    dependencies.add(new JobDependency(
                            resultSet.getLong("ID"),
                            JobKey.jobKey(resultSet.getString("PARENT_JOB_NAME"), resultSet.getString("PARENT_JOB_GROUP")),
                            JobKey.jobKey(resultSet.getString("CHILD_JOB_NAME"), resultSet.getString("CHILD_JOB_GROUP")),
                            TriggerType.valueOf(resultSet.getString("TRIGGER_TYPE")),
                            resultSet.getBoolean("ACTIVE")));
                }
            }
        }
        return dependencies;
    }

    public void upsertDependency(JobKey parentJobKey, JobKey childJobKey, TriggerType triggerType) throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(INSERT_DEPENDENCY)) {
            statement.setString(1, parentJobKey.getName());
            statement.setString(2, parentJobKey.getGroup());
            statement.setString(3, childJobKey.getName());
            statement.setString(4, childJobKey.getGroup());
            statement.setString(5, triggerType.name());
            statement.executeUpdate();
        }
    }

    public boolean hasActiveDependencies(JobKey parentJobKey) throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(EXISTS_ACTIVE_DEPENDENCY)) {
            statement.setString(1, parentJobKey.getName());
            statement.setString(2, parentJobKey.getGroup());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    public List<JobDependency> findActiveParentDependencies(JobKey childJobKey) throws SQLException {
        List<JobDependency> dependencies = new ArrayList<>();
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(SELECT_ACTIVE_PARENT_DEPENDENCIES)) {
            statement.setString(1, childJobKey.getName());
            statement.setString(2, childJobKey.getGroup());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    dependencies.add(new JobDependency(
                            resultSet.getLong("ID"),
                            JobKey.jobKey(resultSet.getString("PARENT_JOB_NAME"), resultSet.getString("PARENT_JOB_GROUP")),
                            JobKey.jobKey(resultSet.getString("CHILD_JOB_NAME"), resultSet.getString("CHILD_JOB_GROUP")),
                            TriggerType.valueOf(resultSet.getString("TRIGGER_TYPE")),
                            resultSet.getBoolean("ACTIVE")));
                }
            }
        }
        return dependencies;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }
}
