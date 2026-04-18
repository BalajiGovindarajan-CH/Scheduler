package com.balaj.scheduler.audit;

import com.balaj.scheduler.config.QuartzConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;

public final class JobRunAuditRepository {
    private static final String UPSERT_AUDIT_SQL = """
            MERGE INTO JOB_RUN_AUDIT (
              FIRE_INSTANCE_ID,
              CORRELATION_ID,
              ROOT_JOB_NAME,
              ROOT_JOB_GROUP,
              PARENT_JOB_NAME,
              PARENT_JOB_GROUP,
              JOB_NAME,
              JOB_GROUP,
              TRIGGER_NAME,
              TRIGGER_GROUP,
              SCHEDULED_FIRE_TIME,
              ACTUAL_FIRE_TIME,
              COMPLETED_AT,
              STATUS,
              ERROR_MESSAGE,
              UPDATED_AT
            ) KEY (FIRE_INSTANCE_ID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """;
    private static final String SELECT_BY_CORRELATION_ID = """
            SELECT
              CORRELATION_ID,
              FIRE_INSTANCE_ID,
              ROOT_JOB_NAME,
              ROOT_JOB_GROUP,
              PARENT_JOB_NAME,
              PARENT_JOB_GROUP,
              JOB_NAME,
              JOB_GROUP,
              TRIGGER_NAME,
              TRIGGER_GROUP,
              STATUS,
              ERROR_MESSAGE
            FROM JOB_RUN_AUDIT
            WHERE CORRELATION_ID = ?
            ORDER BY ACTUAL_FIRE_TIME, CREATED_AT, FIRE_INSTANCE_ID
            """;
    private static final String SELECT_LATEST_BY_JOB_KEY = """
            SELECT
              CORRELATION_ID,
              FIRE_INSTANCE_ID,
              ROOT_JOB_NAME,
              ROOT_JOB_GROUP,
              PARENT_JOB_NAME,
              PARENT_JOB_GROUP,
              JOB_NAME,
              JOB_GROUP,
              TRIGGER_NAME,
              TRIGGER_GROUP,
              STATUS,
              ERROR_MESSAGE
            FROM JOB_RUN_AUDIT
            WHERE JOB_NAME = ? AND JOB_GROUP = ?
            ORDER BY ACTUAL_FIRE_TIME DESC, CREATED_AT DESC, FIRE_INSTANCE_ID DESC
            FETCH FIRST 1 ROW ONLY
            """;

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    public JobRunAuditRepository(Properties quartzProperties) {
        this.jdbcUrl = QuartzConfiguration.getJdbcUrl(quartzProperties);
        this.jdbcUser = QuartzConfiguration.getJdbcUser(quartzProperties);
        this.jdbcPassword = QuartzConfiguration.getJdbcPassword(quartzProperties);
    }

    public void recordJobStarted(
            JobExecutionContext context, String correlationId, JobKey rootJobKey, JobKey parentJobKey)
            throws SQLException {
        upsertAuditRow(context, correlationId, rootJobKey, parentJobKey, JobRunAuditStatus.RUNNING, null);
    }

    public void recordJobCompletion(
            JobExecutionContext context,
            String correlationId,
            JobKey rootJobKey,
            JobKey parentJobKey,
            JobExecutionException jobException)
            throws SQLException {
        JobRunAuditStatus status = jobException == null ? JobRunAuditStatus.SUCCEEDED : JobRunAuditStatus.FAILED;
        String errorMessage = jobException == null ? null : truncate(jobException.getMessage(), 1000);
        upsertAuditRow(context, correlationId, rootJobKey, parentJobKey, status, errorMessage);
    }

    public void recordJobVetoed(
            JobExecutionContext context, String correlationId, JobKey rootJobKey, JobKey parentJobKey)
            throws SQLException {
        upsertAuditRow(context, correlationId, rootJobKey, parentJobKey, JobRunAuditStatus.VETOED, null);
    }

    public List<JobRunAuditRecord> findByCorrelationId(String correlationId) throws SQLException {
        List<JobRunAuditRecord> records = new ArrayList<>();
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(SELECT_BY_CORRELATION_ID)) {
            statement.setString(1, correlationId);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    records.add(new JobRunAuditRecord(
                            resultSet.getString("CORRELATION_ID"),
                            resultSet.getString("FIRE_INSTANCE_ID"),
                            JobKey.jobKey(resultSet.getString("ROOT_JOB_NAME"), resultSet.getString("ROOT_JOB_GROUP")),
                            toNullableJobKey(resultSet.getString("PARENT_JOB_NAME"), resultSet.getString("PARENT_JOB_GROUP")),
                            JobKey.jobKey(resultSet.getString("JOB_NAME"), resultSet.getString("JOB_GROUP")),
                            resultSet.getString("TRIGGER_NAME"),
                            resultSet.getString("TRIGGER_GROUP"),
                            JobRunAuditStatus.valueOf(resultSet.getString("STATUS")),
                            resultSet.getString("ERROR_MESSAGE")));
                }
            }
        }
        return records;
    }

    public Optional<JobRunAuditRecord> findLatestByJobKey(JobKey jobKey) throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(SELECT_LATEST_BY_JOB_KEY)) {
            statement.setString(1, jobKey.getName());
            statement.setString(2, jobKey.getGroup());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(new JobRunAuditRecord(
                        resultSet.getString("CORRELATION_ID"),
                        resultSet.getString("FIRE_INSTANCE_ID"),
                        JobKey.jobKey(resultSet.getString("ROOT_JOB_NAME"), resultSet.getString("ROOT_JOB_GROUP")),
                        toNullableJobKey(resultSet.getString("PARENT_JOB_NAME"), resultSet.getString("PARENT_JOB_GROUP")),
                        JobKey.jobKey(resultSet.getString("JOB_NAME"), resultSet.getString("JOB_GROUP")),
                        resultSet.getString("TRIGGER_NAME"),
                        resultSet.getString("TRIGGER_GROUP"),
                        JobRunAuditStatus.valueOf(resultSet.getString("STATUS")),
                        resultSet.getString("ERROR_MESSAGE")));
            }
        }
    }

    private void upsertAuditRow(
            JobExecutionContext context,
            String correlationId,
            JobKey rootJobKey,
            JobKey parentJobKey,
            JobRunAuditStatus status,
            String errorMessage)
            throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(UPSERT_AUDIT_SQL)) {
            statement.setString(1, context.getFireInstanceId());
            statement.setString(2, correlationId);
            statement.setString(3, rootJobKey.getName());
            statement.setString(4, rootJobKey.getGroup());
            statement.setString(5, parentJobKey == null ? null : parentJobKey.getName());
            statement.setString(6, parentJobKey == null ? null : parentJobKey.getGroup());
            statement.setString(7, context.getJobDetail().getKey().getName());
            statement.setString(8, context.getJobDetail().getKey().getGroup());
            statement.setString(9, context.getTrigger() == null ? null : context.getTrigger().getKey().getName());
            statement.setString(10, context.getTrigger() == null ? null : context.getTrigger().getKey().getGroup());
            statement.setTimestamp(11, toTimestamp(context.getScheduledFireTime()));
            statement.setTimestamp(12, toTimestamp(context.getFireTime()));
            statement.setTimestamp(13, status == JobRunAuditStatus.RUNNING ? null : toTimestamp(context.getJobRunTime(), context.getFireTime()));
            statement.setString(14, status.name());
            statement.setString(15, errorMessage);
            statement.executeUpdate();
        }
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
    }

    private static Timestamp toTimestamp(java.util.Date value) {
        return value == null ? null : Timestamp.from(value.toInstant());
    }

    private static Timestamp toTimestamp(long jobRunTime, java.util.Date fireTime) {
        if (fireTime == null) {
            return null;
        }
        return Timestamp.from(fireTime.toInstant().plusMillis(jobRunTime));
    }

    private static JobKey toNullableJobKey(String name, String group) {
        if (name == null || group == null) {
            return null;
        }
        return JobKey.jobKey(name, group);
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
