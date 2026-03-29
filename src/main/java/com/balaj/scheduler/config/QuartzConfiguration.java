package com.balaj.scheduler.config;

import com.balaj.scheduler.jobs.LoggingJob;
import com.balaj.scheduler.jobs.LoggingJob2;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

public final class QuartzConfiguration {
    public static final String PRIMARY_JOB_GROUP = "demo";
    public static final String PRIMARY_JOB_NAME = "sampleLoggingJob";
    public static final String PRIMARY_TRIGGER_NAME = "sampleLoggingTrigger";
    public static final String DOWNSTREAM_JOB_GROUP = "demo-downstream";
    public static final String DOWNSTREAM_JOB_NAME = "sampleLoggingJob2";
    public static final String DOWNSTREAM_TRIGGER_NAME_PREFIX = "sampleLoggingTrigger2";
    public static final String DESCRIPTION_KEY = "description";
    public static final String PARENT_JOB_KEY = "parentJobKey";
    public static final String PARENT_JOB_NAME_KEY = "parentJobName";
    public static final String PARENT_JOB_GROUP_KEY = "parentJobGroup";
    public static final String PARENT_FIRE_TIME_KEY = "parentFireTime";
    public static final String CORRELATION_ID_KEY = "correlationId";
    public static final String ROOT_JOB_NAME_KEY = "rootJobName";
    public static final String ROOT_JOB_GROUP_KEY = "rootJobGroup";
    private static final String PROPERTIES_RESOURCE = "/quartz.properties";
    private static final String DATA_SOURCE_PREFIX = "org.quartz.dataSource.quartzDataSource.";

    private QuartzConfiguration() {
    }

    public static Properties loadQuartzProperties() throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = QuartzConfiguration.class.getResourceAsStream(PROPERTIES_RESOURCE)) {
            if (inputStream == null) {
                throw new IOException("Missing Quartz configuration resource: " + PROPERTIES_RESOURCE);
            }
            properties.load(inputStream);
        }
        return properties;
    }

    public static String getJdbcUrl(Properties properties) {
        return properties.getProperty(DATA_SOURCE_PREFIX + "URL");
    }

    public static String getJdbcUser(Properties properties) {
        return properties.getProperty(DATA_SOURCE_PREFIX + "user");
    }

    public static String getJdbcPassword(Properties properties) {
        return properties.getProperty(DATA_SOURCE_PREFIX + "password", "");
    }

    public static JobDetail createPrimaryJobDetail() {
        return JobBuilder.newJob(LoggingJob.class)
                .withIdentity(PRIMARY_JOB_NAME, PRIMARY_JOB_GROUP)
                .usingJobData(DESCRIPTION_KEY, "Replace this with your real scheduled work.")
                .storeDurably()
                .build();
    }

    public static Trigger createPrimaryTrigger() {
        return TriggerBuilder.newTrigger()
                .withIdentity(PRIMARY_TRIGGER_NAME, PRIMARY_JOB_GROUP)
                .forJob(PRIMARY_JOB_NAME, PRIMARY_JOB_GROUP)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(30)
                        .repeatForever())
                .startNow()
                .build();
    }

    public static JobDetail createDownstreamJobDetail() {
        return JobBuilder.newJob(LoggingJob2.class)
                .withIdentity(DOWNSTREAM_JOB_NAME, DOWNSTREAM_JOB_GROUP)
                .storeDurably()
                .build();
    }
}
