package com.balaj.scheduler.jobs;

import com.balaj.scheduler.config.QuartzConfiguration;
import java.time.Instant;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingJob2 implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingJob2.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String parentJobName = context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_JOB_NAME_KEY);
        String parentJobGroup = context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_JOB_GROUP_KEY);
        String parentJobKey = parentJobName == null || parentJobGroup == null
                ? context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_JOB_KEY)
                : parentJobGroup + "." + parentJobName;
        String parentFireTime = context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_FIRE_TIME_KEY);
        String correlationId = context.getMergedJobDataMap().getString(QuartzConfiguration.CORRELATION_ID_KEY);

        LOGGER.info("Executing {} at {}. Triggered by {} at {}. CorrelationId={}",
                context.getJobDetail().getKey(),
                Instant.now(),
                parentJobKey,
                parentFireTime,
                correlationId);
    }
}
