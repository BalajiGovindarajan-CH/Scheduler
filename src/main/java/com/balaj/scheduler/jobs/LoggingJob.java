package com.balaj.scheduler.jobs;

import com.balaj.scheduler.config.QuartzConfiguration;
import java.time.Instant;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingJob implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String description = context.getMergedJobDataMap().getString(QuartzConfiguration.DESCRIPTION_KEY);
        LOGGER.info("Executing {} at {}. {}",
                context.getJobDetail().getKey(),
                Instant.now(),
                description);
    }
}
