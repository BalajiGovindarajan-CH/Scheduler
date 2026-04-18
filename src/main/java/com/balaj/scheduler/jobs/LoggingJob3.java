package com.balaj.scheduler.jobs;

import com.balaj.scheduler.audit.JobRunAuditRecord;
import com.balaj.scheduler.audit.JobRunAuditRepository;
import com.balaj.scheduler.audit.JobRunAuditStatus;
import com.balaj.scheduler.config.QuartzConfiguration;
import com.balaj.scheduler.dependency.JobDependency;
import com.balaj.scheduler.dependency.JobDependencyRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingJob3 implements Job {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingJob3.class);
    private static final Duration RETRY_DELAY = Duration.ofMinutes(2);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            Properties quartzProperties = QuartzConfiguration.loadQuartzProperties();
            JobDependencyRepository dependencyRepository = new JobDependencyRepository(quartzProperties);
            JobRunAuditRepository auditRepository = new JobRunAuditRepository(quartzProperties);
            JobKey currentJobKey = context.getJobDetail().getKey();

            List<JobDependency> parentDependencies = dependencyRepository.findActiveParentDependencies(currentJobKey);
            if (!parentsAreReady(context, auditRepository, parentDependencies)) {
                return;
            }

            LOGGER.info("Executing {} at {}. Parent dependency checks passed.", currentJobKey, Instant.now());
        } catch (Exception ex) {
            throw new JobExecutionException("Failed to evaluate parent dependency status for LoggingJob3.", ex);
        }
    }

    private boolean parentsAreReady(
            JobExecutionContext context,
            JobRunAuditRepository auditRepository,
            List<JobDependency> parentDependencies)
            throws Exception {
        if (parentDependencies.isEmpty()) {
            LOGGER.info("{} has no active parent dependencies. Running scheduled job.",
                    context.getJobDetail().getKey());
            return true;
        }

        for (JobDependency dependency : parentDependencies) {
            JobKey parentJobKey = dependency.parentJobKey();
            Optional<JobRunAuditRecord> latestParentRun = auditRepository.findLatestByJobKey(parentJobKey);
            if (latestParentRun.isEmpty()) {
                LOGGER.info("{} is waiting for parent {} because no completed or running parent audit row exists.",
                        context.getJobDetail().getKey(), parentJobKey);
                return false;
            }

            JobRunAuditStatus parentStatus = latestParentRun.get().status();
            if (parentStatus == JobRunAuditStatus.SUCCEEDED) {
                continue;
            }

            if (parentStatus == JobRunAuditStatus.RUNNING) {
                LOGGER.info("{} is waiting for parent {} to complete. Scheduling retry in {} minutes.",
                        context.getJobDetail().getKey(), parentJobKey, RETRY_DELAY.toMinutes());
                scheduleRetry(context, parentJobKey);
                return false;
            }

            LOGGER.info("{} will not run because latest parent {} status is {}.",
                    context.getJobDetail().getKey(), parentJobKey, parentStatus);
            return false;
        }

        return true;
    }

    private void scheduleRetry(JobExecutionContext context, JobKey parentJobKey) throws SchedulerException {
        String retryId = sanitize(context.getFireInstanceId() + "-" + parentJobKey);
        JobDataMap retryData = new JobDataMap();
        retryData.putAll(context.getMergedJobDataMap());
        retryData.put("retryForParentJob", parentJobKey.toString());

        Trigger retryTrigger = TriggerBuilder.newTrigger()
                .withIdentity(
                        QuartzConfiguration.SCHEDULED_DEPENDENT_RETRY_TRIGGER_NAME_PREFIX + "-" + retryId,
                        context.getJobDetail().getKey().getGroup())
                .forJob(context.getJobDetail())
                .usingJobData(retryData)
                .startAt(Date.from(Instant.now().plus(RETRY_DELAY)))
                .build();

        context.getScheduler().scheduleJob(retryTrigger);
    }

    private String sanitize(String value) {
        if (value == null || value.isBlank()) {
            return Long.toString(System.currentTimeMillis());
        }
        return value.replaceAll("[^A-Za-z0-9_-]", "_");
    }
}
