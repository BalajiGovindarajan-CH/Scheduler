package com.balaj.scheduler.listener;

import com.balaj.scheduler.audit.JobRunAuditRepository;
import com.balaj.scheduler.config.QuartzConfiguration;
import com.balaj.scheduler.dependency.JobDependency;
import com.balaj.scheduler.dependency.JobDependencyRepository;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DatabaseBackedChildTriggeringListener implements JobListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseBackedChildTriggeringListener.class);
    private static final String LISTENER_NAME = "database-backed-child-triggering-listener";
    private final JobDependencyRepository dependencyRepository;
    private final JobRunAuditRepository jobRunAuditRepository;

    public DatabaseBackedChildTriggeringListener(
            JobDependencyRepository dependencyRepository, JobRunAuditRepository jobRunAuditRepository) {
        this.dependencyRepository = dependencyRepository;
        this.jobRunAuditRepository = jobRunAuditRepository;
    }

    @Override
    public String getName() {
        return LISTENER_NAME;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        if (!shouldProcess(context)) {
            return;
        }

        try {
            jobRunAuditRepository.recordJobStarted(
                    context,
                    resolveCorrelationId(context),
                    resolveRootJobKey(context),
                    resolveParentJobKey(context));
        } catch (Exception ex) {
            LOGGER.error("Failed to write RUNNING audit row for job {}.", context.getJobDetail().getKey(), ex);
        }
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
        if (!shouldProcess(context)) {
            return;
        }

        try {
            jobRunAuditRepository.recordJobVetoed(
                    context,
                    resolveCorrelationId(context),
                    resolveRootJobKey(context),
                    resolveParentJobKey(context));
        } catch (Exception ex) {
            LOGGER.error("Failed to write VETOED audit row for job {}.", context.getJobDetail().getKey(), ex);
        }
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        try {
            if (!shouldProcess(context)) {
                return;
            }

            String correlationId = resolveCorrelationId(context);
            JobKey rootJobKey = resolveRootJobKey(context);
            JobKey parentJobKey = resolveParentJobKey(context);

            jobRunAuditRepository.recordJobCompletion(context, correlationId, rootJobKey, parentJobKey, jobException);

            if (jobException != null) {
                LOGGER.info("Parent job {} failed. Child dependency lookup skipped.", context.getJobDetail().getKey());
                return;
            }

            if (!dependencyRepository.hasActiveDependencies(context.getJobDetail().getKey())) {
                return;
            }

            List<JobDependency> dependencies =
                    dependencyRepository.findActiveDependencies(context.getJobDetail().getKey());
            for (JobDependency dependency : dependencies) {
                scheduleChildTrigger(context, dependency, correlationId, rootJobKey);
            }
        } catch (Exception ex) {
            LOGGER.error("Dependency lookup failed for parent job {}.", context.getJobDetail().getKey(), ex);
        }
    }

    private void scheduleChildTrigger(
            JobExecutionContext context, JobDependency dependency, String correlationId, JobKey rootJobKey)
            throws SchedulerException {
        if (dependency.triggerType() == TriggerType.SCHEDULE_ONLY) {
            LOGGER.info("Dependency {} from parent {} to child {} is schedule-only. Child trigger creation skipped.",
                    dependency.id(), dependency.parentJobKey(), dependency.childJobKey());
            return;
        }

        Trigger trigger = createTrigger(context, dependency.childJobKey(), dependency.triggerType(), correlationId, rootJobKey);
        TriggerKey triggerKey = trigger.getKey();
        if (context.getScheduler().checkExists(triggerKey)) {
            LOGGER.info("Skipping child trigger creation because {} already exists for dependency {}.",
                    triggerKey, dependency.id());
            return;
        }

        context.getScheduler().scheduleJob(trigger);
        LOGGER.info("Scheduled child job {} from parent {} using dependency {} with trigger {}",
                dependency.childJobKey(), dependency.parentJobKey(), dependency.id(), triggerKey);
    }

    private Trigger createTrigger(
            JobExecutionContext context,
            JobKey childJobKey,
            TriggerType triggerType,
            String correlationId,
            JobKey rootJobKey) {
        String triggerCorrelationId = buildTriggerCorrelationId(context, childJobKey, correlationId);
        TriggerBuilder<Trigger> builder = TriggerBuilder.newTrigger()
                .withIdentity(QuartzConfiguration.DOWNSTREAM_TRIGGER_NAME_PREFIX + "-" + triggerCorrelationId,
                        childJobKey.getGroup())
                .forJob(childJobKey)
                .usingJobData(QuartzConfiguration.PARENT_JOB_KEY, context.getJobDetail().getKey().toString())
                .usingJobData(QuartzConfiguration.PARENT_JOB_NAME_KEY, context.getJobDetail().getKey().getName())
                .usingJobData(QuartzConfiguration.PARENT_JOB_GROUP_KEY, context.getJobDetail().getKey().getGroup())
                .usingJobData(QuartzConfiguration.PARENT_FIRE_TIME_KEY, formatFireTime(context.getFireTime()))
                .usingJobData(QuartzConfiguration.CORRELATION_ID_KEY, correlationId);
        builder = builder
                .usingJobData(QuartzConfiguration.ROOT_JOB_NAME_KEY, rootJobKey.getName())
                .usingJobData(QuartzConfiguration.ROOT_JOB_GROUP_KEY, rootJobKey.getGroup());

        return switch (triggerType) {
            case FIRE_ONCE_IMMEDIATELY -> builder.startNow().build();
            case SCHEDULE_ONLY -> throw new IllegalArgumentException("Schedule-only dependencies do not create triggers.");
        };
    }

    private boolean shouldProcess(JobExecutionContext context) {
        try {
            return hasCorrelationId(context)
                    || dependencyRepository.hasActiveDependencies(context.getJobDetail().getKey());
        } catch (Exception ex) {
            LOGGER.error("Failed to evaluate listener filter for job {}.", context.getJobDetail().getKey(), ex);
            return false;
        }
    }

    private boolean hasCorrelationId(JobExecutionContext context) {
        String correlationId = context.getMergedJobDataMap().getString(QuartzConfiguration.CORRELATION_ID_KEY);
        return correlationId != null && !correlationId.isBlank();
    }

    private String resolveCorrelationId(JobExecutionContext context) {
        String correlationId = context.getMergedJobDataMap().getString(QuartzConfiguration.CORRELATION_ID_KEY);
        if (correlationId != null && !correlationId.isBlank()) {
            return correlationId;
        }

        String source = context.getFireInstanceId();
        if (source == null || source.isBlank()) {
            Date fireTime = context.getFireTime();
            String fallback = fireTime == null ? Instant.now().toString() : fireTime.toInstant().toString();
            source = context.getJobDetail().getKey() + "-" + fallback;
        }
        return source.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private String buildTriggerCorrelationId(JobExecutionContext context, JobKey childJobKey, String correlationId) {
        return (childJobKey + "-" + correlationId + "-" + resolveCorrelationId(context)).replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private JobKey resolveRootJobKey(JobExecutionContext context) {
        String rootJobName = context.getMergedJobDataMap().getString(QuartzConfiguration.ROOT_JOB_NAME_KEY);
        String rootJobGroup = context.getMergedJobDataMap().getString(QuartzConfiguration.ROOT_JOB_GROUP_KEY);
        if (rootJobName != null && !rootJobName.isBlank() && rootJobGroup != null && !rootJobGroup.isBlank()) {
            return JobKey.jobKey(rootJobName, rootJobGroup);
        }
        return context.getJobDetail().getKey();
    }

    private JobKey resolveParentJobKey(JobExecutionContext context) {
        String parentJobName = context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_JOB_NAME_KEY);
        String parentJobGroup = context.getMergedJobDataMap().getString(QuartzConfiguration.PARENT_JOB_GROUP_KEY);
        if (parentJobName != null && !parentJobName.isBlank() && parentJobGroup != null && !parentJobGroup.isBlank()) {
            return JobKey.jobKey(parentJobName, parentJobGroup);
        }
        return null;
    }

    private String formatFireTime(Date fireTime) {
        return fireTime == null ? Instant.now().toString() : fireTime.toInstant().toString();
    }
}
