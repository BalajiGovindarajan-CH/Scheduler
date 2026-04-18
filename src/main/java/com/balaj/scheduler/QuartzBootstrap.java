package com.balaj.scheduler;

import com.balaj.scheduler.audit.JobRunAuditRepository;
import com.balaj.scheduler.config.QuartzConfiguration;
import com.balaj.scheduler.db.DatabaseSchemaInitializer;
import com.balaj.scheduler.dependency.JobDependencyRepository;
import com.balaj.scheduler.listener.DatabaseBackedChildTriggeringListener;
import com.balaj.scheduler.listener.TriggerType;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class QuartzBootstrap {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzBootstrap.class);

    private QuartzBootstrap() {
    }

    public static void main(String[] args) throws Exception {
        Properties quartzProperties = QuartzConfiguration.loadQuartzProperties();
        DatabaseSchemaInitializer.ensureSchema(quartzProperties);

        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        JobDependencyRepository dependencyRepository = new JobDependencyRepository(quartzProperties);
        JobRunAuditRepository jobRunAuditRepository = new JobRunAuditRepository(quartzProperties);
        JobDetail jobDetail = QuartzConfiguration.createPrimaryJobDetail();
        JobDetail childJobDetail = QuartzConfiguration.createDownstreamJobDetail();
        JobDetail scheduledDependentJobDetail = QuartzConfiguration.createScheduledDependentJobDetail();
        Trigger trigger = QuartzConfiguration.createPrimaryTrigger();
        Trigger scheduledDependentTrigger = QuartzConfiguration.createScheduledDependentTrigger();

        registerDurableJobIfMissing(scheduler, childJobDetail);
        registerDependencyIfMissing(dependencyRepository, jobDetail, childJobDetail, TriggerType.FIRE_ONCE_IMMEDIATELY);
        registerDependencyIfMissing(dependencyRepository, jobDetail, scheduledDependentJobDetail, TriggerType.SCHEDULE_ONLY);
        registerScheduleIfMissing(scheduler, jobDetail, trigger);
        registerScheduleIfMissing(scheduler, scheduledDependentJobDetail, scheduledDependentTrigger);
        attachCommonDependencyListener(scheduler, dependencyRepository, jobRunAuditRepository);
        scheduler.start();

        Date nextFireTime = scheduler.getTrigger(trigger.getKey()).getNextFireTime();
        LOGGER.info("Scheduler started. Next fire time for {} is {}", trigger.getKey(), nextFireTime);
        LOGGER.info("Quartz is using H2 at {}", QuartzConfiguration.getJdbcUrl(quartzProperties));
        LOGGER.info("Press Ctrl+C to stop the scheduler.");

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("Shutting down Quartz scheduler.");
                scheduler.shutdown(true);
            } catch (SchedulerException ex) {
                LOGGER.error("Scheduler shutdown failed.", ex);
            } finally {
                shutdownLatch.countDown();
            }
        }));

        shutdownLatch.await();
    }

    private static void registerScheduleIfMissing(Scheduler scheduler, JobDetail jobDetail, Trigger trigger)
            throws SchedulerException {
        if (!scheduler.checkExists(jobDetail.getKey())) {
            scheduler.scheduleJob(jobDetail, trigger);
            LOGGER.info("Scheduled job {} with trigger {}", jobDetail.getKey(), trigger.getKey());
            return;
        }

        if (!scheduler.checkExists(trigger.getKey())) {
            scheduler.scheduleJob(trigger);
            LOGGER.info("Attached missing trigger {} to existing job {}", trigger.getKey(), jobDetail.getKey());
            return;
        }

        LOGGER.info("Job {} and trigger {} already exist in the store.", jobDetail.getKey(), trigger.getKey());
    }

    private static void registerDurableJobIfMissing(Scheduler scheduler, JobDetail jobDetail) throws SchedulerException {
        if (scheduler.checkExists(jobDetail.getKey())) {
            LOGGER.info("Durable job {} already exists in the store.", jobDetail.getKey());
            return;
        }

        scheduler.addJob(jobDetail, false);
        LOGGER.info("Registered durable job {} without a startup trigger.", jobDetail.getKey());
    }

    private static void registerDependencyIfMissing(
            JobDependencyRepository dependencyRepository,
            JobDetail parentJobDetail,
            JobDetail childJobDetail,
            TriggerType triggerType)
            throws java.sql.SQLException {
        dependencyRepository.upsertDependency(
                parentJobDetail.getKey(),
                childJobDetail.getKey(),
                triggerType);
    }

    private static void attachCommonDependencyListener(
            Scheduler scheduler,
            JobDependencyRepository dependencyRepository,
            JobRunAuditRepository jobRunAuditRepository)
            throws SchedulerException {
        scheduler.getListenerManager().addJobListener(
                new DatabaseBackedChildTriggeringListener(dependencyRepository, jobRunAuditRepository));
    }
}
