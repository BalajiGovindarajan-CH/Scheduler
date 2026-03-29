package com.balaj.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.balaj.scheduler.audit.JobRunAuditRecord;
import com.balaj.scheduler.audit.JobRunAuditStatus;
import com.balaj.scheduler.config.QuartzConfiguration;
import com.balaj.scheduler.dependency.JobDependency;
import com.balaj.scheduler.listener.TriggerType;
import org.quartz.JobKey;
import org.junit.jupiter.api.Test;

class DependentJobSchedulerTest {
    @Test
    void listenerConfigurationUsesExpectedConstants() {
        assertEquals("demo-downstream", QuartzConfiguration.DOWNSTREAM_JOB_GROUP);
        assertEquals("sampleLoggingJob", QuartzConfiguration.PRIMARY_JOB_NAME);
        assertEquals(TriggerType.FIRE_ONCE_IMMEDIATELY, TriggerType.valueOf("FIRE_ONCE_IMMEDIATELY"));
    }

    @Test
    void dependencyRecordUsesJobKeys() {
        JobDependency dependency = new JobDependency(
                1L,
                JobKey.jobKey("parent", "group-a"),
                JobKey.jobKey("child", "group-b"),
                TriggerType.FIRE_ONCE_IMMEDIATELY,
                true);

        assertEquals("group-a", dependency.parentJobKey().getGroup());
        assertEquals("child", dependency.childJobKey().getName());
    }

    @Test
    void auditRecordKeepsCorrelationIdAndHierarchy() {
        JobRunAuditRecord record = new JobRunAuditRecord(
                "corr-1",
                "fire-1",
                JobKey.jobKey("root", "root-group"),
                JobKey.jobKey("parent", "parent-group"),
                JobKey.jobKey("child", "child-group"),
                "trigger-a",
                "trigger-group",
                JobRunAuditStatus.SUCCEEDED,
                null);

        assertEquals("corr-1", record.correlationId());
        assertEquals("root", record.rootJobKey().getName());
        assertEquals("parent-group", record.parentJobKey().getGroup());
        assertEquals(JobRunAuditStatus.SUCCEEDED, record.status());
    }
}
