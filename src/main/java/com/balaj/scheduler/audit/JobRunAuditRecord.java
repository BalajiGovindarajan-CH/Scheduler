package com.balaj.scheduler.audit;

import org.quartz.JobKey;

public record JobRunAuditRecord(
        String correlationId,
        String fireInstanceId,
        JobKey rootJobKey,
        JobKey parentJobKey,
        JobKey jobKey,
        String triggerName,
        String triggerGroup,
        JobRunAuditStatus status,
        String errorMessage) {
}
