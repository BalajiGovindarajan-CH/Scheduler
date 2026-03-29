package com.balaj.scheduler.dependency;

import com.balaj.scheduler.listener.TriggerType;
import org.quartz.JobKey;

public record JobDependency(
        long id,
        JobKey parentJobKey,
        JobKey childJobKey,
        TriggerType triggerType,
        boolean active) {
}
