package com.balaj.scheduler;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.balaj.scheduler.config.QuartzConfiguration;
import org.junit.jupiter.api.Test;

class QuartzConfigurationTest {
    @Test
    void loadsQuartzProperties() throws Exception {
        assertNotNull(QuartzConfiguration.loadQuartzProperties());
    }
}
