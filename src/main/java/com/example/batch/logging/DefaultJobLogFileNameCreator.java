package com.example.batch.logging;

import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

@Component
public class DefaultJobLogFileNameCreator implements JobLogFileNameCreator {

    private final static String DEFAULT_EXTENSION = ".log";

    @Override
    public String getName(JobExecution jobExecution) {
        return getBaseName(jobExecution) + getExtension();
    }

    @Override
    public String getBaseName(JobExecution jobExecution) {
        return "batch-" + jobExecution.getJobInstance().getJobName() + "-" + jobExecution.getId();
    }

    @Override
    public String getExtension() {
        return DEFAULT_EXTENSION;
    }

    @Override
    public String getBaseName(String jobName, Long jobid) {
        return "batch-" + jobName + "-" + jobid;
    }

}