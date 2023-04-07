package com.example.batch.logging;

import org.springframework.batch.core.JobExecution;

/**
 * Components implementing this interface specify the name of the job run
 * specific log file. Data from the JobExecution may be used to compose the
 * name. There are also methods to get the specific parts (basename and
 * extension) of the filename. Default implementation used when there's no other
 * Spring bean implementing this interface is the
 * {@link DefaultJobLogFileNameCreator}.
 *
 */
public interface JobLogFileNameCreator {

    String getName(JobExecution jobExecution);

    String getBaseName(JobExecution jobExecution);

    String getBaseName(String jobName, Long jobid);

    String getExtension();

}