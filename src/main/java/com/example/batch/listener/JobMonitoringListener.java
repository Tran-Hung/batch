package com.example.batch.listener;

import com.example.batch.logging.DefaultJobLogFileNameCreator;
import com.example.batch.logging.JobLogFileNameCreator;
import com.example.batch.monitoring.RunningExecutionTracker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.batch.core.*;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Component;


import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * JobMonitoringListener
 *
 * @author Nam Tran Date Jul 12, 2017
 */
@Component
@Slf4j
public class JobMonitoringListener implements JobExecutionListener, StepExecutionListener, Ordered {
    public static final String JOBLOG_FILENAME = "jobLogFileName";
    private static final int DEFAULT_WIDTH = 80;
//    @Autowired
//    private RunningExecutionTracker runningExecutionTracker;
    private JobLogFileNameCreator jobLogFileNameCreator = new DefaultJobLogFileNameCreator();
    @Autowired
    private JdbcOperations jdbcTemplate;

//    @Autowired
//    private OmrRepository omrRepository;
//
//    @Autowired
//    protected OeCutofftimeControlRepository oeCutofftimeControlRepository;

    public void beforeJob(JobExecution jobExecution) {
        ExecutionContext jobContext = jobExecution.getExecutionContext();
        // set jobName and jobId and pass to execution context
        jobExecution.getExecutionContext().put("jobName", jobExecution.getJobInstance().getJobName());
        jobExecution.getExecutionContext().put("jobId", jobExecution.getId());

        insertValuesIntoMDC(jobExecution);

//        runningExecutionTracker.addRunningExecution(jobExecution.getJobInstance().getJobName(), jobExecution.getId());

        StringBuilder protocol = new StringBuilder();
        protocol.append(createFilledLine('-'));
        protocol.append("Job " + jobExecution.getJobInstance().getJobName() + " started with Job-Execution-Id "
                + jobExecution.getId() + " \n");
        protocol.append("Job-Parameter: \n");
        JobParameters jp = jobExecution.getJobParameters();
        for (Iterator<Entry<String, JobParameter>> iter = jp.getParameters().entrySet().iterator(); iter.hasNext(); ) {
            Entry<String, JobParameter> entry = iter.next();
            protocol.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        protocol.append(createFilledLine('-'));
        log.info(protocol.toString());

        // Update record_no
        String jobName = jobExecution.getJobInstance().getJobName();
        getOeCutOffTimeControl(jobContext, jobName);
//        switch (jobName) {
//            case OneBatchConstant.TRANSACTION_IMPORT_JOB:
//                jdbcTemplate.execute(OneBatchConstant.UPDATE_DWH_TEMP_PROD_TRANSACTION_RECORD_NO);
//                break;
//            case OneBatchConstant.UPDATE_BRAND_FROM_HUB_JOB:
//                jdbcTemplate.execute(OneBatchConstant.DELETE_UPDATE_BRAND_FAIL);
//                break;
//        }
    }

    public void afterJob(JobExecution jobExecution) {

//        runningExecutionTracker.removeRunningExecution(jobExecution.getId());
        ExecutionContext jobContext = jobExecution.getExecutionContext();

        if (BatchStatus.COMPLETED.equals(jobExecution.getStatus())) {
            updateOeCutOffTimeControl(jobContext, jobExecution.getJobInstance().getJobName());
        } else if (BatchStatus.FAILED.equals(jobExecution.getStatus())) {
            updateBatchFail(jobExecution);
        }

        StringBuilder logFile = new StringBuilder();
        logFile.append("\n");
        logFile.append(createFilledLine('*'));
        logFile.append(createFilledLine('-'));
        logFile.append("Protocol for " + jobExecution.getJobInstance().getJobName() + " \n");
        logFile.append("  Started:      " + jobExecution.getStartTime() + "\n");
        logFile.append("  Finished:     " + jobExecution.getEndTime() + "\n");
        logFile.append("  Exit-Code:    " + jobExecution.getExitStatus().getExitCode() + "\n");
        logFile.append("  Exit-Descr:   " + jobExecution.getExitStatus().getExitDescription() + "\n");
        logFile.append("  Status:       " + jobExecution.getStatus() + "\n");
        logFile.append("  Content of Job-ExecutionContext:\n");
        for (Entry<String, Object> entry : jobExecution.getExecutionContext().entrySet()) {
            logFile.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        logFile.append("  Job-Parameter: \n");
        JobParameters jp = jobExecution.getJobParameters();
        for (Iterator<Entry<String, JobParameter>> iter = jp.getParameters().entrySet().iterator(); iter.hasNext(); ) {
            Entry<String, JobParameter> entry = iter.next();
            logFile.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
        }
        logFile.append(createFilledLine('-'));
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            logFile.append("Step " + stepExecution.getStepName() + " \n");
            logFile.append("  ReadCount:    " + stepExecution.getReadCount() + "\n");
            logFile.append("  WriteCount:   " + stepExecution.getWriteCount() + "\n");
            logFile.append("  Commits:      " + stepExecution.getCommitCount() + "\n");
            logFile.append("  SkipCount:    " + stepExecution.getSkipCount() + "\n");
            logFile.append("  Rollbacks:    " + stepExecution.getRollbackCount() + "\n");
            logFile.append("  Started:      " + stepExecution.getStartTime() + "\n");
            logFile.append("  Finished:     " + stepExecution.getEndTime() + "\n");
            logFile.append("  Filter:       " + stepExecution.getFilterCount() + "\n");
            logFile.append("  Content of Step-ExecutionContext:\n");
            for (Entry<String, Object> entry : stepExecution.getExecutionContext().entrySet()) {
                logFile.append("  " + entry.getKey() + "=" + entry.getValue() + "\n");
            }
            logFile.append(createFilledLine('-'));
        }
        logFile.append(createFilledLine('*'));

        log.info("logFile: {}", logFile);

        removeValuesFromMDC();

    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        insertValuesIntoMDC(stepExecution.getJobExecution());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        removeValuesFromMDC();
        return null;
    }

    private void insertValuesIntoMDC(JobExecution jobExecution) {
        MDC.put(JOBLOG_FILENAME, jobLogFileNameCreator.getBaseName(jobExecution));
    }

    private void removeValuesFromMDC() {
        MDC.remove(JOBLOG_FILENAME);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Autowired(required = false)
    public void setJobLogFileNameCreator(JobLogFileNameCreator jobLogFileNameCreator) {
        this.jobLogFileNameCreator = jobLogFileNameCreator;
    }

    private String createFilledLine(char filler) {
        return StringUtils.leftPad("", DEFAULT_WIDTH, filler) + "\n";
    }

    private void getOeCutOffTimeControl(ExecutionContext jobContext, String jobName) {

//        if (!OneBatchConstant.BATCH_ID_CUT_OFF_TIME.contains(jobName)) {
//            return;
//        }
//        Date currentCutOffTime = new Date();;
//        Date lastCutOffTime;
//        OeCutofftimeControl oeCutofftimeControl = oeCutofftimeControlRepository.findOneByModuleIdAndBusinessId(jobName, CommonKeys.DEFAULT_BUSINESS_ID_NUMBER);
//
//        if (oeCutofftimeControl != null) {
//            lastCutOffTime = oeCutofftimeControl.getCurrentCutoffTime();
//        } else {
//            try {
//                lastCutOffTime = ConversionUtil.toDate("19000101", ConversionUtil.OE_DATE_SHORT);
//            } catch (Exception e) {
//                log.error("Error: ", e);
//                lastCutOffTime = null;
//            }
//        }
//
//        jobContext.put(OneBatchConstant.CUT_OFF_TIME.LAST, lastCutOffTime);
//        jobContext.put(OneBatchConstant.CUT_OFF_TIME.CURRENT, currentCutOffTime);
        return;
    }


    private void updateOeCutOffTimeControl(ExecutionContext jobContext, String jobName) {
//        if (!OneBatchConstant.BATCH_ID_CUT_OFF_TIME.contains(jobName)) {
//            return;
//        }
//        OeCutofftimeControl oeCutofftimeControl = oeCutofftimeControlRepository.findOneByModuleIdAndBusinessId(jobName, CommonKeys.DEFAULT_BUSINESS_ID_NUMBER);
//
//        Date lastCutOffTime = (Date) jobContext.get(OneBatchConstant.CUT_OFF_TIME.LAST);
//        Date currentCutOffTime = (Date) jobContext.get(OneBatchConstant.CUT_OFF_TIME.CURRENT);
//
//        if (oeCutofftimeControl == null) {
//            oeCutofftimeControl = new OeCutofftimeControl();
//            oeCutofftimeControl.setBusinessId(CommonKeys.DEFAULT_BUSINESS_ID_NUMBER);
//            oeCutofftimeControl.setModuleId(jobName);
//            oeCutofftimeControl.setModuleName(jobName);
//        }
//        oeCutofftimeControl.setCurrentCutoffTime(currentCutOffTime);
//        oeCutofftimeControl.setLastCutoffTime(lastCutOffTime);
//        oeCutofftimeControl.setLastCutoffBy(jobName);
//        oeCutofftimeControl.setCurrentCutoffBy(jobName);
//        oeCutofftimeControlRepository.save(oeCutofftimeControl);
        return;
    }

    private void updateBatchFail(JobExecution jobExecution) {
//        if (OneBatchConstant.ONE_MARKETING_REQUEST_JOB.equals(jobExecution.getJobInstance().getJobName())) {
//            omrRepository.uploadBatchFail(jobExecution.getJobId(), jobExecution.getJobParameters().getString("omrId"));
//        }
        return;
    }
}