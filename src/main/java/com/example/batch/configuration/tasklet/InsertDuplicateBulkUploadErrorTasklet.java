package com.example.batch.configuration.tasklet;

import com.example.batch.common.BatchErrorCode;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import javax.persistence.EntityManager;
import javax.persistence.Query;


@Slf4j
@Builder
@Data
public class InsertDuplicateBulkUploadErrorTasklet implements Tasklet {

    private EntityManager entityManager;

    private String table;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        JobExecution jobExecution = chunkContext.getStepContext().getStepExecution().getJobExecution();
        Long jobId = jobExecution.getId();

        StringBuilder sqlBuilder = new StringBuilder();

        sqlBuilder.append("INSERT INTO BULK_UPLOAD_ERROR ");
        sqlBuilder.append("(RECORD_NO, BULK_UPLOAD, BULK_UPLOAD_ID, LINE_NUMBER, LINE, ERROR_CODE, ERROR_MESSAGE, ETL_DATE, BATCH_NO) ");
        sqlBuilder.append("select SEQ_BULK_UPLOAD_ERROR.nextval,:bulkUpload,BULK_UPLOAD_ID,LINE_NUMBER,LINE,:errorCode,:errorMessage,ETL_DATE,BATCH_NO ");
        sqlBuilder.append("from " + table + " where record_no in ");
        sqlBuilder.append("(SELECT record_no FROM (SELECT ROW_NUMBER() OVER (PARTITION BY line ORDER BY record_no) AS rnum, line, record_no FROM " + table + " where batch_no =:jobId) WHERE rnum > 1 )");
        Query query = entityManager.createNativeQuery(sqlBuilder.toString());
        query.setParameter("bulkUpload", table);
        query.setParameter("jobId", jobId);
        query.setParameter("errorCode", BatchErrorCode.EC_DUPLICATE_LINE);
        query.setParameter("errorMessage", BatchErrorCode.EM_DUPLICATE_LINE);
        query.executeUpdate();
        sqlBuilder = new StringBuilder();
        sqlBuilder.append("delete from " + table + " where record_no in ( SELECT record_no FROM (SELECT ROW_NUMBER() OVER (PARTITION BY line ORDER BY record_no) AS rnum, line, record_no FROM " + table + " where batch_no =:jobId) WHERE rnum > 1 )");
        query = entityManager.createNativeQuery(sqlBuilder.toString());
        query.setParameter("jobId", jobId);
        query.executeUpdate();
        return RepeatStatus.FINISHED;
    }
}
