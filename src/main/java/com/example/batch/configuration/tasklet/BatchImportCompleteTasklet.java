package com.example.batch.configuration.tasklet;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcOperations;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Data
@Builder
public class BatchImportCompleteTasklet implements Tasklet {

    private final static List<String> TABLE_BAK_LIST = Arrays.asList("");
    private final static String INS_TABLE_BAK = "INSERT INTO %s ( %s ,BATCH_NO ) SELECT %s, %s FROM %s ";
    private final static String DEL_JOB_ID = "SELECT BATCH_NO FROM (SELECT  BATCH_NO, COUNT(1) OVER() AS BAK_NBR FROM %s GROUP BY BATCH_NO ORDER BY BATCH_NO ASC) WHERE ROWNUM=1 AND BAK_NBR > %s";
    private final static String DEL_TABLE_BAK = "DELETE FROM %s WHERE BATCH_NO = %s";
    private final static String TRUNC_TABLE = "DELETE FROM %s";

    private final static String WHERE = " WHERE %s";
    private JdbcOperations jdbc;
    private String table;
    private String column;

    private boolean isUpload;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        log.info("BatchImportCompleteTasklet - Post processing (insert BAK table and delete TMP table)");
        long jobId = chunkContext.getStepContext().getStepExecution().getJobExecution().getId();

        processDwh(jobId);

        return RepeatStatus.FINISHED;
    }

    private void processDwh(long jobId) {
        if (TABLE_BAK_LIST.contains(table)) {
            String bakNumber = "5";

            // insert bak
            String tableBak = table + "_BAK";

            jdbc.execute(String.format(INS_TABLE_BAK, tableBak, column, column, jobId, table));
            log.info("Backup data for table {} successful!", tableBak);

            // del bak
            try {
                Integer delJobId = jdbc.queryForObject(String.format(DEL_JOB_ID, tableBak, bakNumber), Integer.class);
                if (delJobId != null) {
                    jdbc.execute(String.format(DEL_TABLE_BAK, tableBak, delJobId));
                }
            } catch (Exception e) {
                log.info("Cant get jobId to delete {} [{}]", tableBak, e.getMessage());
            }
        }

        // del talbe
        String delTable = String.format(TRUNC_TABLE, table) +
                (isUpload ? String.format(WHERE, String.format(" batch_no = %s", jobId)) : "");
        jdbc.execute(delTable);
    }

}
