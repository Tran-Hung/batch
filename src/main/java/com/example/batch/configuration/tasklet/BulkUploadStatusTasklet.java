package com.example.batch.configuration.tasklet;

import com.example.batch.common.CommonKeys;
import com.example.batch.entity.BulkUploadError;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.example.batch.repository.BulkUploadErrorRepository;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
@StepScope
@Slf4j
public class BulkUploadStatusTasklet implements Tasklet {
    @Value("#{jobParameters['oaTableName']}")
    private String oaTableName;

    @Value("#{jobParameters['oaColumnKey']}")
    private String oaColumnKey;

    @Value("#{jobParameters['bulkUpload']}")
    private String bulkUpload;

    @Value("#{jobParameters['bulkUploadId']}")
    private String id;

    @Value("#{jobExecutionContext['jobId']}")
    private Long batchNo;

    private EntityManager entityManager;

    @Autowired
    private BulkUploadErrorRepository bulkUploadErrorRepository;


    private static String SQL_UPDATE = "update %s set batch_no = :batchNo, error_file = :errorFile, " +
            "processing_status = 'D', last_update_by = 'BATCH', last_update_date = current_timestamp where %s = :id";

    private static String SQL_GET_PROCESSING_STATUS = "SELECT PROCESSING_STATUS FROM %s WHERE %s = :id AND STATUS = 'A' ";

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        try {

            List<BulkUploadError> bulkUploadErrorList = bulkUploadErrorRepository.findByBulkUploadAndBulkUploadIdAndBatchNo(bulkUpload, id, String.valueOf(batchNo));
            String errorPath = "";
            if (bulkUploadErrorList.size() > 0) {
                errorPath = createErrorFile(bulkUploadErrorList);
            }

            updateStatusBatch(errorPath);
        } catch (Exception e) {
            log.info("error : ", e);
        }

        return RepeatStatus.FINISHED;
    }

    private String createErrorFile(List<BulkUploadError> errors) throws IOException {
        String fileName = StringUtils.join("ErrorFile", batchNo, ".error");
        File fileOut = new File(fileName);
        if (!fileOut.createNewFile()) {
            throw new IOException("Can't create new file!");
        }
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileOut), StandardCharsets.UTF_8))) {
            for (BulkUploadError error : errors) {
                out.newLine();
                out.append(line(error));
            }
            out.flush();
        }

        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(CommonKeys.DEFAULT_FORMAT_DATE_FILE_NAME);

        String path = CommonKeys.DIR_FILES + CommonKeys.FILE_SEPARATOR + sdf.format(now);

        return path + CommonKeys.FILE_SEPARATOR + fileName;
    }

    private void updateStatusBatch(String error) throws Exception {

        Date runTime = new Date();

        String processStatus = getProcessStatus(oaTableName, oaColumnKey, id);

        while (!CommonKeys.PENDING_STATUS.equals(processStatus)) {
            Thread.sleep(15 * 1000);
            processStatus = getProcessStatus(oaTableName, oaColumnKey, id);
            if (new Date().after(DateUtils.addMinutes(runTime, 5))) {
                updateStatus(oaTableName, oaColumnKey, batchNo, error, id);
                throw new Exception("update status time out.");
            }
        }

        updateStatus(oaTableName, oaColumnKey, batchNo, error, id);
    }

    private void updateStatus(String oaTableName, String oaColumnKey, Long batchNo, String error, String id) {
        Query query = entityManager.createNativeQuery(String.format(SQL_UPDATE, oaTableName, oaColumnKey));
        query.setParameter("batchNo", batchNo);
        query.setParameter("errorFile", error);
        query.setParameter("id", id);
        query.executeUpdate();
    }

    private String getProcessStatus(String oaTableName, String oaColumnKey, String id) {
        Query query = entityManager.createNativeQuery(String.format(SQL_GET_PROCESSING_STATUS, oaTableName, oaColumnKey));
        query.setParameter("id", id);
        List<String> result = query.getResultList();
        return result.isEmpty() ? "" : result.get(0);
    }

    private String line(BulkUploadError error) {
        return StringUtils.joinWith(CommonKeys.PIPE, error.getLineNumber(), error.getLine(), error.getErrorCode(), error.getErrorMessage());
    }
}
