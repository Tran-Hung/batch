package com.example.batch.configuration;

import com.example.batch.beans.AbstractBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;

@Configuration
@Slf4j
public class BulkUploadConfig extends BatchConfiguration {

    private static String INSERT_SQL = "INSERT INTO BULK_UPLOAD_ERROR " +
            "(RECORD_NO, BULK_UPLOAD, BULK_UPLOAD_ID, LINE_NUMBER, LINE, ERROR_CODE, ERROR_MESSAGE, ETL_DATE, BATCH_NO) " +
            "VALUES (SEQ_BULK_UPLOAD_ERROR.nextval, '%s', '%s', :lineNumber, :line, :errorCode, :errorMessage, current_timestamp, %s)";

    @Bean
    @StepScope
    public JdbcBatchItemWriter bulkUploadErrorItemWriter(
            @Value("#{jobParameters['bulkUpload']}") String bulkUpload,
            @Value("#{jobParameters['bulkUploadId']}") String bulkUploadId,
            @Value("#{jobExecutionContext['jobId']}") Long jobId) {
        ItemSqlParameterSourceProvider sqlParameter = fileBean -> {
            BeanPropertySqlParameterSource source = new BeanPropertySqlParameterSource(fileBean);
            return source;
        };
        return new JdbcBatchItemWriterBuilder<AbstractBean>()
                .dataSource(dataSource)
                .sql(String.format(INSERT_SQL, bulkUpload, bulkUploadId, jobId))
                .itemSqlParameterSourceProvider(sqlParameter)
                .build();
    }
}
