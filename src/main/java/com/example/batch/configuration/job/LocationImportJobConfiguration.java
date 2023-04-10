package com.example.batch.configuration.job;

import com.example.batch.beans.LocationBean;
import com.example.batch.common.OneBatchConstant;
import com.example.batch.configuration.BatchConfiguration;
import com.example.batch.configuration.tasklet.BatchImportCompleteTasklet;
import com.example.batch.configuration.tasklet.InsertDuplicateBulkUploadErrorTasklet;
import com.example.batch.partitioner.BatchImportPartitioner;
import com.example.batch.processor.CustomItemValidatingProcessor;
import com.example.batch.processor.CustomSpringValidator;
import com.example.batch.reader.CustomLineMapper;
import com.example.batch.validator.LocationBeanValidator;
import com.example.batch.writer.LocationWriter;
import com.example.batch.writer.classifier.FileCustomClassifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.validator.SpringValidator;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.validation.Validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.batch.common.BulkUploadConstant.LOCATION_IMPORT.*;

@Configuration
@Slf4j
public class LocationImportJobConfiguration extends BatchConfiguration {

    @Autowired
    private LocationBeanValidator locationBeanValidator;

    @Autowired
    private LocationWriter locationWriter;

    @Autowired
    @Qualifier("bulkUploadErrorItemWriter")
    private JdbcBatchItemWriter<LocationBean> bulkUploadErrorItemWriter;

    @Bean
    public Job locationUploadJob() throws Exception {
        return jobBuilderFactory.get(OneBatchConstant.LOCATION_IMPORT_JOB)
                .incrementer(new RunIdIncrementer())
                .start(locationUploadFileToDbStep())
                .next(checkDuplicateLocationUpload())
                .next(locationUploadStep())
                .next(updateLocationStatusStep())
                .next(bulkUploadStatusStep())
                .listener(jobMonitoringListener)
                .build();
    }

    @Bean
    public Step locationUploadFileToDbStep() {
        return stepBuilderFactory.get("locationUploadFileToDbStep")
                .listener(jobMonitoringListener)
                .<LocationBean, LocationBean>chunk(chunkSize)
                .reader(locationUploadFileReader(null))
                .listener(customSkipListener)
                .processor(locationUploadValidateProcessor())
                .writer(classifierLocationUploadWriter())
                .taskExecutor(taskExecutorConfiguration.taskExecutor())
                .throttleLimit(10)
                .build();
    }

    @Bean
    public ClassifierCompositeItemWriter<LocationBean> classifierLocationUploadWriter() {
        ClassifierCompositeItemWriter<LocationBean> itemWriter = new ClassifierCompositeItemWriter<>();
        itemWriter.setClassifier(FileCustomClassifier.builder()
                .success(insertLocationUploadWriter(null, null))
                .error(bulkUploadErrorItemWriter)
                .build());
        return itemWriter;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<LocationBean> locationUploadFileReader(
            @Value("#{jobParameters['pathName']}") String pathName) {
        try {
            FlatFileItemReader<LocationBean> reader = new FlatFileItemReader<>();
            reader.setLinesToSkip(1);
            reader.setResource(inputStreamResource(pathName));
            reader.setLineMapper(locationUploadLineMapper());
            return reader;
        } catch (Exception e) {
            return null;
        }
    }

    @Bean
    public CustomLineMapper<LocationBean> locationUploadLineMapper() {
        CustomLineMapper<LocationBean> lineMapper = new CustomLineMapper<>();
        Map<String, LineTokenizer> tokenizers = new HashMap<>();
        DelimitedLineTokenizer bodyDelimitedLineTokenizer = new DelimitedLineTokenizer(DELIMITER);
        bodyDelimitedLineTokenizer.setNames(COLUMN_DETAIL);
        tokenizers.put("*", bodyDelimitedLineTokenizer);
        lineMapper.setTokenizers(tokenizers);

        Map<String, FieldSetMapper<LocationBean>> fieldSetMapperHashMap = new HashMap<>();
        BeanWrapperFieldSetMapper<LocationBean> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(LocationBean.class);

        fieldSetMapperHashMap.put("*", fieldSetMapper);
        lineMapper.setFieldSetMappers(fieldSetMapperHashMap);
        return lineMapper;
    }

    @Bean
    public ValidatingItemProcessor<LocationBean> locationUploadValidateProcessor() {
        return new CustomItemValidatingProcessor<>(locationUploadValidator());
    }

    @Bean
    public SpringValidator<LocationBean> locationUploadValidator() {
        List<Validator> listValidator = new ArrayList<>();
        listValidator.add(locationBeanValidator);
        return new CustomSpringValidator<>(listValidator);
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter insertLocationUploadWriter(@Value("#{jobExecutionContext['jobId']}") Long jobId,
                                                          @Value("#{jobParameters['bulkUploadId']}") String bulkUploadId) {
        ItemSqlParameterSourceProvider<LocationBean> sqlParameter = fileBean -> {
            BeanPropertySqlParameterSource source = new BeanPropertySqlParameterSource(fileBean);
            return source;
        };
        return new JdbcBatchItemWriterBuilder<LocationBean>()
                .dataSource(dataSource)
                .sql(String.format(FILE_TO_DB_SQL, bulkUploadId, jobId))
                .itemSqlParameterSourceProvider(sqlParameter)
                .build();
    }

    @Bean
    public Step checkDuplicateLocationUpload() {
        return stepBuilderFactory.get("checkDuplicateLocationUpload")
                .tasklet(InsertDuplicateBulkUploadErrorTasklet
                        .builder()
                        .table(TABLE)
                        .entityManager(entityManager)
                        .build())
                .build();
    }


    @Bean
    @Qualifier(value = "locationUploadStep")
    public Step locationUploadStep() {
        return stepBuilderFactory.get("locationUploadStep")
                .partitioner(locationUploadSlaveStep().getName(), locationUploadPartitioner())
                .step(locationUploadSlaveStep())
                .gridSize(gridSize)
                .taskExecutor(batchTaskExecutor())
                .build();
    }

    @Bean
    public BatchImportPartitioner locationUploadPartitioner() {
        return BatchImportPartitioner.builder()
                .columnName(KEY)
                .tableName(TABLE)
                .build()
                .dataSource(this.dataSource);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public Step locationUploadSlaveStep() {
        return stepBuilderFactory.get("locationUploadSlaveStep").<LocationBean, LocationBean>chunk(chunkSize)
                .reader(locationUploadReader(null, null, null))
                .writer(locationWriter)
                .faultTolerant()
                .skip(Exception.class)
                .skipPolicy(skipPolicy())
                .listener(customSkipListener)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<? extends LocationBean> locationUploadReader(
            @Value("#{jobExecutionContext['jobId']}") Long jobId,
            @Value("#{stepExecutionContext['minValue']}") String minValue,
            @Value("#{stepExecutionContext['maxValue']}") String maxValue) {

        JdbcPagingItemReader<LocationBean> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(this.dataSource);
        reader.setFetchSize(chunkSize);
        reader.setPageSize(chunkSize);
        reader.setRowMapper(new BeanPropertyRowMapper<>(LocationBean.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("*");
        queryProvider.setFromClause(TABLE);
        queryProvider.setWhereClause(" BATCH_NO = " + jobId + " AND " + KEY + " >= '" + minValue + "' AND  " + KEY + " <= '" + maxValue + "' ");

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("LINE_NUMBER", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    public Step updateLocationStatusStep() throws Exception {
        return stepBuilderFactory.get("updateLocationStatusStep")
                .tasklet(BatchImportCompleteTasklet
                        .builder()
                        .table(TABLE)
                        .isUpload(true)
                        .jdbc(new JdbcTemplate(dataSource))
                        .build())
                .build();
    }
}
