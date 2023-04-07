package com.example.batch.configuration;

import com.example.batch.configuration.tasklet.BulkUploadStatusTasklet;
import com.example.batch.listener.CustomSkipListener;
import com.example.batch.listener.JobMonitoringListener;
import com.example.batch.listener.SkipJobPolicy;
import com.example.batch.logging.JobLogFileNameCreator;
import com.example.batch.monitoring.RunningExecutionTracker;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Nam Tran Date Jul 13, 2017
 */
@Configuration
public class BatchConfiguration implements ApplicationContextAware {

    public static String UPLOAD;
    @Autowired
    public JobExplorer jobExplorer;
    @Autowired
    public JobRepository jobRepository;
    @Autowired
    public JobRegistry jobRegistry;
    @Autowired
    public JobLauncher jobLauncher;
    @Autowired
    protected TaskExecutorConfiguration taskExecutorConfiguration;
    @Autowired
    protected JobBuilderFactory jobBuilderFactory;
    @Autowired
    protected StepBuilderFactory stepBuilderFactory;
    @Autowired
    protected DataSource dataSource;
    @Autowired
    protected JobMonitoringListener jobMonitoringListener;
    @Autowired
    protected CustomSkipListener customSkipListener;
    @Autowired
    protected JobLogFileNameCreator jobLogFileNameCreator;
    @Value("${skip.limit}")
    protected int skipLimit;

    @Value("${data.chunk.size}")
    protected int chunkSize;

    @Value("${BATCH_THREAD_SIZE}")
    protected int gridSize;

    @Value("${partition.file.grid.size}")
    protected int fileGridSize;

    private ApplicationContext applicationContext;
    @Value("${BATCH_THREAD_MAX_POOL}")
    private int maxPoolSize;
    @Value("${BATCH_THREAD_CORE_POOL}")
    private int corePoolSize;
    @Value("${BATCH_QUEUE_CAPACITY}")
    private int queueCapacity;
    @Value("${taskExecutor.thread.timeout}")
    private int threadTimeOut;

    protected EntityManager entityManager;

    @Autowired
    private BulkUploadStatusTasklet bulkUploadStatusTasklet;


    @Value("${upload.folder}")
    public void setUpload(String uploadFolder) {
        if (StringUtils.isNotBlank(uploadFolder)) {
            UPLOAD = uploadFolder;
        } else {
            UPLOAD = System.getProperty("user.home");
        }

    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistrar() throws Exception {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();

        registrar.setJobRegistry(this.jobRegistry);
        registrar.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
        registrar.afterPropertiesSet();

        return registrar;
    }

    @Bean
    public JobOperator jobOperator() throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();

        simpleJobOperator.setJobLauncher(simpleJobLauncher());
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(this.jobRepository);
        simpleJobOperator.setJobExplorer(this.jobExplorer);
        simpleJobOperator.setJobRegistry(this.jobRegistry);
        simpleJobOperator.afterPropertiesSet();

        return simpleJobOperator;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean
    public SkipJobPolicy skipPolicy() {
        SkipJobPolicy skipPolicy = new SkipJobPolicy();
        skipPolicy.setSkipLimit(skipLimit);
        return skipPolicy;
    }

    public InputStreamResource inputStreamResource(String path) throws IOException {
        InputStream file = new FileInputStream("/sharedata/ols/datas/" + path);
        return file == null ? null : new InputStreamResource(file);
    }

    @Bean
    public RunningExecutionTracker runningExecutionTracker() {
        return new RunningExecutionTracker();
    }

    @Bean
    public TaskExecutor batchTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(corePoolSize);
        taskExecutor.setQueueCapacity(queueCapacity);
        taskExecutor.setMaxPoolSize(maxPoolSize);
        taskExecutor.setKeepAliveSeconds(threadTimeOut);
        taskExecutor.setThreadNamePrefix("batch-task-");
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    public JobLauncher simpleJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    @Bean
    public Step bulkUploadStatusStep() {
        return stepBuilderFactory.get("bulkUploadStatusStep")
                .tasklet(bulkUploadStatusTasklet)
                .build();
    }

}
