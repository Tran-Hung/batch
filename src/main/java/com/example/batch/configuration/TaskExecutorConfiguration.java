package com.example.batch.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class TaskExecutorConfiguration {

    @Value("${BATCH_THREAD_MAX_POOL}")
    private int maxPoolSize;

    @Value("${BATCH_THREAD_CORE_POOL}")
    private int corePoolSize;

    @Value("${BATCH_QUEUE_CAPACITY}")
    private int queueCapacity;

    @Value("${taskExecutor.thread.timeout}")
    private int threadTimeOut;

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(corePoolSize);
        taskExecutor.setQueueCapacity(queueCapacity);
        taskExecutor.setMaxPoolSize(maxPoolSize);
        taskExecutor.setKeepAliveSeconds(threadTimeOut);
        taskExecutor.afterPropertiesSet();
        taskExecutor.setThreadNamePrefix("task-async-");
        return taskExecutor;
    }

}
