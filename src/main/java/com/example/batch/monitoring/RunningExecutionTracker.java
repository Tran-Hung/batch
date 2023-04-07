package com.example.batch.monitoring;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Nam Tran Date Jul 18, 2017
 */
public class RunningExecutionTracker {
    private final Map<Long, String> runningExecutions = new ConcurrentHashMap<Long, String>();

    public void addRunningExecution(String jobName, Long executionId) {
        runningExecutions.put(executionId, jobName);
    }

    public void removeRunningExecution(Long executionId) {
        runningExecutions.remove(executionId);
    }

    public Set<Long> getAllRunningExecutionIds() {
        return new HashSet<Long>(runningExecutions.keySet());
    }

    public Set<Long> getRunningExecutionIdsForJobName(String jobName) {
        Set<Long> runningExecutionIds = new HashSet<Long>();
        for (Entry<Long, String> entry : runningExecutions.entrySet()) {
            if (entry.getValue().equals(jobName)) {
                runningExecutionIds.add(entry.getKey());
            }
        }
        return runningExecutionIds;
    }
}
