package com.example.batch.controller;

import static com.example.batch.common.BulkUploadConstant.JOB_PARAM;
import static com.example.batch.common.BulkUploadConstant.BULK_UPLOAD;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;



@RestController
@Slf4j
@RequestMapping("/launcher")
public class JobLauncherController {

    @Autowired
    private JobOperator jobOperator;

    @GetMapping(value = "/job/{jobName}")
    public Long batch(@PathVariable String jobName, @RequestParam Map<String, String> params) throws Exception {
        params.put("name", RandomStringUtils.randomAlphabetic(8));

        if (StringUtils.isNotBlank(params.get(JOB_PARAM.BULK_UPLOAD_ID))) { // Add param if job upload.
            List<String> bulkParams = BULK_UPLOAD.get(jobName);
            if (bulkParams != null && bulkParams.size() == 3) {
                params.put(JOB_PARAM.OA_TABLE, bulkParams.get(0));
                params.put(JOB_PARAM.OA_COLUMN, bulkParams.get(1));
                params.put(JOB_PARAM.BULK_TABLE_NAME, bulkParams.get(2));
            }
        }

        StringBuilder param = new StringBuilder();
        Optional.ofNullable(params).orElseGet(Collections::emptyMap).entrySet().stream().forEach(map -> {
                    param.append(map.getKey()).append("=").append(map.getValue()).append(",");
                }
        );
        Long jobExecutionId = this.jobOperator.start(jobName, RandomStringUtils.randomAlphabetic(8));
        log.info("Job Execution Id : {}", jobExecutionId);
        return jobExecutionId;
    }

}
