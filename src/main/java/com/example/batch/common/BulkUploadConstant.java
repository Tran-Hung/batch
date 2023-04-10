package com.example.batch.common;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BulkUploadConstant {

    public interface JOB_PARAM {
        String OA_TABLE = "oaTableName";
        String OA_COLUMN = "oaColumnKey";
        String BULK_TABLE_NAME = "bulkUpload";

        String BULK_UPLOAD_ID = "bulkUploadId";
    }

    public static Map<String, List<String>> BULK_UPLOAD = ImmutableMap.<String, List<String>>builder()
            .put(LOCATION_IMPORT.JOB_NAME, LOCATION_IMPORT.PARAM)
            .build();

    public interface LOCATION_IMPORT {
        String JOB_NAME = OneBatchConstant.LOCATION_IMPORT_JOB;
        String OA_TABLE_NAME = "LOCATION_UPLOAD";
        String OA_COLUMN_KEY = "id";

        String TABLE = "BULK_UPLOAD_LOCATION";
        String KEY = "location_id";
        List<String> PARAM = Arrays.asList(OA_TABLE_NAME, OA_COLUMN_KEY, TABLE);

        String DELIMITER = ",";
        String[] COLUMN_DETAIL = new String[]{"locationId", "name"};
        String FILE_TO_DB_SQL = "INSERT INTO BULK_UPLOAD_LOCATION (BULK_UPLOAD_ID,LINE_NUMBER,ETL_DATE,LINE,location_id,NAME,BATCH_NO) " +
                " VALUES ('%s',:lineNumber,current_timestamp,:line,:locationId,:name,%s)";    }
}
