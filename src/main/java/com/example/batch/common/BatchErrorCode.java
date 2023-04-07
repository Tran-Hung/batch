package com.example.batch.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Hai NV on 8/8/2017.
 */
public class BatchErrorCode {

    public final static String DATA_FORMAT_ERROR = "E001";

    public final static String DATA_VALUE_ERROR = "E002";
    public final static String DUPLICATE_DATA_ERROR = "E003";

    public static String BATCH_APPLICATION_ERROR = "BE04";

    public static String WRONG_TOTAL_AMOUNT_PARAMETER = "BE04";
    public static String WRONG_PARAMETER = "BE05";

    public static String EC_DUPLICATE_LINE = "BE06";
    public static String EM_DUPLICATE_LINE = "Duplicate Data";

    public static Map<String, String> ERROR_MAP = new HashMap<>();

    static {
        ERROR_MAP.put(DATA_FORMAT_ERROR, "INVALID DATA FORMAT PROVIDED");
        ERROR_MAP.put(DATA_VALUE_ERROR, "INVALID DATA VALUE PROVIDED");
        ERROR_MAP.put(DUPLICATE_DATA_ERROR, "DUPLICATE DATA PROVIDED");
    }

    public static String getMessageByErrorCode(String errorCode) {
        return ERROR_MAP.get(errorCode);
    }
}
