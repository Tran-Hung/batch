package com.example.batch.exceprion;

import lombok.Data;

/**
 * @author Nam Tran Date Jul 13, 2017
 */
@Data
public class BatchException extends RuntimeException {

    private String errorCode;

    private String message;

    public BatchException() {
        super();
    }

    public BatchException(String errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public BatchException(String message) {
        super(message);
    }

    public BatchException(String message, Throwable cause) {
        super(message, cause);
    }

}
