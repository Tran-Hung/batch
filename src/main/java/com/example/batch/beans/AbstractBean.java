package com.example.batch.beans;

import lombok.Data;

@Data
public class AbstractBean {
    private String errorCode;

    private String errorMessage;

    private boolean isValid = true;

    private boolean flagInsertUpdate = false;

    private boolean flagDelete = false;

    private Long recordNo;

    private Long recordNoHis;

    private String line;

    private int lineNumber;

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

}
