package com.example.batch.reader;

import com.example.batch.beans.AbstractBean;
import com.example.batch.common.BatchErrorCode;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;

public class CustomLineMapper<T extends AbstractBean> extends PatternMatchingCompositeLineMapper<T> {

    @Override
    public T mapLine(String line, int lineNumber) {
        T bean;
        try {
            bean = super.mapLine(line, lineNumber);
        } catch (Exception e) {
            bean = (T) new AbstractBean();
            bean.setValid(false);
            bean.setErrorCode(BatchErrorCode.DATA_FORMAT_ERROR);
            bean.setErrorMessage("Can't parse: " + BatchErrorCode.getMessageByErrorCode(BatchErrorCode.DATA_FORMAT_ERROR));
        }

        bean.setLine(line);
        bean.setLineNumber(lineNumber);

        return bean;
    }
}
