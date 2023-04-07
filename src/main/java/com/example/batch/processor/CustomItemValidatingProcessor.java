package com.example.batch.processor;

import com.example.batch.beans.AbstractBean;
import com.example.batch.exceprion.BatchException;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.Validator;

/**
 * Created by Hai NV on 8/8/2017.
 */
public class CustomItemValidatingProcessor<T extends AbstractBean> extends ValidatingItemProcessor<T> {

    private final Validator validator;

    @SuppressWarnings("unchecked")
    public CustomItemValidatingProcessor(Validator validator) {
        this.validator = validator;
        super.setValidator(this.validator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T process(T item) throws BatchException {
        if (item.isValid()) {
            try {
                validator.validate(item);
            } catch (BatchException e) {
                item.setErrorCode(e.getErrorCode());
                item.setErrorMessage(e.getMessage());
                item.setValid(false);
            }
        }
        return item;
    }
}
