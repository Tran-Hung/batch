package com.example.batch.processor;

import com.example.batch.common.BatchErrorCode;
import com.example.batch.exceprion.BatchException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.validator.SpringValidator;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.validation.Validator;

import java.util.List;

/**
 * Created by Hai NV on 8/8/2017.
 */
@Slf4j
public class CustomSpringValidator<T> extends SpringValidator<T> {

    private final List<Validator> listValidator;

    public CustomSpringValidator(List<Validator> listValidator) {
        this.listValidator = listValidator;
    }

    @Override
    public void validate(T item) throws BatchException {
        BeanPropertyBindingResult errors = new BeanPropertyBindingResult(item, "item");
        StringBuilder errorMessage = new StringBuilder();
        String errorCode = null;

        for (Validator validator : listValidator) {
            try {
                validator.validate(item, errors);
            } catch (Exception e) {
                log.error("{}", validator.getClass().getSimpleName(), e);
                throw new BatchException(BatchErrorCode.DATA_VALUE_ERROR, e.getMessage());
            }

            if (errors.hasErrors()) {
                List<ObjectError> errorList = errors.getAllErrors();
                for (ObjectError objectError : errorList) {
                    errorMessage.append(objectError.getDefaultMessage());
                    errorCode = objectError.getCode();
                }
                log.error("{}: {}", validator.getClass().getSimpleName(), errorMessage);
                throw new BatchException(errorCode, errorMessage.toString());
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // do nothing here
    }
}
