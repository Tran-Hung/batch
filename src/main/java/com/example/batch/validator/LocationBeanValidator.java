package com.example.batch.validator;

import com.example.batch.beans.LocationBean;
import com.example.batch.common.BatchErrorCode;
import com.example.batch.util.BatchUtil;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.util.Set;

@Component
public class LocationBeanValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return true;
    }

    @Override
    public void validate(Object target, Errors errors) {
        LocationBean bean = (LocationBean) target;
        if (bean.isValid()) {
            ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
            javax.validation.Validator validator = factory.getValidator();

            Set<ConstraintViolation<Object>> constraintViolations = validator.validate(bean);
            if (constraintViolations.size() > 0) {
                bean.setValid(false);
                errors.reject(BatchErrorCode.DATA_FORMAT_ERROR,
                        BatchUtil.buildErrorMessageDetail(constraintViolations));
                return;
            }

        }
    }
}
