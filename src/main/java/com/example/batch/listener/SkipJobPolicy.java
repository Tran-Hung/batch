package com.example.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

/**
 * @author Nam Tran Date Jul 13, 2017
 */
@Slf4j
public class SkipJobPolicy implements SkipPolicy {
    private int skipLimit;

    @Override
    public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
        if (t instanceof Exception) {
            log.warn("----------------- Skip customer....:" + t, t);

            return true;
        } else {
            return false;
        }
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }
}