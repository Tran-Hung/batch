package com.example.batch.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

/**
 * @author Nam Tran Date Jul 13, 2017
 */
@Component
@Slf4j
public class CustomSkipListener implements SkipListener {
    @Override
    public void onSkipInRead(Throwable t) {
        log.info("onSkipInRead", t);
        log.info("------------------ Skipping because writing it caused the error: " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        log.info("onSkipInWrite", t);
        log.info("------------------ Skipping " + item + " because writing it caused the error: " + t.getMessage());
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        log.info("onSkipInProcess", t);
        log.info("------------------ Skipping " + item + " because processing it caused the error: " + t.getMessage());
    }
}
