package com.example.batch.writer.classifier;

import com.example.batch.beans.AbstractBean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

/**
 * Created by TrungNT on 26-Jan-18
 */
@Jacksonized
@AllArgsConstructor
@Builder
public class FileCustomClassifier<T extends AbstractBean> implements Classifier<AbstractBean, ItemWriter<T>> {
    private ItemWriter<T> success;

    private ItemWriter<T> error;

    @Override
    public ItemWriter<T> classify(AbstractBean classifiable) {
        if (!classifiable.isValid()) {
            return error;
        }

        return success;
    }
}
