package com.example.batch.writer.classifier;

import com.example.batch.beans.AbstractBean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.jackson.Jacksonized;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

/**
 * @author Nam Tran Date Jul 14, 2017
 */
@Jacksonized
@AllArgsConstructor
@Builder
public class CustomClassifier<T extends AbstractBean> implements Classifier<AbstractBean, ItemWriter<T>> {

    private ItemWriter<T> validInsertItemWriter;

    private ItemWriter<T> validUpdateItemWriter;

    private ItemWriter<T> invalidItemWriter;

    private ItemWriter<T> validDeleteItemWriter;

    public CustomClassifier(ItemWriter<T> validItemWriter, ItemWriter<T> invalidItemWriter) {
        this.validInsertItemWriter = validItemWriter;
        this.invalidItemWriter = invalidItemWriter;
    }

    @Override
    public ItemWriter<T> classify(AbstractBean classifiable) {
        if (!classifiable.isValid()) {
            return invalidItemWriter;
        } else if (classifiable.isFlagInsertUpdate()) {
            return validInsertItemWriter;
        } else if (classifiable.isFlagDelete()) {
            return validDeleteItemWriter;
        } else {
            return validUpdateItemWriter;
        }
    }
}
