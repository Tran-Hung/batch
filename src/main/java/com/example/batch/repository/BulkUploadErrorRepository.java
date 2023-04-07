package com.example.batch.repository;

import com.example.batch.entity.BulkUploadError;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BulkUploadErrorRepository extends JpaRepository<BulkUploadError, Long> {
    List<BulkUploadError> findByBulkUploadAndBulkUploadIdAndBatchNo(String bulkUpload, String bulkUploadId, String batchNo);
}
