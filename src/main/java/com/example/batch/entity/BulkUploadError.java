package com.example.batch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;


@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Table(name = "BULK_UPLOAD_ERROR")
public class BulkUploadError implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "RECORD_NO")
    private Long recordNo;

    @Column(name = "BULK_UPLOAD")
    private String bulkUpload;

    @Column(name = "BULK_UPLOAD_ID")
    private String bulkUploadId;

    @Column(name = "LINE_NUMBER")
    private Long lineNumber;

    @Column(name = "LINE")
    private String line;

    @Column(name = "ERROR_CODE")
    private String errorCode;

    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;

    @Column(name = "ETL_DATE")
    private Date etlDate;

    @Column(name = "BATCH_NO")
    private String batchNo;

}