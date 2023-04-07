package com.example.batch.beans;

import lombok.Data;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotBlank;

@Data
public class LocationBean extends AbstractBean {

    @NotBlank(message = "Merchant Id must be not null")
    @Length(max = 30, message = "Invalid Merchant ID format")
    private String locationId;

    @Length(max = 300, message = "Invalid Merchant Name format")
    private String name;
}
