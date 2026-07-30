package org.apache.seatunnel.transform.filterValue;

import lombok.Data;

import java.io.Serializable;

@Data
public class Rule implements Serializable {
    private String name;
    private String value;
    private String operator;
}
