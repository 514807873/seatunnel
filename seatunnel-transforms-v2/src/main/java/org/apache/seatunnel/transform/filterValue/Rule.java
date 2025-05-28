package org.apache.seatunnel.transform.filterValue;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Rule implements Serializable {
    private String name;
    private String value;
    private String operator;
    private String logicalOperator;
    private List<Rule> rules;
}
