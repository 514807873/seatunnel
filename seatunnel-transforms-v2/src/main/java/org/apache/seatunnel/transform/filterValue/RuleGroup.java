package org.apache.seatunnel.transform.filterValue;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class RuleGroup implements Serializable {
    private String logicalOperator;
    private List<Object> rules;
}


