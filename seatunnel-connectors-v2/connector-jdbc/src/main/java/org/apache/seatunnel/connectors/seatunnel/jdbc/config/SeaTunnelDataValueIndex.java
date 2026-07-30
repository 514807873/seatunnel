package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.Data;

import java.io.Serializable;

@Data
public class SeaTunnelDataValueIndex implements Serializable {
    private static final long serialVersionUID = 2L;
    private String originColumnName;
    private String newColumnName;
    private SeaTunnelDataType seaTunnelDataType;
    private int originColumnValueIndex;
}
