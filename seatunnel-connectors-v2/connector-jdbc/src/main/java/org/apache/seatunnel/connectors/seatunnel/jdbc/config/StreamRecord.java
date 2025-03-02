package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class StreamRecord implements Serializable {
    private static final long serialVersionUID = -1L;
    private String seatunnelId;
    private String rq;
    private Long writeCount = 0L;
    private Long insertCount = 0L;
    private Long updateCount = 0L;
    private Long deleteCount = 0L;

    public StreamRecord(String seatunnelId) {
        this.seatunnelId = seatunnelId;
    }

    public void plusWriteCount() {
        this.writeCount++;
    }

    public void plusInsertCount() {
        this.insertCount++;
    }

    public void plusUpdateCount() {
        this.updateCount++;
    }

    public void plusDeleteCount() {
        this.deleteCount++;
    }

}
