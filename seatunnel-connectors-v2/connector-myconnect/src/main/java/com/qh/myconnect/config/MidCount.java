package com.qh.myconnect.config;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
public class MidCount implements Serializable {
    private static final long serialVersionUID = -1L;
    private Long writeCount = 0L;
    private Long insertCount = 0L;
    private Long keepCount = 0L;
    private Long updateCount = 0L;
    private Long deleteCount = 0L;
    private Long errorCount = 0L;
    private Long qualityCount = 0L;

    public void plusQualityCount() {
        this.qualityCount++;
    }
}
