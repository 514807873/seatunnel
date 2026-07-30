package org.apache.seatunnel.transform.http;

import lombok.Data;

import java.io.Serializable;

@Data
public class HttpParam implements Serializable {

    private static final long serialVersionUID = -1L;
    private String name;
    private String type;
    private String value;
}
