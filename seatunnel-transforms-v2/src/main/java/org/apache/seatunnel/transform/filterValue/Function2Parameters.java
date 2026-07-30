package org.apache.seatunnel.transform.filterValue;

@FunctionalInterface
public interface Function2Parameters<J, D, R> {
    R apply(J j, D d);
}
