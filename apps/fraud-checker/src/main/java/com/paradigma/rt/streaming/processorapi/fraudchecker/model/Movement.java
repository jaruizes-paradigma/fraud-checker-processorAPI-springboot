package com.paradigma.rt.streaming.processorapi.fraudchecker.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Movement {

    private String id;
    private String card;
    private float amount;
    private int origin;
    private String site;
    private String device;
    private String createdAt;

    @Override
    public String toString() {
        return "[id='" + id + '\'' +
                ", card='" + card + '\'' +
                ", amount=" + amount +
                ", origin=" + origin +
                ", site='" + site + '\'' +
                ", device='" + device + '\'' +
                ", createdAt='" + createdAt + '\'' +
                ']';
    }

}
