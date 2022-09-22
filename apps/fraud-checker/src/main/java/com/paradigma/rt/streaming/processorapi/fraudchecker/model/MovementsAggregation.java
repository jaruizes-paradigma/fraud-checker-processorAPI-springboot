package com.paradigma.rt.streaming.processorapi.fraudchecker.model;

import lombok.Data;

import java.util.HashSet;
import java.util.Set;

@Data
public class MovementsAggregation {

    private String card;
    private String lastMovementTimestamp;
    private Set<Movement> movements;

    public MovementsAggregation(){
        this.movements = new HashSet<>();
    }

    public void addMovement(Movement movement){
        this.card = movement.getCard();
        this.movements.add(movement);
        this.lastMovementTimestamp = movement.getCreatedAt();
    }
}