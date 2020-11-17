package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenhuiup
 * @create 2020-11-17 11:56
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private long ts;
    private double temp;

    @Override
    public String toString() {
        return id + ", " + ts + ", " + temp;
    }

}
