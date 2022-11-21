package com.code.stream.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author
 */
@AllArgsConstructor
@NoArgsConstructor
public class UrlViewCount {
    public String url;
    public Long count;
    public Long winStart;
    public Long winEnd;

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", winStart=" + new Timestamp(winStart) +
                ", winEnd=" + new Timestamp(winEnd) +
                '}';
    }
}
