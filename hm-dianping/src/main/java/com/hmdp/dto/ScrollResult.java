package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    private List<?> list; //通用的
    private Long minTime;//最小时间
    private Integer offset;//偏移量
}
