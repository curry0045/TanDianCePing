package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

//实现滚动查询
@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime;
    private Integer offset;
}
