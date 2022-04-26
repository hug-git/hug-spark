package com.atguigu.es.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Stu {
    private String class_id;
    private String name;
    private String gender;
    private int age;
    private String favo1;
    private String favo2;
}
