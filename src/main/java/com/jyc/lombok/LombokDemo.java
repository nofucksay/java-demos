package com.jyc.lombok;

import lombok.*;

@Data
@Builder
public class LombokDemo {

    @NonNull
    private int id ;

    @NonNull
    private String name;

    public static void main(@NonNull String[] args) {
        LombokDemo lombokDemo1 = new LombokDemo(1, "");
        LombokDemo name = LombokDemo.builder().id(1).name("name").build();

        String s = "s";

        name.start(null);
    }

    private void start(@NonNull Object o) {
    }

}
