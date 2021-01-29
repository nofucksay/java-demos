package com.jyc.jdk8.stream;

import org.junit.Test;

import java.util.Optional;

public class OptionalTest {


    @Test
    private void testOptional(){
        Object ssss = Optional.ofNullable(null).orElse(1);
    }
}
