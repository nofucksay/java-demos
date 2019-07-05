package com.jyc.utils;

/**
 * Created by jyc on 2018/10/10.
 */
public class MyUtils {
    public static String coalesce(String... array){
        for (String s : array) {
            if (s == null || "null".equals(s) || "".equals(s)) {
                continue;
            }else{
             return s;
            }
        }
        return null;
    }





    public static void main(String[] args) {
        // coalesce
        System.out.println(coalesce("", null , "null", "null", "jyc"));
    }
}
