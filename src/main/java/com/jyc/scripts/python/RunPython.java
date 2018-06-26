package com.jyc.scripts.python;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 * Created by jyc on 2018/1/23.
 */
public class RunPython {

    public static void main(String[] args) {

    }

    /**
     * java执行python脚本并获取输出
     */
    @Test
    public void test01(){
        try{

            Process p = Runtime.getRuntime().exec("python D:\\workspaces\\git\\py-work-plugins\\jyc\\ucar\\umeng.py ");
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String result = in.readLine();
            System.out.println("result is : " + result);
        }catch(Exception e){}
    }
}
