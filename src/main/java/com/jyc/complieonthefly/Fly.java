package com.jyc.complieonthefly;

import javax.tools.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class Fly {
    public static void main(String[] args) throws Exception {
        String source = "public class Main { public void test() {System.out.println(\"Hello World!\");return ;} }";
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        StringSourceJavaObject sourceObject = new StringSourceJavaObject("Main", source);
        Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(sourceObject);
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, Arrays.asList("-d", "./target/classes"), null, fileObjects);
        boolean result = task.call();
        if (result) {
            //生成对象
            Object obj = Class.forName("Main").newInstance();
            Class<? extends Object> cls = obj.getClass();
            //调用test方法
            Method m = cls.getMethod("test");
            String str = (String) m.invoke(obj);
            System.out.println(str);
//            System.out.println("编译成功。");
        }
    }

    static class StringSourceJavaObject extends SimpleJavaFileObject {

        private String content = null;

        public StringSourceJavaObject(String name, String content) throws URISyntaxException {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.content = content;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return content;
        }
    }
}
