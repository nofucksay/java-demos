package com.jyc.complieonthefly;

import org.junit.Test;

import javax.tools.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author jiayuchen
 */
public class Fly {

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


    public static boolean compliesOnTheFly(String sourceCode, String className) throws URISyntaxException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        StringSourceJavaObject sourceObject = new StringSourceJavaObject(className, sourceCode);
        Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(sourceObject);
        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, Arrays.asList("-d", Fly.class.getProtectionDomain().getCodeSource().getLocation().getPath()), null, fileObjects);
        return task.call();
    }

    public static Object invokeMethodOnTheFly(String className, String methodName, Class[] paramType, Object[] args) throws Exception {
        Object obj = Class.forName(className).newInstance();
        Class<? extends Object> cls = obj.getClass();
        Method m = Objects.isNull(paramType) ?  cls.getMethod(methodName) : cls.getMethod(methodName, paramType);
        return m.invoke(obj, args);
    }

    @Test
    public void test01() throws Exception {
        String sourceCode = "package com.jyc.fly;public class TestFly { public void test() {System.out.println(\"Hello World!\");return ;} public String test02(String name) {System.out.println(name);return name;}}";
        String className = "com.jyc.fly.TestFly";
        if (compliesOnTheFly(sourceCode, "TestFly")){
            Object result = invokeMethodOnTheFly(className, "test", null, null);
            System.out.println(result);

            Object o = invokeMethodOnTheFly(className, "test02", new Class[]{String.class}, new Object[]{"jiayuchen"});
            System.out.println(o);
        }
    }

}
