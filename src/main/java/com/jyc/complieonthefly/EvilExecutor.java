package com.jyc.complieonthefly;

import org.junit.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class EvilExecutor {

    private String readCode(String sourcePath) throws FileNotFoundException {
        InputStream stream = new FileInputStream(sourcePath);
        String separator = System.getProperty("line.separator");
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return reader.lines().collect(Collectors.joining(separator));
    }

    private Path saveSource(String source) throws IOException {
        String tmpProperty = System.getProperty("java.io.tmpdir");
        Path sourcePath = Paths.get(tmpProperty, "Harmless.java");
        Files.write(sourcePath, source.getBytes("UTF-8"));
        return sourcePath;
    }

    private Path compileSource(Path javaFile) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        return javaFile.getParent().resolve("Harmless.class");
    }

    private void runClass(Path javaClass)
            throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        URL classUrl = javaClass.getParent().toFile().toURI().toURL();
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classUrl});
        Class<?> clazz = Class.forName("Harmless", true, classLoader);
        clazz.newInstance();
    }

    public void doEvil(String sourcePath) throws Exception {
        String source = readCode(sourcePath);
        Path javaFile = saveSource(source);
        Path classFile = compileSource(javaFile);
        runClass(classFile);
    }

    public static void main(String... args) throws Exception {
        new EvilExecutor().doEvil(args[0]);
    }

    @Test
    public void testEvil(){
        String classPath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        System.out.println(System.getProperty("java.io.tmpdir"));
    }
}
