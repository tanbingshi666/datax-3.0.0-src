package com.alibaba.datax.core.util.container;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 提供Jar隔离的加载机制，会把传入的路径、及其子路径、以及路径中的jar文件加入到class path。
 */
public class JarLoader extends URLClassLoader {
    public JarLoader(String[] paths) {
        this(paths, JarLoader.class.getClassLoader());
    }

    public JarLoader(String[] paths, ClassLoader parent) {
        super(getURLs(paths), parent);
    }

    private static URL[] getURLs(String[] paths) {
        Validate.isTrue(null != paths && 0 != paths.length,
                "jar包路径不能为空.");

        List<String> dirs = new ArrayList<String>();
        for (String path : paths) {
            dirs.add(path);
            JarLoader.collectDirs(path, dirs);
        }

        List<URL> urls = new ArrayList<URL>();
        for (String path : dirs) {
            urls.addAll(doGetURLs(path));
        }

        /**
         * 记载某个路径下的所有 jar
         * 比如这个 D:/app/datax/plugin/reader/streamreader 路径下的 jar (递归读取)
         * 0 = {URL@1308} "file:/D:/app/datax/plugin/reader/streamreader/._streamreader-0.0.1-SNAPSHOT.jar"
         * 1 = {URL@1309} "file:/D:/app/datax/plugin/reader/streamreader/streamreader-0.0.1-SNAPSHOT.jar"
         * 2 = {URL@1310} "file:/D:/app/datax/plugin/reader/streamreader/libs/._commons-io-2.4.jar"
         * 3 = {URL@1311} "file:/D:/app/datax/plugin/reader/streamreader/libs/._commons-lang3-3.3.2.jar"
         * 4 = {URL@1312} "file:/D:/app/datax/plugin/reader/streamreader/libs/._commons-math3-3.1.1.jar"
         * 5 = {URL@1313} "file:/D:/app/datax/plugin/reader/streamreader/libs/._datax-common-0.0.1-SNAPSHOT.jar"
         * 6 = {URL@1314} "file:/D:/app/datax/plugin/reader/streamreader/libs/._fastjson-1.1.46.sec01.jar"
         * 7 = {URL@1315} "file:/D:/app/datax/plugin/reader/streamreader/libs/._guava-16.0.1.jar"
         * 8 = {URL@1316} "file:/D:/app/datax/plugin/reader/streamreader/libs/._hamcrest-core-1.3.jar"
         * 9 = {URL@1317} "file:/D:/app/datax/plugin/reader/streamreader/libs/._logback-classic-1.0.13.jar"
         * 10 = {URL@1318} "file:/D:/app/datax/plugin/reader/streamreader/libs/._logback-core-1.0.13.jar"
         * 11 = {URL@1319} "file:/D:/app/datax/plugin/reader/streamreader/libs/._slf4j-api-1.7.10.jar"
         * 12 = {URL@1320} "file:/D:/app/datax/plugin/reader/streamreader/libs/commons-io-2.4.jar"
         * 13 = {URL@1321} "file:/D:/app/datax/plugin/reader/streamreader/libs/commons-lang3-3.3.2.jar"
         * 14 = {URL@1322} "file:/D:/app/datax/plugin/reader/streamreader/libs/commons-math3-3.1.1.jar"
         * 15 = {URL@1323} "file:/D:/app/datax/plugin/reader/streamreader/libs/datax-common-0.0.1-SNAPSHOT.jar"
         * 16 = {URL@1324} "file:/D:/app/datax/plugin/reader/streamreader/libs/fastjson-1.1.46.sec01.jar"
         * 17 = {URL@1325} "file:/D:/app/datax/plugin/reader/streamreader/libs/guava-16.0.1.jar"
         * 18 = {URL@1326} "file:/D:/app/datax/plugin/reader/streamreader/libs/hamcrest-core-1.3.jar"
         * 19 = {URL@1327} "file:/D:/app/datax/plugin/reader/streamreader/libs/logback-classic-1.0.13.jar"
         * 20 = {URL@1328} "file:/D:/app/datax/plugin/reader/streamreader/libs/logback-core-1.0.13.jar"
         * 21 = {URL@1329} "file:/D:/app/datax/plugin/reader/streamreader/libs/slf4j-api-1.7.10.jar"
         */
        return urls.toArray(new URL[0]);
    }

    private static void collectDirs(String path, List<String> collector) {
        if (null == path || StringUtils.isBlank(path)) {
            return;
        }

        File current = new File(path);
        if (!current.exists() || !current.isDirectory()) {
            return;
        }

        for (File child : Objects.requireNonNull(current.listFiles())) {
            if (!child.isDirectory()) {
                continue;
            }

            collector.add(child.getAbsolutePath());
            collectDirs(child.getAbsolutePath(), collector);
        }
    }

    private static List<URL> doGetURLs(final String path) {
        Validate.isTrue(!StringUtils.isBlank(path), "jar包路径不能为空.");

        File jarPath = new File(path);

        Validate.isTrue(jarPath.exists() && jarPath.isDirectory(),
                "jar包路径必须存在且为目录.");

		/* set filter */
        FileFilter jarFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".jar");
            }
        };

		/* iterate all jar */
        File[] allJars = new File(path).listFiles(jarFilter);
        List<URL> jarURLs = new ArrayList<URL>(allJars.length);

        for (int i = 0; i < allJars.length; i++) {
            try {
                jarURLs.add(allJars[i].toURI().toURL());
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.PLUGIN_INIT_ERROR,
                        "系统加载jar包出错", e);
            }
        }

        return jarURLs;
    }
}
