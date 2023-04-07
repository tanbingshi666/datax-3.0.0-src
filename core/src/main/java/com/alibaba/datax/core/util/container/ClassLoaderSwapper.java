package com.alibaba.datax.core.util.container;

/**
 * Created by jingxing on 14-8-29.
 *
 * 为避免jar冲突，比如hbase可能有多个版本的读写依赖jar包，JobContainer和TaskGroupContainer
 * 就需要脱离当前classLoader去加载这些jar包，执行完成后，又退回到原来classLoader上继续执行接下来的代码
 */
public final class ClassLoaderSwapper {
    private ClassLoader storeClassLoader = null;

    private ClassLoaderSwapper() {
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper() {
        return new ClassLoaderSwapper();
    }

    /**
     * 保存当前classLoader，并将当前线程的classLoader设置为所给classLoader
     *
     * @param
     * @return
     */
    public ClassLoader setCurrentThreadClassLoader(ClassLoader classLoader) {
        // 获取当前线程类加载器
        this.storeClassLoader = Thread.currentThread().getContextClassLoader();
        // 重新设置当前线程类加载器 主要保存了当前线程的开始类加载器 后续可以恢复当前类加载器
        Thread.currentThread().setContextClassLoader(classLoader);
        return this.storeClassLoader;
    }

    /**
     * 将当前线程的类加载器设置为保存的类加载
     * @return
     */
    public ClassLoader restoreCurrentThreadClassLoader() {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        // storeClassLoader 就是当前线程一开始的类加载器
        Thread.currentThread().setContextClassLoader(this.storeClassLoader);
        return classLoader;
    }
}
