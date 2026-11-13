package org.apache.seatunnel.connectors.seatunnel.groovy.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

@Slf4j
public class GroovySourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final String code;
    private final SingleSplitReaderContext context;
    private final List<String> fieldNames;
    private GroovyClassLoader groovyClassLoader;
    private GroovySourceScript script;

    public GroovySourceReader(String code, SingleSplitReaderContext context, List<String> fieldNames) {
        this.code = code;
        this.context = context;
        this.fieldNames = fieldNames;
    }

    @Override
    public void open() throws Exception {
        groovyClassLoader = new GroovyClassLoader(GroovySourceScript.class.getClassLoader());
        Class<?> scriptClass = resolveScriptClass(parseScriptClass());
        Object instance = scriptClass.getDeclaredConstructor().newInstance();
        Method openMethod = findMethod(scriptClass, "open", 0);
        Method pollNextMethod = findMethod(scriptClass, "pollNext", 1);
        if (openMethod == null || pollNextMethod == null) {
            throw new SeaTunnelException(
                    "Groovy code must define open() and pollNext(output)");
        }
        script = new MethodScript(instance, openMethod, pollNextMethod, fieldNames);
        log.info("Groovy source script loaded: {}", scriptClass.getName());
        script.open();
    }

    @Override
    public void close() throws IOException {
        if (groovyClassLoader != null) {
            groovyClassLoader.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (script.pollNext(output)) {
            // 脚本返回 true 时继续拉下一条
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            log.info("Closed the bounded groovy source");
            context.signalNoMoreElement();
        }
    }

    /**
     * 不写参数类型时，Groovy 不会把 pollNext(output) 认成接口方法。
     * 这种脚本去掉 implements 后再按方法名调用。
     */
    private Class<?> parseScriptClass() throws Exception {
        try {
            return groovyClassLoader.parseClass(code, "GroovySource.groovy");
        } catch (Exception first) {
            String relaxed =
                    code.replaceAll("implements\\s+([\\w.]+\\.)?GroovySourceScript", "");
            if (relaxed.equals(code)) {
                throw first;
            }
            groovyClassLoader.close();
            groovyClassLoader = new GroovyClassLoader(GroovySourceScript.class.getClassLoader());
            log.info("Load groovy script by open/pollNext method names");
            return groovyClassLoader.parseClass(relaxed, "GroovySource.groovy");
        }
    }

    private Class<?> resolveScriptClass(Class<?> parsed) {
        if (isCandidate(parsed)) {
            return parsed;
        }
        for (Class<?> loaded : groovyClassLoader.getLoadedClasses()) {
            if (isCandidate(loaded)) {
                return loaded;
            }
        }
        throw new SeaTunnelException("Groovy code must define open() and pollNext(output)");
    }

    private boolean isCandidate(Class<?> clazz) {
        if (clazz == null || clazz.isInterface()) {
            return false;
        }
        if (GroovySourceScript.class.isAssignableFrom(clazz)) {
            return true;
        }
        return findMethod(clazz, "open", 0) != null && findMethod(clazz, "pollNext", 1) != null;
    }

    private static Method findMethod(Class<?> clazz, String name, int paramCount) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Method method : current.getDeclaredMethods()) {
                if (method.getName().equals(name) && method.getParameterCount() == paramCount) {
                    method.setAccessible(true);
                    return method;
                }
            }
            current = current.getSuperclass();
        }
        return null;
    }

    private static final class MethodScript implements GroovySourceScript {

        private final Object target;
        private final Method openMethod;
        private final Method pollNextMethod;
        private final List<String> fieldNames;

        private MethodScript(Object target, Method openMethod, Method pollNextMethod, List<String> fieldNames) {
            this.target = target;
            this.openMethod = openMethod;
            this.pollNextMethod = pollNextMethod;
            this.fieldNames = fieldNames;
        }

        @Override
        public void open() throws Exception {
            invoke(openMethod);
        }

        @Override
        public boolean pollNext(Collector<SeaTunnelRow> output) throws Exception {
            GroovyOutput groovyOutput = new GroovyOutput(output, fieldNames);
            Object result;
            try {
                result = pollNextMethod.invoke(target, groovyOutput);
            } catch (IllegalArgumentException incompatible) {
                result = invoke(pollNextMethod, output);
            } catch (InvocationTargetException e) {
                throw unwrap(e);
            }
            return asBoolean(result);
        }

        private boolean asBoolean(Object result) {
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            throw new SeaTunnelException("pollNext must return boolean");
        }

        private Object invoke(Method method, Object... args) throws Exception {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw unwrap(e);
            }
        }

        private static Exception unwrap(InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                return (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            return e;
        }
    }
}
