package com.mogujie.mst;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

/**
 * Created by fenqi on 16/6/21.
 */
public final class SingleJUnitTestRunner {

    private SingleJUnitTestRunner() {}

    public static void main(String... args) throws ClassNotFoundException {
        String[] classAndMethod = args[0].split("@");
        Request request = Request.method(Class.forName(classAndMethod[0]),
                classAndMethod[1]);

        Result result = new JUnitCore().run(request);
        System.exit(result.wasSuccessful() ? 0 : 1);
    }

}
