package com.mogujie.mst.hbase;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fenqi on 16/6/30.
 */
public class JavaRegexTest {
    private static final Logger log = LoggerFactory.getLogger(JavaRegexTest.class);

    @Test
    public void testJavaRegexExmaple() {
        // String to be scanned to find the pattern.
        String line = "This order was placed for QT3000! OK?";
        String pattern = "(.*)(\\d+)(.*)";

        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);

        // Now create matcher object.
        Matcher m = r.matcher(line);
        if (m.find()) {
            Assert.assertEquals(3, m.groupCount());
            log.info("Found value: " + m.group(0) );
            log.info("Found value: " + m.group(1) );
            log.info("Found value: " + m.group(2) );
        } else {
            log.info("NO MATCH");
            Assert.fail();
        }
    }

    @Test
    public void testDesignedPattern() {
        String line = "1467228764759-5";
        char seperator = '-';
        int wantedId = 5;
        int len = String.valueOf(System.currentTimeMillis()).length();
        String pattern = "^[0-9]{" + len + "}" + String.valueOf(seperator) + wantedId + "$";

        log.info("timestamp length : {}, pattern is {}", len, pattern);
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);
        Assert.assertTrue(m.find());
    }
}
