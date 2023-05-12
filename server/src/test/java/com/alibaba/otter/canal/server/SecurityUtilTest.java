package com.alibaba.otter.canal.server;

import java.security.NoSuchAlgorithmException;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.otter.canal.protocol.SecurityUtil;

public class SecurityUtilTest {

    @Test
    public void testSimple() throws NoSuchAlgorithmException {
        byte[] seed = {1, 2, 3, 4, 5, 6, 7, 8};
        // String str = "e3619321c1a937c46a0d8bd1dac39f93b27d4458"; // canal
        // passwd
        String str = SecurityUtil.scrambleGenPass("123456".getBytes());

        System.out.println(str.toUpperCase());  // canal密码
        //byte[] client = SecurityUtil.scramble411("Cckg@canal".getBytes(), seed);
        //boolean check = SecurityUtil.scrambleServerAuth(client, SecurityUtil.hexStr2Bytes(str), seed);
        //Assert.assertTrue(check);
    }

    @Test
    public void testSimple1() throws NoSuchAlgorithmException {
        byte[] seed = {1, 2, 3, 4, 5, 6, 7, 8};
        // String str = "e3619321c1a937c46a0d8bd1dac39f93b27d4458"; // canal
        // passwd
        String str = SecurityUtil.scrambleGenPass("123456".getBytes()).toUpperCase();
        System.out.println(str);
        //byte[] client = SecurityUtil.scramble411("canal".getBytes(), seed);
        //boolean check = SecurityUtil.scrambleServerAuth(client, SecurityUtil.hexStr2Bytes(str), seed);
        //Assert.assertTrue(check);
    }
}
