package com.jivesoftware.os.amzabot.deployable;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AmzaBotUtilTest {

    @Test
    public void testTruncVal() {
        Assert.assertEquals(AmzaBotUtil.truncVal(null), "");
        Assert.assertEquals(AmzaBotUtil.truncVal(""), "");

        String nine = "123456789";
        Assert.assertEquals(AmzaBotUtil.truncVal(nine), nine);

        String ten = "1234567890";
        Assert.assertEquals(AmzaBotUtil.truncVal(ten), ten);

        String eleven = "12345678901";
        Assert.assertEquals(AmzaBotUtil.truncVal(eleven), ten + "...");
    }

}
