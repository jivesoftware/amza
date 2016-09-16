package com.jivesoftware.os.amzabot.deployable;

import java.util.concurrent.atomic.AtomicInteger;
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

    @Test
    public void verifyCompareAndSet() {
        AtomicInteger ai = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            ai.set(i);

            if (ai.compareAndSet(5, 10)) {
                break;
            }
        }
        Assert.assertEquals(ai.get(), 10);
    }

}
