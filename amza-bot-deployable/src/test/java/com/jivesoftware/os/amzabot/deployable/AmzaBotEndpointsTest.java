package com.jivesoftware.os.amzabot.deployable;

import com.beust.jcommander.internal.Sets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AmzaBotEndpointsTest {

    @Test
    public void verifyGetEndpointJoinUsage() throws Exception {
        List<String> noValues = Lists.newArrayList();
        Assert.assertEquals(noValues.size(), 0);

        String noValue = Joiner.on(',').join(noValues);
        Assert.assertEquals(noValue, "");

        List<String> oneValues = Lists.newArrayList();
        oneValues.add("one");
        String oneValue = Joiner.on(',').join(oneValues);
        Assert.assertEquals(oneValue, "one");

        List<String> twoValues = Lists.newArrayList();
        twoValues.add("one");
        twoValues.add("two");
        String twoValue = Joiner.on(',').join(twoValues);
        Assert.assertEquals(twoValue, "one,two");
    }

    @Test
    public void verifyBatchSetPost() throws Exception {
        Set<Entry<String, String>> set = Sets.newHashSet();
        set.add(new AbstractMap.SimpleEntry<>("foo", "bar"));
        set.add(new AbstractMap.SimpleEntry<>("ham", "eggs"));

        String setJson = new ObjectMapper().writeValueAsString(set);
        Assert.assertEquals(setJson, "[{\"ham\":\"eggs\"},{\"foo\":\"bar\"}]");

        // i.e.
        //
        // curl -X POST \
        //   -H "Content-Type:application/json" \
        //   -d '[{"ham":"eggs"},{"foo":"bar"}]' \
        //   10.126.5.87:10000/api/amzabot/v1/keys
    }

}
