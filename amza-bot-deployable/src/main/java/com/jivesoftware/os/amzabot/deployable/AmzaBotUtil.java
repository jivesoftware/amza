package com.jivesoftware.os.amzabot.deployable;

public class AmzaBotUtil {

    public static String truncVal(String s) {
        if (s == null) {
            return "";
        }

        int len = Math.min(s.length(), 10);
        String postfix = "";
        if (s.length() > len) {
            postfix = "...";
        }

        return s.substring(0, len) + postfix;
    }

}
