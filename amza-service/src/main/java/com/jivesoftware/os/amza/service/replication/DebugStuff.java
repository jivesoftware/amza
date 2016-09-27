package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.BAInterner;
import java.util.Arrays;

/**
 *
 */
public class DebugStuff {
    public static void main(String[] args) throws Exception {
        String s =
            "00000047000000000007616D7A61626F7400000036616D7A61626F742D72616E646F6D6F70732D3066373" +
                "9336561322D333164302D343038362D623162392D3366303438326137626462610000000018303030" +
                "30315F353233323932383736323231353731303732074A89DC34802002000000001830303030315F3" +
                "53233323932383736323231353731303732";
        byte[] bytes = hexStringToByteArray(s);
        System.out.println("key: " + Arrays.toString(bytes));
        AmzaAquariumProvider.streamStateKey(bytes, (partitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
            System.out.println("partitionName: " + partitionName);
            System.out.println("context: " + context);
            System.out.println("rootRingMember: " + rootRingMember);
            System.out.println("partitionVersion: " + partitionVersion);
            System.out.println("isSelf: " + isSelf);
            System.out.println("ackRingMember: " + ackRingMember);
            return true;
        }, new BAInterner());
    }


    public static byte[] hexStringToByteArray(String s) {
        if (s.equals("null")) {
            return null;
        }
        if (s.length() == 0) {
            return new byte[0];
        }
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
