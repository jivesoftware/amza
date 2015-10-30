package com.jivesoftware.os.amza.shared.filer;

import com.jivesoftware.os.amza.api.filer.UIO;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class AutoGrowingByteBufferBackedFilerTest {

    @Test
    public void writeALongTest() throws Exception {
        byte[] intLongBuffer = new byte[8];
        for (int i = 1; i < 10; i++) {

            @SuppressWarnings("unchecked")
            Function<Long, ByteBuffer>[] bufferFactorys = new Function[]{
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocate(capacity.intValue()),
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocateDirect(capacity.intValue())
            };
            for (Function<Long, ByteBuffer> bf : bufferFactorys) {

                System.out.println("i:" + i);
                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(i, i, bf);
                UIO.writeLong(filer, Long.MAX_VALUE, "a long");
                filer.seek(0);
                Assert.assertEquals(UIO.readLong(filer, "a long", intLongBuffer), Long.MAX_VALUE, "Booya");
            }
        }
    }

    @Test
    public void writeByteTest() throws Exception {
        for (int b = 1; b < 10; b++) {
            System.out.println("b:" + b);

            @SuppressWarnings("unchecked")
            Function<Long, ByteBuffer>[] bufferFactorys = new Function[]{
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocate(capacity.intValue()),
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocateDirect(capacity.intValue())
            };
            for (Function<Long, ByteBuffer> bf : bufferFactorys) {

                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(b, b, bf);
                for (int i = 0; i < b * 4; i++) {
                    System.out.println(b + " " + i + " " + bf);
                    filer.write(new byte[]{(byte) i}, 0, 1);
                    filer.seek(i);
                    Assert.assertEquals(filer.read(), i, "Boo " + i + " at " + b + " " + bf);
                }
            }
        }
    }

    @Test
    public void writeIntsTest() throws Exception {
        byte[] intLongBuffer = new byte[8];
        for (int b = 1; b < 10; b++) {
            System.out.println("b:" + b);

            @SuppressWarnings("unchecked")
            Function<Long, ByteBuffer>[] bufferFactorys = new Function[]{
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocate(capacity.intValue()),
                (Function<Long, ByteBuffer>) capacity -> ByteBuffer.allocateDirect(capacity.intValue())
            };
            for (Function<Long, ByteBuffer> bf : bufferFactorys) {

                AutoGrowingByteBufferBackedFiler filer = new AutoGrowingByteBufferBackedFiler(b, b, bf);
                for (int i = 0; i < b * 4; i++) {
                    System.out.println(b + " " + i + " " + bf);
                    UIO.writeInt(filer, i, "", new byte[4]);
                    filer.seek(i * 4);
                    Assert.assertEquals(UIO.readInt(filer, "", intLongBuffer), i, "Boo " + i + " at " + b + " " + bf);
                }
            }
        }
    }

}
