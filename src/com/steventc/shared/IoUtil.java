package com.steventc.shared;

import java.nio.Buffer;

public class IoUtil {

    public static void resetBuffer(Buffer readableIntBuffer) {
        if (readableIntBuffer.position() == readableIntBuffer.limit()) {
            readableIntBuffer.rewind();
        }
    }
}
