package com.zq.simpledatax.helper;

import java.util.concurrent.atomic.AtomicInteger;

public class TrackHelper {

    private final static AtomicInteger nextSerialNumber = new AtomicInteger(0);

    private final static int ATOMIC_INT_BOUNDS = 2147483647;

    /**
     * 
     * @param sourceFlag
     * @return
     */
    public static long generalTrackId(String sourceFlag) {
        return System.currentTimeMillis() + serialNumber();
    }

    /**
     * 获取递增数值
     * @return
     */
    public static int serialNumber() {
        int current;
        int next;
        do {
            current = nextSerialNumber.get();
            next = current >= ATOMIC_INT_BOUNDS ? 0 : current + 1;
        } while (!nextSerialNumber.compareAndSet(current, next));
        return next;
    }
}
