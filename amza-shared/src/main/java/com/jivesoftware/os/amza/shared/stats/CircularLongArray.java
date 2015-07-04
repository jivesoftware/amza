package com.jivesoftware.os.amza.shared.stats;

class CircularLongArray {

    int size;
    int p;
    double[] rate;
    long[] signal;
    double ratePerSec = 0;
    long lastTime = 0;
    long lastV = Long.MAX_VALUE;

    CircularLongArray(int size) {
        this.size = size;
        signal = new long[size];
        rate = new double[size];
        p = 0;
    }

    public double[] rawSignal() {
        int lp = p;
        double[] copy = new double[size];
        for (int i = 0; i < size; i++) {
            copy[i] = signal[(lp + i) % size];
        }
        return copy;
    }

    public double[] rawRate() {
        int lp = p;
        double[] copy = new double[size];
        for (int i = 0; i < size; i++) {
            copy[i] = rate[(lp + i) % size];
        }
        return copy;
    }

    public void push(long time, long v) {
        signal[p] = v;
        if (lastV == Long.MAX_VALUE) {
            lastTime = time;
            lastV = v;
        } else {
            double delta = v - lastV;
            ratePerSec = Math.abs((delta / (time - lastTime)) * 1000.0);
            rate[p] = ratePerSec;
            lastTime = time;
            lastV = v;
        }
        p++;
        p %= size;
    }
}
