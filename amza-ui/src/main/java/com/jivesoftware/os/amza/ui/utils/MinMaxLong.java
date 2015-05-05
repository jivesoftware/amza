package com.jivesoftware.os.amza.ui.utils;

/**
 *
 * @author jonathan.colt
 */
public class MinMaxLong {

    /**
     *
     */
    public long min = Long.MAX_VALUE;
    /**
     *
     */
    public long max = Long.MIN_VALUE;
    /**
     *
     */
    public int minIndex = -1;
    /**
     *
     */
    public int maxIndex = -1;
    private float sum = 0;
    private int count = 0;

    /**
     *
     */
    public MinMaxLong() {
    }

    /**
     *
     * @param _min
     * @param _max
     */
    public MinMaxLong(long _min, long _max) {
        min = _min;
        max = _max;
        count = 2;
    }

    /**
     *
     * @param _value
     * @return
     */
    public double std(long _value) {
        double mean = Math.pow(mean(), 2);
        double value = Math.pow((long) _value, 2);
        double variation = Math.max(mean, value) - Math.min(mean, value);
        return Math.sqrt(variation);
    }

    /**
     *
     * @param _p
     * @return
     */
    public boolean inclusivelyContained(long _p) {
        if (_p < min) {
            return false;
        }
        return _p <= max;
    }

    /**
     *
     * @return
     */
    public long min() {
        return min;
    }

    /**
     *
     * @return
     */
    public long max() {
        return max;
    }

    /**
     *
     * @param _long
     * @return
     */
    public long value(long _long) {
        sum += _long;
        if (_long > max) {
            max = _long;
            maxIndex = count;
        }
        if (_long < min) {
            min = _long;
            minIndex = count;
        }
        count++;
        return _long;
    }

    /**
     *
     */
    public void reset() {
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
        minIndex = -1;
        maxIndex = -1;
        sum = 0;
        count = 0;
    }

    /**
     *
     * @return
     */
    public long samples() {
        return count;
    }

    /**
     *
     * @return
     */
    public double mean() {
        return sum / (double) count;
    }

    /**
     *
     * @return
     */
    public long range() {
        return max - min;
    }

    /**
     *
     * @return
     */
    public long middle() {
        return min + ((max - min) / 2);
    }

    /**
     *
     * @param _v
     * @param _inclusive
     * @return
     */
    public boolean isBetween(long _v, boolean _inclusive) {
        if (_inclusive) {
            return _v <= max && _v >= min;
        } else {
            return _v < max && _v > min;
        }
    }

    /**
     *
     * @param _long
     * @return
     */
    public double negativeOneToOne(long _long) {
        return (zeroToOne(_long) - 0.5) * 2.0;
    }

    /**
     *
     * @param _long
     * @return
     */
    public double zeroToOne(long _long) {
        return zeroToOne(min, max, _long);
    }

    /**
     *
     * @param _min
     * @param _max
     * @param _long
     * @return
     */
    public static double zeroToOne(long _min, long _max, long _long) {
        return (double) (_long - _min) / (double) (_max - _min);
    }

    /**
     *
     * @param _long
     * @return
     */
    public long unzeroToOne(long _long) {
        return unzeroToOne(min, max, _long);
    }

    /**
     *
     * @param _min
     * @param _max
     * @param _long
     * @return
     */
    public static long unzeroToOne(long _min, long _max, long _long) {
        return ((_max - _min) * _long) + _min;
    }

    @Override
    public String toString() {
        return "Min:" + min + " Max:" + max;
    }

}
