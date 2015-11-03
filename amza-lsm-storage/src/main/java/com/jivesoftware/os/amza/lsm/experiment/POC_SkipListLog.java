package com.jivesoftware.os.amza.lsm.experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author jonathan.colt
 */
public class POC_SkipListLog {

    public static void main(String[] args) {
        POC_SkipListLog s = new POC_SkipListLog();
        int[] input = new int[30];
        for (int i = 0; i < input.length; i++) {
            input[i] = i+1;
        }
        shuffleArray(input);
        for (int i : input) {
            s.add(i);
        }
        System.out.println(s);

        Arrays.sort(input);
        int avg = 0;
        for (int i : input) {
            int hops = s.get(i);
            avg += hops;
            System.out.println(i + " " + hops);
        }
        System.out.println("avg:" + ((float) avg / (float) input.length));
    }

    static void shuffleArray(int[] ar) {
        // If running on Java 6 or older, use `new Random()` on RHS here
        Random rnd = new Random();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            int a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

    List<Entry> log = new ArrayList<>();

    public int get(int key) {
        int size = log.size();
        int hops = 0;
        for (int i = size - 1; i > -1; i--) {
            Entry e = log.get(i);
            if (e.pivot.key == key) {
                return hops;
            } else if (key < e.pivot.key && !e.lessThan.isEmpty()) {
                for (Key lt : e.lessThan) {
                    if (lt.key == key) {
                        return hops;
                    }
                }
            } else if (key > e.pivot.key && !e.greaterThan.isEmpty()) {
                for (Key gt : e.greaterThan) {
                    if (gt.key == key) {
                        return hops;
                    }
                }
            }
            hops++;

        }
        return hops;
    }

    public void add(int key) {
        if (log.isEmpty()) {
            log.add(new Entry(new Edge(0, new Key(0, key), new Key(0, key)), new Key(0, key)));
        } else {
            int size = log.size();
            Key pivot = new Key(size, key);
            size--;
            Entry e = log.get(size);

            Entry next = new Entry(
                new Edge(0,
                    (key <= e.range.from.key) ? pivot : e.range.from,
                    (key >= e.range.to.key) ? pivot : e.range.to),
                pivot
            );

            if (pivot.key < e.pivot.key) {
                for (Key lt : (e.lessThan.isEmpty() ? Arrays.asList(e.range.from) : e.lessThan)) {
                    if (pivot.key < lt.key) {
                        next.greaterThan.add(lt);

                    } else if (pivot.key > lt.key) {
                        next.lessThan.add(lt);

                    }
                }
            } else if (e.pivot.key < pivot.key) {
                for (Key gt : (e.greaterThan.isEmpty() ? Arrays.asList(e.range.to) : e.greaterThan)) {
                    if (gt.key < pivot.key) {
                        next.lessThan.add(gt);

                    } else if (pivot.key < gt.key) {
                        next.greaterThan.add(gt);
                    }
                }
            }
            log.add(next);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry entry : log) {
            sb.append(entry.toString());
        }
        return sb.toString();
    }

    public static class Entry {

        Edge range;
        List<Edge> span = new ArrayList<>();

        List<Key> lessThan = new ArrayList<>();
        Key pivot;
        List<Key> greaterThan = new ArrayList<>();

        public Entry(Edge range, Key pivot) {
            this.range = range;
            this.pivot = pivot;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(pivot);
            

            int max = Math.max(greaterThan.size(), lessThan.size());
            for (int i = 0; i < max; i++) {
                Key less = (i < lessThan.size()) ? lessThan.get(i) : null;
                Key greater = (i < greaterThan.size()) ? greaterThan.get(i) : null;
                sb.append("\t\t");
                int run = 0;
                if (less == null) {
                    run = pivot.key - 1;
                    for (int r = 0; r < run; r++) {
                        sb.append("    ");
                    }
                    sb.append(pivot);
                } else {
                    run = less.key - 1;
                    for (int r = 0; r < run; r++) {
                        sb.append("    ");
                    }
                    sb.append(less);
                    run = (pivot.key - less.key) - 1;
                    for (int r = 0; r < run; r++) {
                        sb.append("<<<<");
                    }
                    sb.append(pivot);
                }

                if (greater != null) {
                    run = (greater.key - pivot.key) - 1;
                    for (int r = 0; r < run; r++) {
                        sb.append(">>>>");
                    }
                    sb.append(greater);
                }

            }
            return sb.append("\n").toString();
        }

    }

    public static class Edge {

        int level;
        Key from;
        Key to;

        public Edge(int level, Key from, Key to) {
            this.level = level;
            this.from = from;
            this.to = to;
        }

        @Override
        public String toString() {
            return level + ": " + from + "<->" + to;
        }

    }

    public static class Key {

        int logIndex;
        int key;

        public Key(int logIndex, int key) {
            this.logIndex = logIndex;
            this.key = key;
        }

        @Override
        public String toString() {
            return "(" + String.format("%1$2s", key) + ")";
        }

    }

}
