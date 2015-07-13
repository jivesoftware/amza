package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntBiFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;
import sun.misc.Unsafe;

/**
 * Borrowed from ByteArrayConcurrentHashMap. Occurrences of {@code key.hashCode()} are replaced with {@code Arrays.hashCode(key)}
 * and occurrences of {@code key.equals(o)} are replaced with {@code Arrays.equals(key, o)}.
 */
public class ByteArrayConcurrentHashMap<V> implements ConcurrentMap<byte[], V> {

    public static void main(String[] args) throws Exception {
        ByteArrayConcurrentHashMap<Long> map = new ByteArrayConcurrentHashMap<>();
        System.out.println("Add...");
        for (long i = 0; i < 1_000_000; i++) {
            map.put(UIO.longBytes(i), i);
        }

        if (map.get(UIO.longBytes(-1)) != null) {
            System.out.println("shouldn't get -1");
        }

        if (map.get(UIO.longBytes(1_000_000)) != null) {
            System.out.println("shouldn't get 1_000_000");
        }

        if (map.get(new byte[0]) != null) {
            System.out.println("shouldn't get empty");
        }

        System.out.println("Get...");
        for (long i = 0; i < 1_000_000; i++) {
            if (map.get(UIO.longBytes(i)).longValue() != i) {
                System.out.println("wrong get! " + i);
            }
        }

        if (map.contains(UIO.longBytes(-1))) {
            System.out.println("shouldn't contain -1");
        }

        if (map.contains(UIO.longBytes(1_000_000))) {
            System.out.println("shouldn't contain 1_000_000");
        }

        if (map.contains(new byte[0])) {
            System.out.println("shouldn't contain empty");
        }

        System.out.println("Contains...");
        for (long i = 0; i < 1_000_000; i++) {
            if (!map.containsKey(UIO.longBytes(i))) {
                System.out.println("wrong contains! " + i);
            }
        }

        System.out.println("Count...");
        int count = 0;
        for (byte[] key : map.keySet()) {
            long i = UIO.bytesLong(key);
            if (i < 0 || i >= 1_000_000) {
                System.out.println("wrong key set! " + i);
            }
            count++;
        }
        if (count != 1_000_000) {
            System.out.println("wrong count! " + count);
        }

    }

    /* ---------------- Constants -------------- */

    /**
     * The largest possible table capacity.  This value must be
     * exactly 1<<30 to stay within Java array allocation and indexing
     * bounds for power of two table sizes, and is further required
     * because the top two bits of 32bit hash fields are used for
     * control purposes.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The default initial table capacity.  Must be a power of 2
     * (i.e., at least 1) and at most MAXIMUM_CAPACITY.
     */
    private static final int DEFAULT_CAPACITY = 16;

    /**
     * The largest possible (non-power of two) array size.
     * Needed by toArray and related methods.
     */
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * The default concurrency level for this table. Unused but
     * defined for compatibility with previous versions of this class.
     */
    private static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The load factor for this table. Overrides of this value in
     * constructors affect only the initial table capacity.  The
     * actual floating point value isn't normally used -- it is
     * simpler to use expressions such as {@code n - (n >>> 2)} for
     * the associated resizing threshold.
     */
    private static final float LOAD_FACTOR = 0.75f;

    /**
     * The bin count threshold for using a tree rather than list for a
     * bin.  Bins are converted to trees when adding an element to a
     * bin with at least this many nodes. The value must be greater
     * than 2, and should be at least 8 to mesh with assumptions in
     * tree removal about conversion back to plain bins upon
     * shrinkage.
     */
    static final int TREEIFY_THRESHOLD = 8;

    /**
     * The bin count threshold for untreeifying a (split) bin during a
     * resize operation. Should be less than TREEIFY_THRESHOLD, and at
     * most 6 to mesh with shrinkage detection under removal.
     */
    static final int UNTREEIFY_THRESHOLD = 6;

    /**
     * The smallest table capacity for which bins may be treeified.
     * (Otherwise the table is resized if too many nodes in a bin.)
     * The value should be at least 4 * TREEIFY_THRESHOLD to avoid
     * conflicts between resizing and treeification thresholds.
     */
    static final int MIN_TREEIFY_CAPACITY = 64;

    /**
     * Minimum number of rebinnings per transfer step. Ranges are
     * subdivided to allow multiple resizer threads.  This value
     * serves as a lower bound to avoid resizers encountering
     * excessive memory contention.  The value should be at least
     * DEFAULT_CAPACITY.
     */
    private static final int MIN_TRANSFER_STRIDE = 16;

    /**
     * The number of bits used for generation stamp in sizeCtl.
     * Must be at least 6 for 32bit arrays.
     */
    private static int RESIZE_STAMP_BITS = 16;

    /**
     * The maximum number of threads that can help resize.
     * Must fit in 32 - RESIZE_STAMP_BITS bits.
     */
    private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

    /**
     * The bit shift for recording size stamp in sizeCtl.
     */
    private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;

    /*
     * Encodings for Node hash fields. See above for explanation.
     */
    static final int MOVED = -1; // hash for forwarding nodes
    static final int TREEBIN = -2; // hash for roots of trees
    static final int RESERVED = -3; // hash for transient reservations
    static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

    /** Number of CPUS, to place bounds on some sizings */
    static final int NCPU = Runtime.getRuntime().availableProcessors();

    /** For serialization compatibility. */
    private static final ObjectStreamField[] serialPersistentFields = {
        new ObjectStreamField("segments", Segment[].class),
        new ObjectStreamField("segmentMask", Integer.TYPE),
        new ObjectStreamField("segmentShift", Integer.TYPE)
    };

    /* ---------------- Nodes -------------- */

    /**
     * Key-value entry.  This class is never exported out as a
     * user-mutable Map.Entry (i.e., one supporting setValue; see
     * MapEntry below), but can be used for read-only traversals used
     * in bulk tasks.  Subclasses of Node with a negative hash field
     * are special, and contain null keys and values (but are never
     * exported).  Otherwise, keys and vals are never null.
     */
    static class Node<V> implements Map.Entry<byte[], V> {
        final int hash;
        final byte[] key;
        volatile V val;
        volatile Node<V> next;

        Node(int hash, byte[] key, V val, Node<V> next) {
            this.hash = hash;
            this.key = key;
            this.val = val;
            this.next = next;
        }

        public final byte[] getKey() {
            return key;
        }

        public final V getValue() {
            return val;
        }

        public final int hashCode() {
            return Arrays.hashCode(key) ^ val.hashCode();
        }

        public final String toString() {
            return Arrays.toString(key) + "=" + val;
        }

        public final V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        public final boolean equals(Object o) {
            Object k, v, u;
            Map.Entry<?, ?> e;
            return ((o instanceof Map.Entry) &&
                (k = (e = (Map.Entry<?, ?>) o).getKey()) != null &&
                (v = e.getValue()) != null &&
                (k == key || keyEquals(k, key)) &&
                (v == (u = val) || v.equals(u)));
        }

        /**
         * Virtualized support for map.get(); overridden in subclasses.
         */
        Node<V> find(int h, Object k) {
            Node<V> e = this;
            if (k != null) {
                do {
                    byte[] ek;
                    if (e.hash == h &&
                        ((ek = e.key) == k || (ek != null && keyEquals(k, ek)))) {
                        return e;
                    }
                }
                while ((e = e.next) != null);
            }
            return null;
        }
    }

    /* ---------------- Static utilities -------------- */

    /**
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a tradeoff between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    static final int spread(int h) {
        return (h ^ (h >>> 16)) & HASH_BITS;
    }

    /**
     * Returns a power of two table size for the given desired capacity.
     * See Hackers Delight, sec 3.2
     */
    private static final int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /**
     * Returns x's Class if it is of the form "class C implements
     * Comparable<C>", else null.
     */
    static Class<?> comparableClassFor(Object x) {
        if (x instanceof Comparable) {
            Class<?> c;
            Type[] ts, as;
            Type t;
            ParameterizedType p;
            if ((c = x.getClass()) == String.class) // bypass checks
            {
                return c;
            }
            if ((ts = c.getGenericInterfaces()) != null) {
                for (int i = 0; i < ts.length; ++i) {
                    if (((t = ts[i]) instanceof ParameterizedType) &&
                        ((p = (ParameterizedType) t).getRawType() ==
                            Comparable.class) &&
                        (as = p.getActualTypeArguments()) != null &&
                        as.length == 1 && as[0] == c) // type arg is c
                    {
                        return c;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns k.compareTo(x) if x matches kc (k's screened comparable
     * class), else 0.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" }) // for cast to Comparable
    static int compareComparables(Class<?> kc, Object k, Object x) {
        return (x == null || x.getClass() != kc ? 0 :
            ((Comparable) k).compareTo(x));
    }

    /* ---------------- Table element access -------------- */

    /*
     * Volatile access methods are used for table elements as well as
     * elements of in-progress next table while resizing.  All uses of
     * the tab arguments must be null checked by callers.  All callers
     * also paranoically precheck that tab's length is not zero (or an
     * equivalent check), thus ensuring that any index argument taking
     * the form of a hash value anded with (length - 1) is a valid
     * index.  Note that, to be correct wrt arbitrary concurrency
     * errors by users, these checks must operate on local variables,
     * which accounts for some odd-looking inline assignments below.
     * Note that calls to setTabAt always occur within locked regions,
     * and so in principle require only release ordering, not
     * full volatile semantics, but are currently coded as volatile
     * writes to be conservative.
     */

    @SuppressWarnings("unchecked")
    static final <V> Node<V> tabAt(Node<V>[] tab, int i) {
        return (Node<V>) U.getObjectVolatile(tab, ((long) i << ASHIFT) + ABASE);
    }

    static final <V> boolean casTabAt(Node<V>[] tab, int i,
        Node<V> c, Node<V> v) {
        return U.compareAndSwapObject(tab, ((long) i << ASHIFT) + ABASE, c, v);
    }

    static final <V> void setTabAt(Node<V>[] tab, int i, Node<V> v) {
        U.putObjectVolatile(tab, ((long) i << ASHIFT) + ABASE, v);
    }

    /* ---------------- Fields -------------- */

    /**
     * The array of bins. Lazily initialized upon first insertion.
     * Size is always a power of two. Accessed directly by iterators.
     */
    transient volatile Node<V>[] table;

    /**
     * The next table to use; non-null only while resizing.
     */
    private transient volatile Node<V>[] nextTable;

    /**
     * Base counter value, used mainly when there is no contention,
     * but also as a fallback during table initialization
     * races. Updated via CAS.
     */
    private transient volatile long baseCount;

    /**
     * Table initialization and resizing control.  When negative, the
     * table is being initialized or resized: -1 for initialization,
     * else -(1 + the number of active resizing threads).  Otherwise,
     * when table is null, holds the initial table size to use upon
     * creation, or 0 for default. After initialization, holds the
     * next element count value upon which to resize the table.
     */
    private transient volatile int sizeCtl;

    /**
     * The next table index (plus one) to split while resizing.
     */
    private transient volatile int transferIndex;

    /**
     * Spinlock (locked via CAS) used when resizing and/or creating CounterCells.
     */
    private transient volatile int cellsBusy;

    /**
     * Table of counter cells. When non-null, size is a power of 2.
     */
    private transient volatile CounterCell[] counterCells;

    // views
    private transient KeySetView<V> keySet;
    private transient ValuesView<V> values;
    private transient EntrySetView<V> entrySet;


    /* ---------------- Public operations -------------- */

    /**
     * Creates a new, empty map with the default initial table size (16).
     */
    public ByteArrayConcurrentHashMap() {
    }

    /**
     * Creates a new, empty map with an initial table size
     * accommodating the specified number of elements without the need
     * to dynamically resize.
     *
     * @param initialCapacity The implementation performs internal
     *                        sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *                                  elements is negative
     */
    public ByteArrayConcurrentHashMap(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException();
        }
        int cap = ((initialCapacity >= (MAXIMUM_CAPACITY >>> 1)) ?
            MAXIMUM_CAPACITY :
            tableSizeFor(initialCapacity + (initialCapacity >>> 1) + 1));
        this.sizeCtl = cap;
    }

    /**
     * Creates a new map with the same mappings as the given map.
     *
     * @param m the map
     */
    public ByteArrayConcurrentHashMap(Map<? extends byte[], ? extends V> m) {
        this.sizeCtl = DEFAULT_CAPACITY;
        putAll(m);
    }

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}) and
     * initial table density ({@code loadFactor}).
     *
     * @param initialCapacity the initial capacity. The implementation
     *                        performs internal sizing to accommodate this many elements,
     *                        given the specified load factor.
     * @param loadFactor      the load factor (table density) for
     *                        establishing the initial table size
     * @throws IllegalArgumentException if the initial capacity of
     *                                  elements is negative or the load factor is nonpositive
     * @since 1.6
     */
    public ByteArrayConcurrentHashMap(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, 1);
    }

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}), table
     * density ({@code loadFactor}), and number of concurrently
     * updating threads ({@code concurrencyLevel}).
     *
     * @param initialCapacity  the initial capacity. The implementation
     *                         performs internal sizing to accommodate this many elements,
     *                         given the specified load factor.
     * @param loadFactor       the load factor (table density) for
     *                         establishing the initial table size
     * @param concurrencyLevel the estimated number of concurrently
     *                         updating threads. The implementation may use this value as
     *                         a sizing hint.
     * @throws IllegalArgumentException if the initial capacity is
     *                                  negative or the load factor or concurrencyLevel are
     *                                  nonpositive
     */
    public ByteArrayConcurrentHashMap(int initialCapacity,
        float loadFactor, int concurrencyLevel) {
        if (!(loadFactor > 0.0f) || initialCapacity < 0 || concurrencyLevel <= 0) {
            throw new IllegalArgumentException();
        }
        if (initialCapacity < concurrencyLevel)   // Use at least as many bins
        {
            initialCapacity = concurrencyLevel;   // as estimated threads
        }
        long size = (long) (1.0 + (long) initialCapacity / loadFactor);
        int cap = (size >= (long) MAXIMUM_CAPACITY) ?
            MAXIMUM_CAPACITY : tableSizeFor((int) size);
        this.sizeCtl = cap;
    }

    // Original (since JDK1.2) Map methods

    /**
     * {@inheritDoc}
     */
    public int size() {
        long n = sumCount();
        return ((n < 0L) ? 0 :
            (n > (long) Integer.MAX_VALUE) ? Integer.MAX_VALUE :
                (int) n);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return sumCount() <= 0L; // ignore transient negative values
    }

    private static boolean keyEquals(Object key, byte[] o) {
        return key instanceof byte[] && Arrays.equals((byte[]) key, o);
    }

    private static int keyHashCode(Object key) {
        return key instanceof byte[] ? Arrays.hashCode((byte[]) key) : key.hashCode();
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     * <p>
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key.equals(k)},
     * then this method returns {@code v}; otherwise it returns
     * {@code null}.  (There can be at most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    public V get(Object key) {
        Node<V>[] tab;
        Node<V> e, p;
        int n, eh;
        byte[] ek;
        int h = spread(keyHashCode(key));
        if ((tab = table) != null && (n = tab.length) > 0 &&
            (e = tabAt(tab, (n - 1) & h)) != null) {
            if ((eh = e.hash) == h) {
                if ((ek = e.key) == key || (ek != null && keyEquals(key, ek))) {
                    return e.val;
                }
            } else if (eh < 0) {
                return (p = e.find(h, key)) != null ? p.val : null;
            }
            while ((e = e.next) != null) {
                if (e.hash == h &&
                    ((ek = e.key) == key || (ek != null && keyEquals(key, ek)))) {
                    return e.val;
                }
            }
        }
        return null;
    }

    /**
     * Tests if the specified object is a key in this table.
     *
     * @param key possible key
     * @return {@code true} if and only if the specified object
     * is a key in this table, as determined by the
     * {@code equals} method; {@code false} otherwise
     * @throws NullPointerException if the specified key is null
     */
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the
     * specified value. Note: This method may require a full traversal
     * of the map, and is much slower than method {@code containsKey}.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the
     * specified value
     * @throws NullPointerException if the specified value is null
     */
    public boolean containsValue(Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V v;
                if ((v = p.val) == value || (v != null && value.equals(v))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Maps the specified key to the specified value in this table.
     * Neither the key nor the value can be null.
     * <p>
     * <p>The value can be retrieved by calling the {@code get} method
     * with a key that is equal to the original key.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(byte[] key, V value) {
        return putVal(key, value, false);
    }

    /** Implementation for put and putIfAbsent */
    final V putVal(byte[] key, V value, boolean onlyIfAbsent) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        int hash = spread(keyHashCode(key));
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & hash)) == null) {
                if (casTabAt(tab, i, null,
                    new Node<V>(hash, key, value, null))) {
                    break;                   // no lock when adding to empty bin
                }
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                V oldVal = null;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f; ; ++binCount) {
                                byte[] ek;
                                if (e.hash == hash &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    oldVal = e.val;
                                    if (!onlyIfAbsent) {
                                        e.val = value;
                                    }
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    pred.next = new Node<V>(hash, key,
                                        value, null);
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            Node<V> p;
                            binCount = 2;
                            if ((p = ((TreeBin<V>) f).putTreeVal(hash, key,
                                value)) != null) {
                                oldVal = p.val;
                                if (!onlyIfAbsent) {
                                    p.val = value;
                                }
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD) {
                        treeifyBin(tab, i);
                    }
                    if (oldVal != null) {
                        return oldVal;
                    }
                    break;
                }
            }
        }
        addCount(1L, binCount);
        return null;
    }

    /**
     * Copies all of the mappings from the specified map to this one.
     * These mappings replace any mappings that this map had for any of the
     * keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     */
    public void putAll(Map<? extends byte[], ? extends V> m) {
        tryPresize(m.size());
        for (Map.Entry<? extends byte[], ? extends V> e : m.entrySet())
            putVal(e.getKey(), e.getValue(), false);
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @param key the key that needs to be removed
     * @return the previous value associated with {@code key}, or
     * {@code null} if there was no mapping for {@code key}
     * @throws NullPointerException if the specified key is null
     */
    public V remove(Object key) {
        return replaceNode(key, null, null);
    }

    /**
     * Implementation for the four public remove/replace methods:
     * Replaces node value with v, conditional upon match of cv if
     * non-null.  If resulting value is null, delete.
     */
    final V replaceNode(Object key, V value, Object cv) {
        int hash = spread(keyHashCode(key));
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0 ||
                (f = tabAt(tab, i = (n - 1) & hash)) == null) {
                break;
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                V oldVal = null;
                boolean validated = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            validated = true;
                            for (Node<V> e = f, pred = null; ; ) {
                                byte[] ek;
                                if (e.hash == hash &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    V ev = e.val;
                                    if (cv == null || cv == ev ||
                                        (ev != null && cv.equals(ev))) {
                                        oldVal = ev;
                                        if (value != null) {
                                            e.val = value;
                                        } else if (pred != null) {
                                            pred.next = e.next;
                                        } else {
                                            setTabAt(tab, i, e.next);
                                        }
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            validated = true;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                (p = r.findTreeNode(hash, key, null)) != null) {
                                V pv = p.val;
                                if (cv == null || cv == pv ||
                                    (pv != null && cv.equals(pv))) {
                                    oldVal = pv;
                                    if (value != null) {
                                        p.val = value;
                                    } else if (t.removeTreeNode(p)) {
                                        setTabAt(tab, i, untreeify(t.first));
                                    }
                                }
                            }
                        }
                    }
                }
                if (validated) {
                    if (oldVal != null) {
                        if (value == null) {
                            addCount(-1L, -1);
                        }
                        return oldVal;
                    }
                    break;
                }
            }
        }
        return null;
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        long delta = 0L; // negative number of deletions
        int i = 0;
        Node<V>[] tab = table;
        while (tab != null && i < tab.length) {
            int fh;
            Node<V> f = tabAt(tab, i);
            if (f == null) {
                ++i;
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
                i = 0; // restart
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> p = (fh >= 0 ? f :
                            (f instanceof TreeBin) ?
                                ((TreeBin<V>) f).first : null);
                        while (p != null) {
                            --delta;
                            p = p.next;
                        }
                        setTabAt(tab, i++, null);
                    }
                }
            }
        }
        if (delta != 0L) {
            addCount(delta, -1);
        }
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa. The set supports element
     * removal, which removes the corresponding mapping from this map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or
     * {@code addAll} operations.
     * <p>
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     * <p>
     * <p>The view's {@code spliterator} reports {@link Spliterator#CONCURRENT},
     * {@link Spliterator#DISTINCT}, and {@link Spliterator#NONNULL}.
     *
     * @return the set view
     */
    public KeySetView<V> keySet() {
        KeySetView<V> ks;
        return (ks = keySet) != null ? ks : (keySet = new KeySetView<V>(this, null));
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  The collection
     * supports element removal, which removes the corresponding
     * mapping from this map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll},
     * {@code retainAll}, and {@code clear} operations.  It does not
     * support the {@code add} or {@code addAll} operations.
     * <p>
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     * <p>
     * <p>The view's {@code spliterator} reports {@link Spliterator#CONCURRENT}
     * and {@link Spliterator#NONNULL}.
     *
     * @return the collection view
     */
    public Collection<V> values() {
        ValuesView<V> vs;
        return (vs = values) != null ? vs : (values = new ValuesView<V>(this));
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.
     * <p>
     * <p>The view's iterators and spliterators are
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     * <p>
     * <p>The view's {@code spliterator} reports {@link Spliterator#CONCURRENT},
     * {@link Spliterator#DISTINCT}, and {@link Spliterator#NONNULL}.
     *
     * @return the set view
     */
    public Set<Map.Entry<byte[], V>> entrySet() {
        EntrySetView<V> es;
        return (es = entrySet) != null ? es : (entrySet = new EntrySetView<V>(this));
    }

    /**
     * Returns the hash code value for this {@link Map}, i.e.,
     * the sum of, for each key-value pair in the map,
     * {@code key.hashCode() ^ value.hashCode()}.
     *
     * @return the hash code value for this map
     */
    public int hashCode() {
        int h = 0;
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; )
                h += Arrays.hashCode(p.key) ^ p.val.hashCode();
        }
        return h;
    }

    /**
     * Returns a string representation of this map.  The string
     * representation consists of a list of key-value mappings (in no
     * particular order) enclosed in braces ("{@code {}}").  Adjacent
     * mappings are separated by the characters {@code ", "} (comma
     * and space).  Each key-value mapping is rendered as the key
     * followed by an equals sign ("{@code =}") followed by the
     * associated value.
     *
     * @return a string representation of this map
     */
    public String toString() {
        Node<V>[] t;
        int f = (t = table) == null ? 0 : t.length;
        Traverser<V> it = new Traverser<V>(t, f, 0, f);
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        Node<V> p;
        if ((p = it.advance()) != null) {
            for (; ; ) {
                byte[] k = p.key;
                V v = p.val;
                sb.append(Arrays.toString(k));
                sb.append('=');
                sb.append(v == this ? "(this Map)" : v);
                if ((p = it.advance()) == null) {
                    break;
                }
                sb.append(',').append(' ');
            }
        }
        return sb.append('}').toString();
    }

    /**
     * Compares the specified object with this map for equality.
     * Returns {@code true} if the given object is a map with the same
     * mappings as this map.  This operation may return misleading
     * results if either map is concurrently modified during execution
     * of this method.
     *
     * @param o object to be compared for equality with this map
     * @return {@code true} if the specified object is equal to this map
     */
    public boolean equals(Object o) {
        if (o != this) {
            if (!(o instanceof Map)) {
                return false;
            }
            Map<?, ?> m = (Map<?, ?>) o;
            Node<V>[] t;
            int f = (t = table) == null ? 0 : t.length;
            Traverser<V> it = new Traverser<V>(t, f, 0, f);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V val = p.val;
                Object v = m.get(p.key);
                if (v == null || (v != val && !v.equals(val))) {
                    return false;
                }
            }
            for (Map.Entry<?, ?> e : m.entrySet()) {
                Object mk, mv, v;
                if ((mk = e.getKey()) == null ||
                    (mv = e.getValue()) == null ||
                    (v = get(mk)) == null ||
                    (mv != v && !mv.equals(v))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Stripped-down version of helper class used in previous version,
     * declared for the sake of serialization compatibility
     */
    static class Segment<V> extends ReentrantLock implements Serializable {
        private static final long serialVersionUID = 2249069246763182397L;
        final float loadFactor;

        Segment(float lf) {
            this.loadFactor = lf;
        }
    }

    /**
     * Saves the state of the {@code ByteArrayConcurrentHashMap} instance to a
     * stream (i.e., serializes it).
     *
     * @param s the stream
     * @throws java.io.IOException if an I/O error occurs
     * @serialData the key (Object) and value (Object)
     * for each key-value mapping, followed by a null pair.
     * The key-value mappings are emitted in no particular order.
     */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        // For serialization compatibility
        // Emulate segment calculation from previous version of this class
        int sshift = 0;
        int ssize = 1;
        while (ssize < DEFAULT_CONCURRENCY_LEVEL) {
            ++sshift;
            ssize <<= 1;
        }
        int segmentShift = 32 - sshift;
        int segmentMask = ssize - 1;
        @SuppressWarnings("unchecked")
        Segment<V>[] segments = (Segment<V>[])
            new Segment<?>[DEFAULT_CONCURRENCY_LEVEL];
        for (int i = 0; i < segments.length; ++i)
            segments[i] = new Segment<V>(LOAD_FACTOR);
        s.putFields().put("segments", segments);
        s.putFields().put("segmentShift", segmentShift);
        s.putFields().put("segmentMask", segmentMask);
        s.writeFields();

        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                s.writeObject(p.key);
                s.writeObject(p.val);
            }
        }
        s.writeObject(null);
        s.writeObject(null);
        segments = null; // throw away
    }

    /**
     * Reconstitutes the instance from a stream (that is, deserializes it).
     *
     * @param s the stream
     * @throws ClassNotFoundException if the class of a serialized object
     *                                could not be found
     * @throws java.io.IOException    if an I/O error occurs
     */
    private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        /*
         * To improve performance in typical cases, we create nodes
         * while reading, then place in table once size is known.
         * However, we must also validate uniqueness and deal with
         * overpopulated bins while doing so, which requires
         * specialized versions of putVal mechanics.
         */
        sizeCtl = -1; // force exclusion for table construction
        s.defaultReadObject();
        long size = 0L;
        Node<V> p = null;
        for (; ; ) {
            @SuppressWarnings("unchecked")
            byte[] k = (byte[]) s.readObject();
            @SuppressWarnings("unchecked")
            V v = (V) s.readObject();
            if (k != null && v != null) {
                p = new Node<V>(spread(keyHashCode(k)), k, v, p);
                ++size;
            } else {
                break;
            }
        }
        if (size == 0L) {
            sizeCtl = 0;
        } else {
            int n;
            if (size >= (long) (MAXIMUM_CAPACITY >>> 1)) {
                n = MAXIMUM_CAPACITY;
            } else {
                int sz = (int) size;
                n = tableSizeFor(sz + (sz >>> 1) + 1);
            }
            @SuppressWarnings("unchecked")
            Node<V>[] tab = (Node<V>[]) new Node<?>[n];
            int mask = n - 1;
            long added = 0L;
            while (p != null) {
                boolean insertAtFront;
                Node<V> next = p.next, first;
                int h = p.hash, j = h & mask;
                if ((first = tabAt(tab, j)) == null) {
                    insertAtFront = true;
                } else {
                    byte[] k = p.key;
                    if (first.hash < 0) {
                        TreeBin<V> t = (TreeBin<V>) first;
                        if (t.putTreeVal(h, k, p.val) == null) {
                            ++added;
                        }
                        insertAtFront = false;
                    } else {
                        int binCount = 0;
                        insertAtFront = true;
                        Node<V> q;
                        byte[] qk;
                        for (q = first; q != null; q = q.next) {
                            if (q.hash == h &&
                                ((qk = q.key) == k ||
                                    (qk != null && keyEquals(k, qk)))) {
                                insertAtFront = false;
                                break;
                            }
                            ++binCount;
                        }
                        if (insertAtFront && binCount >= TREEIFY_THRESHOLD) {
                            insertAtFront = false;
                            ++added;
                            p.next = first;
                            TreeNode<V> hd = null, tl = null;
                            for (q = p; q != null; q = q.next) {
                                TreeNode<V> t = new TreeNode<V>
                                    (q.hash, q.key, q.val, null, null);
                                if ((t.prev = tl) == null) {
                                    hd = t;
                                } else {
                                    tl.next = t;
                                }
                                tl = t;
                            }
                            setTabAt(tab, j, new TreeBin<V>(hd));
                        }
                    }
                }
                if (insertAtFront) {
                    ++added;
                    p.next = first;
                    setTabAt(tab, j, p);
                }
                p = next;
            }
            table = tab;
            sizeCtl = n - (n >>> 2);
            baseCount = added;
        }
    }

    // ConcurrentMap methods

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(byte[] key, V value) {
        return putVal(key, value, true);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    public boolean remove(Object key, Object value) {
        if (key == null) {
            throw new NullPointerException();
        }
        return value != null && replaceNode(key, null, value) != null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    public boolean replace(byte[] key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        return replaceNode(key, newValue, oldValue) != null;
    }

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public V replace(byte[] key, V value) {
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        return replaceNode(key, value, null);
    }

    // Overrides of JDK8+ Map extension method defaults

    /**
     * Returns the value to which the specified key is mapped, or the
     * given default value if this map contains no mapping for the
     * key.
     *
     * @param key          the key whose associated value is to be returned
     * @param defaultValue the value to return if this map contains
     *                     no mapping for the given key
     * @return the mapping for the key, if present; else the default value
     * @throws NullPointerException if the specified key is null
     */
    public V getOrDefault(Object key, V defaultValue) {
        V v;
        return (v = get(key)) == null ? defaultValue : v;
    }

    public void forEach(BiConsumer<? super byte[], ? super V> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                action.accept(p.key, p.val);
            }
        }
    }

    public void replaceAll(BiFunction<? super byte[], ? super V, ? extends V> function) {
        if (function == null) {
            throw new NullPointerException();
        }
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V oldValue = p.val;
                for (byte[] key = p.key; ; ) {
                    V newValue = function.apply(key, oldValue);
                    if (newValue == null) {
                        throw new NullPointerException();
                    }
                    if (replaceNode(key, newValue, oldValue) != null ||
                        (oldValue = get(key)) == null) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * If the specified key is not already associated with a value,
     * attempts to compute its value using the given mapping function
     * and enters it into this map unless {@code null}.  The entire
     * method invocation is performed atomically, so the function is
     * applied at most once per key.  Some attempted update operations
     * on this map by other threads may be blocked while computation
     * is in progress, so the computation should be short and simple,
     * and must not attempt to update any other mappings of this map.
     *
     * @param key             key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with
     * the specified key, or null if the computed value is null
     * @throws NullPointerException  if the specified key or mappingFunction
     *                               is null
     * @throws IllegalStateException if the computation detectably
     *                               attempts a recursive update to this map that would
     *                               otherwise never complete
     * @throws RuntimeException      or Error if the mappingFunction does so,
     *                               in which case the mapping is left unestablished
     */
    public V computeIfAbsent(byte[] key, Function<? super byte[], ? extends V> mappingFunction) {
        if (key == null || mappingFunction == null) {
            throw new NullPointerException();
        }
        int h = spread(keyHashCode(key));
        V val = null;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, null, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = mappingFunction.apply(key)) != null) {
                                node = new Node<V>(h, key, val, null);
                            }
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0) {
                    break;
                }
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                boolean added = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f; ; ++binCount) {
                                byte[] ek;
                                V ev;
                                if (e.hash == h &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    val = e.val;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    if ((val = mappingFunction.apply(key)) != null) {
                                        added = true;
                                        pred.next = new Node<V>(h, key, val, null);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                (p = r.findTreeNode(h, key, null)) != null) {
                                val = p.val;
                            } else if ((val = mappingFunction.apply(key)) != null) {
                                added = true;
                                t.putTreeVal(h, key, val);
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD) {
                        treeifyBin(tab, i);
                    }
                    if (!added) {
                        return val;
                    }
                    break;
                }
            }
        }
        if (val != null) {
            addCount(1L, binCount);
        }
        return val;
    }

    /**
     * If the value for the specified key is present, attempts to
     * compute a new mapping given the key and its current mapped
     * value.  The entire method invocation is performed atomically.
     * Some attempted update operations on this map by other threads
     * may be blocked while computation is in progress, so the
     * computation should be short and simple, and must not attempt to
     * update any other mappings of this map.
     *
     * @param key               key with which a value may be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException  if the specified key or remappingFunction
     *                               is null
     * @throws IllegalStateException if the computation detectably
     *                               attempts a recursive update to this map that would
     *                               otherwise never complete
     * @throws RuntimeException      or Error if the remappingFunction does so,
     *                               in which case the mapping is unchanged
     */
    public V computeIfPresent(byte[] key, BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        if (key == null || remappingFunction == null) {
            throw new NullPointerException();
        }
        int h = spread(keyHashCode(key));
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                break;
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null; ; ++binCount) {
                                byte[] ek;
                                if (e.hash == h &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null) {
                                        e.val = val;
                                    } else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null) {
                                            pred.next = en;
                                        } else {
                                            setTabAt(tab, i, en);
                                        }
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null &&
                                (p = r.findTreeNode(h, key, null)) != null) {
                                val = remappingFunction.apply(key, p.val);
                                if (val != null) {
                                    p.val = val;
                                } else {
                                    delta = -1;
                                    if (t.removeTreeNode(p)) {
                                        setTabAt(tab, i, untreeify(t.first));
                                    }
                                }
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    break;
                }
            }
        }
        if (delta != 0) {
            addCount((long) delta, binCount);
        }
        return val;
    }

    /**
     * Attempts to compute a mapping for the specified key and its
     * current mapped value (or {@code null} if there is no current
     * mapping). The entire method invocation is performed atomically.
     * Some attempted update operations on this map by other threads
     * may be blocked while computation is in progress, so the
     * computation should be short and simple, and must not attempt to
     * update any other mappings of this Map.
     *
     * @param key               key with which the specified value is to be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException  if the specified key or remappingFunction
     *                               is null
     * @throws IllegalStateException if the computation detectably
     *                               attempts a recursive update to this map that would
     *                               otherwise never complete
     * @throws RuntimeException      or Error if the remappingFunction does so,
     *                               in which case the mapping is unchanged
     */
    public V compute(byte[] key,
        BiFunction<? super byte[], ? super V, ? extends V> remappingFunction) {
        if (key == null || remappingFunction == null) {
            throw new NullPointerException();
        }
        int h = spread(keyHashCode(key));
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, null, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = remappingFunction.apply(key, null)) != null) {
                                delta = 1;
                                node = new Node<V>(h, key, val, null);
                            }
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0) {
                    break;
                }
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null; ; ++binCount) {
                                byte[] ek;
                                if (e.hash == h &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null) {
                                        e.val = val;
                                    } else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null) {
                                            pred.next = en;
                                        } else {
                                            setTabAt(tab, i, en);
                                        }
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    val = remappingFunction.apply(key, null);
                                    if (val != null) {
                                        delta = 1;
                                        pred.next =
                                            new Node<V>(h, key, val, null);
                                    }
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 1;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r, p;
                            if ((r = t.root) != null) {
                                p = r.findTreeNode(h, key, null);
                            } else {
                                p = null;
                            }
                            V pv = (p == null) ? null : p.val;
                            val = remappingFunction.apply(key, pv);
                            if (val != null) {
                                if (p != null) {
                                    p.val = val;
                                } else {
                                    delta = 1;
                                    t.putTreeVal(h, key, val);
                                }
                            } else if (p != null) {
                                delta = -1;
                                if (t.removeTreeNode(p)) {
                                    setTabAt(tab, i, untreeify(t.first));
                                }
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD) {
                        treeifyBin(tab, i);
                    }
                    break;
                }
            }
        }
        if (delta != 0) {
            addCount((long) delta, binCount);
        }
        return val;
    }

    /**
     * If the specified key is not already associated with a
     * (non-null) value, associates it with the given value.
     * Otherwise, replaces the value with the results of the given
     * remapping function, or removes if {@code null}. The entire
     * method invocation is performed atomically.  Some attempted
     * update operations on this map by other threads may be blocked
     * while computation is in progress, so the computation should be
     * short and simple, and must not attempt to update any other
     * mappings of this Map.
     *
     * @param key               key with which the specified value is to be associated
     * @param value             the value to use if absent
     * @param remappingFunction the function to recompute a value if present
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException if the specified key or the
     *                              remappingFunction is null
     * @throws RuntimeException     or Error if the remappingFunction does so,
     *                              in which case the mapping is unchanged
     */
    public V merge(byte[] key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        if (key == null || value == null || remappingFunction == null) {
            throw new NullPointerException();
        }
        int h = spread(keyHashCode(key));
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table; ; ) {
            Node<V> f;
            int n, i, fh;
            if (tab == null || (n = tab.length) == 0) {
                tab = initTable();
            } else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                if (casTabAt(tab, i, null, new Node<V>(h, key, value, null))) {
                    delta = 1;
                    val = value;
                    break;
                }
            } else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null; ; ++binCount) {
                                byte[] ek;
                                if (e.hash == h &&
                                    ((ek = e.key) == key ||
                                        (ek != null && keyEquals(key, ek)))) {
                                    val = remappingFunction.apply(e.val, value);
                                    if (val != null) {
                                        e.val = val;
                                    } else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null) {
                                            pred.next = en;
                                        } else {
                                            setTabAt(tab, i, en);
                                        }
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    delta = 1;
                                    val = value;
                                    pred.next =
                                        new Node<V>(h, key, val, null);
                                    break;
                                }
                            }
                        } else if (f instanceof TreeBin) {
                            binCount = 2;
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> r = t.root;
                            TreeNode<V> p = (r == null) ? null :
                                r.findTreeNode(h, key, null);
                            val = (p == null) ? value :
                                remappingFunction.apply(p.val, value);
                            if (val != null) {
                                if (p != null) {
                                    p.val = val;
                                } else {
                                    delta = 1;
                                    t.putTreeVal(h, key, val);
                                }
                            } else if (p != null) {
                                delta = -1;
                                if (t.removeTreeNode(p)) {
                                    setTabAt(tab, i, untreeify(t.first));
                                }
                            }
                        }
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD) {
                        treeifyBin(tab, i);
                    }
                    break;
                }
            }
        }
        if (delta != 0) {
            addCount((long) delta, binCount);
        }
        return val;
    }

    // Hashtable legacy methods

    /**
     * Legacy method testing if some key maps into the specified value
     * in this table.  This method is identical in functionality to
     * {@link #containsValue(Object)}, and exists solely to ensure
     * full compatibility with class {@link java.util.Hashtable},
     * which supported this method prior to introduction of the
     * Java Collections framework.
     *
     * @param value a value to search for
     * @return {@code true} if and only if some key maps to the
     * {@code value} argument in this table as
     * determined by the {@code equals} method;
     * {@code false} otherwise
     * @throws NullPointerException if the specified value is null
     */
    public boolean contains(Object value) {
        return containsValue(value);
    }

    /**
     * Returns an enumeration of the keys in this table.
     *
     * @return an enumeration of the keys in this table
     * @see #keySet()
     */
    public Enumeration<byte[]> keys() {
        Node<V>[] t;
        int f = (t = table) == null ? 0 : t.length;
        return new KeyIterator<V>(t, f, 0, f, this);
    }

    /**
     * Returns an enumeration of the values in this table.
     *
     * @return an enumeration of the values in this table
     * @see #values()
     */
    public Enumeration<V> elements() {
        Node<V>[] t;
        int f = (t = table) == null ? 0 : t.length;
        return new ValueIterator<V>(t, f, 0, f, this);
    }

    // ByteArrayConcurrentHashMap-only methods

    /**
     * Returns the number of mappings. This method should be used
     * instead of {@link #size} because a ByteArrayConcurrentHashMap may
     * contain more mappings than can be represented as an int. The
     * value returned is an estimate; the actual count may differ if
     * there are concurrent insertions or removals.
     *
     * @return the number of mappings
     * @since 1.8
     */
    public long mappingCount() {
        long n = sumCount();
        return (n < 0L) ? 0L : n; // ignore transient negative values
    }

    /**
     * Creates a new {@link Set} backed by a ByteArrayConcurrentHashMap
     * from the given type to {@code Boolean.TRUE}.
     *
     * @param <K> the element type of the returned set
     * @return the new set
     * @since 1.8
     */
    public static <K> KeySetView<Boolean> newKeySet() {
        return new KeySetView<Boolean>
            (new ByteArrayConcurrentHashMap<Boolean>(), Boolean.TRUE);
    }

    /**
     * Creates a new {@link Set} backed by a ByteArrayConcurrentHashMap
     * from the given type to {@code Boolean.TRUE}.
     *
     * @param initialCapacity The implementation performs internal
     *                        sizing to accommodate this many elements.
     * @param <K>             the element type of the returned set
     * @return the new set
     * @throws IllegalArgumentException if the initial capacity of
     *                                  elements is negative
     * @since 1.8
     */
    public static <K> KeySetView<Boolean> newKeySet(int initialCapacity) {
        return new KeySetView<Boolean>
            (new ByteArrayConcurrentHashMap<Boolean>(initialCapacity), Boolean.TRUE);
    }

    /**
     * Returns a {@link Set} view of the keys in this map, using the
     * given common mapped value for any additions (i.e., {@link
     * Collection#add} and {@link Collection#addAll(Collection)}).
     * This is of course only appropriate if it is acceptable to use
     * the same value for all additions from this view.
     *
     * @param mappedValue the mapped value to use for any additions
     * @return the set view
     * @throws NullPointerException if the mappedValue is null
     */
    public KeySetView<V> keySet(V mappedValue) {
        if (mappedValue == null) {
            throw new NullPointerException();
        }
        return new KeySetView<V>(this, mappedValue);
    }

    /* ---------------- Special Nodes -------------- */

    /**
     * A node inserted at head of bins during transfer operations.
     */
    static final class ForwardingNode<V> extends Node<V> {
        final Node<V>[] nextTable;

        ForwardingNode(Node<V>[] tab) {
            super(MOVED, null, null, null);
            this.nextTable = tab;
        }

        Node<V> find(int h, Object k) {
            // loop to avoid arbitrarily deep recursion on forwarding nodes
            outer:
            for (Node<V>[] tab = nextTable; ; ) {
                Node<V> e;
                int n;
                if (k == null || tab == null || (n = tab.length) == 0 ||
                    (e = tabAt(tab, (n - 1) & h)) == null) {
                    return null;
                }
                for (; ; ) {
                    int eh;
                    byte[] ek;
                    if ((eh = e.hash) == h &&
                        ((ek = e.key) == k || (ek != null && keyEquals(k, ek)))) {
                        return e;
                    }
                    if (eh < 0) {
                        if (e instanceof ForwardingNode) {
                            tab = ((ForwardingNode<V>) e).nextTable;
                            continue outer;
                        } else {
                            return e.find(h, k);
                        }
                    }
                    if ((e = e.next) == null) {
                        return null;
                    }
                }
            }
        }
    }

    /**
     * A place-holder node used in computeIfAbsent and compute
     */
    static final class ReservationNode<V> extends Node<V> {
        ReservationNode() {
            super(RESERVED, null, null, null);
        }

        Node<V> find(int h, Object k) {
            return null;
        }
    }

    /* ---------------- Table Initialization and Resizing -------------- */

    /**
     * Returns the stamp bits for resizing a table of size n.
     * Must be negative when shifted left by RESIZE_STAMP_SHIFT.
     */
    static final int resizeStamp(int n) {
        return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
    }

    /**
     * Initializes table, using the size recorded in sizeCtl.
     */
    private final Node<V>[] initTable() {
        Node<V>[] tab;
        int sc;
        while ((tab = table) == null || tab.length == 0) {
            if ((sc = sizeCtl) < 0) {
                Thread.yield(); // lost initialization race; just spin
            } else if (U.compareAndSwapInt(this, SIZECTL, sc, -1)) {
                try {
                    if ((tab = table) == null || tab.length == 0) {
                        int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                        @SuppressWarnings("unchecked")
                        Node<V>[] nt = (Node<V>[]) new Node<?>[n];
                        table = tab = nt;
                        sc = n - (n >>> 2);
                    }
                } finally {
                    sizeCtl = sc;
                }
                break;
            }
        }
        return tab;
    }

    /**
     * Adds to count, and if table is too small and not already
     * resizing, initiates transfer. If already resizing, helps
     * perform transfer if work is available.  Rechecks occupancy
     * after a transfer to see if another resize is already needed
     * because resizings are lagging additions.
     *
     * @param x     the count to add
     * @param check if <0, don't check resize, if <= 1 only check if uncontended
     */
    private final void addCount(long x, int check) {
        CounterCell[] as;
        long b, s;
        if ((as = counterCells) != null ||
            !U.compareAndSwapLong(this, BASECOUNT, b = baseCount, s = b + x)) {
            CounterCell a;
            long v;
            int m;
            boolean uncontended = true;
            if (as == null || (m = as.length - 1) < 0 ||
                (a = as[ThreadLocalRandom.getProbe() & m]) == null ||
                !(uncontended =
                    U.compareAndSwapLong(a, CELLVALUE, v = a.value, v + x))) {
                fullAddCount(x, uncontended);
                return;
            }
            if (check <= 1) {
                return;
            }
            s = sumCount();
        }
        if (check >= 0) {
            Node<V>[] tab, nt;
            int n, sc;
            while (s >= (long) (sc = sizeCtl) && (tab = table) != null &&
                (n = tab.length) < MAXIMUM_CAPACITY) {
                int rs = resizeStamp(n);
                if (sc < 0) {
                    if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                        sc == rs + MAX_RESIZERS || (nt = nextTable) == null ||
                        transferIndex <= 0) {
                        break;
                    }
                    if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
                        transfer(tab, nt);
                    }
                } else if (U.compareAndSwapInt(this, SIZECTL, sc,
                    (rs << RESIZE_STAMP_SHIFT) + 2)) {
                    transfer(tab, null);
                }
                s = sumCount();
            }
        }
    }

    /**
     * Helps transfer if a resize is in progress.
     */
    final Node<V>[] helpTransfer(Node<V>[] tab, Node<V> f) {
        Node<V>[] nextTab;
        int sc;
        if (tab != null && (f instanceof ForwardingNode) &&
            (nextTab = ((ForwardingNode<V>) f).nextTable) != null) {
            int rs = resizeStamp(tab.length);
            while (nextTab == nextTable && table == tab &&
                (sc = sizeCtl) < 0) {
                if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                    sc == rs + MAX_RESIZERS || transferIndex <= 0) {
                    break;
                }
                if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
                    transfer(tab, nextTab);
                    break;
                }
            }
            return nextTab;
        }
        return table;
    }

    /**
     * Tries to presize table to accommodate the given number of elements.
     *
     * @param size number of elements (doesn't need to be perfectly accurate)
     */
    private final void tryPresize(int size) {
        int c = (size >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
            tableSizeFor(size + (size >>> 1) + 1);
        int sc;
        while ((sc = sizeCtl) >= 0) {
            Node<V>[] tab = table;
            int n;
            if (tab == null || (n = tab.length) == 0) {
                n = (sc > c) ? sc : c;
                if (U.compareAndSwapInt(this, SIZECTL, sc, -1)) {
                    try {
                        if (table == tab) {
                            @SuppressWarnings("unchecked")
                            Node<V>[] nt = (Node<V>[]) new Node<?>[n];
                            table = nt;
                            sc = n - (n >>> 2);
                        }
                    } finally {
                        sizeCtl = sc;
                    }
                }
            } else if (c <= sc || n >= MAXIMUM_CAPACITY) {
                break;
            } else if (tab == table) {
                int rs = resizeStamp(n);
                if (sc < 0) {
                    Node<V>[] nt;
                    if ((sc >>> RESIZE_STAMP_SHIFT) != rs || sc == rs + 1 ||
                        sc == rs + MAX_RESIZERS || (nt = nextTable) == null ||
                        transferIndex <= 0) {
                        break;
                    }
                    if (U.compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
                        transfer(tab, nt);
                    }
                } else if (U.compareAndSwapInt(this, SIZECTL, sc,
                    (rs << RESIZE_STAMP_SHIFT) + 2)) {
                    transfer(tab, null);
                }
            }
        }
    }

    /**
     * Moves and/or copies the nodes in each bin to new table. See
     * above for explanation.
     */
    private final void transfer(Node<V>[] tab, Node<V>[] nextTab) {
        int n = tab.length, stride;
        if ((stride = (NCPU > 1) ? (n >>> 3) / NCPU : n) < MIN_TRANSFER_STRIDE) {
            stride = MIN_TRANSFER_STRIDE; // subdivide range
        }
        if (nextTab == null) {            // initiating
            try {
                @SuppressWarnings("unchecked")
                Node<V>[] nt = (Node<V>[]) new Node<?>[n << 1];
                nextTab = nt;
            } catch (Throwable ex) {      // try to cope with OOME
                sizeCtl = Integer.MAX_VALUE;
                return;
            }
            nextTable = nextTab;
            transferIndex = n;
        }
        int nextn = nextTab.length;
        ForwardingNode<V> fwd = new ForwardingNode<V>(nextTab);
        boolean advance = true;
        boolean finishing = false; // to ensure sweep before committing nextTab
        for (int i = 0, bound = 0; ; ) {
            Node<V> f;
            int fh;
            while (advance) {
                int nextIndex, nextBound;
                if (--i >= bound || finishing) {
                    advance = false;
                } else if ((nextIndex = transferIndex) <= 0) {
                    i = -1;
                    advance = false;
                } else if (U.compareAndSwapInt
                    (this, TRANSFERINDEX, nextIndex,
                        nextBound = (nextIndex > stride ?
                            nextIndex - stride : 0))) {
                    bound = nextBound;
                    i = nextIndex - 1;
                    advance = false;
                }
            }
            if (i < 0 || i >= n || i + n >= nextn) {
                int sc;
                if (finishing) {
                    nextTable = null;
                    table = nextTab;
                    sizeCtl = (n << 1) - (n >>> 1);
                    return;
                }
                if (U.compareAndSwapInt(this, SIZECTL, sc = sizeCtl, sc - 1)) {
                    if ((sc - 2) != resizeStamp(n) << RESIZE_STAMP_SHIFT) {
                        return;
                    }
                    finishing = advance = true;
                    i = n; // recheck before commit
                }
            } else if ((f = tabAt(tab, i)) == null) {
                advance = casTabAt(tab, i, null, fwd);
            } else if ((fh = f.hash) == MOVED) {
                advance = true; // already processed
            } else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> ln, hn;
                        if (fh >= 0) {
                            int runBit = fh & n;
                            Node<V> lastRun = f;
                            for (Node<V> p = f.next; p != null; p = p.next) {
                                int b = p.hash & n;
                                if (b != runBit) {
                                    runBit = b;
                                    lastRun = p;
                                }
                            }
                            if (runBit == 0) {
                                ln = lastRun;
                                hn = null;
                            } else {
                                hn = lastRun;
                                ln = null;
                            }
                            for (Node<V> p = f; p != lastRun; p = p.next) {
                                int ph = p.hash;
                                byte[] pk = p.key;
                                V pv = p.val;
                                if ((ph & n) == 0) {
                                    ln = new Node<V>(ph, pk, pv, ln);
                                } else {
                                    hn = new Node<V>(ph, pk, pv, hn);
                                }
                            }
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        } else if (f instanceof TreeBin) {
                            TreeBin<V> t = (TreeBin<V>) f;
                            TreeNode<V> lo = null, loTail = null;
                            TreeNode<V> hi = null, hiTail = null;
                            int lc = 0, hc = 0;
                            for (Node<V> e = t.first; e != null; e = e.next) {
                                int h = e.hash;
                                TreeNode<V> p = new TreeNode<V>
                                    (h, e.key, e.val, null, null);
                                if ((h & n) == 0) {
                                    if ((p.prev = loTail) == null) {
                                        lo = p;
                                    } else {
                                        loTail.next = p;
                                    }
                                    loTail = p;
                                    ++lc;
                                } else {
                                    if ((p.prev = hiTail) == null) {
                                        hi = p;
                                    } else {
                                        hiTail.next = p;
                                    }
                                    hiTail = p;
                                    ++hc;
                                }
                            }
                            ln = (lc <= UNTREEIFY_THRESHOLD) ? untreeify(lo) :
                                (hc != 0) ? new TreeBin<V>(lo) : t;
                            hn = (hc <= UNTREEIFY_THRESHOLD) ? untreeify(hi) :
                                (lc != 0) ? new TreeBin<V>(hi) : t;
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        }
                    }
                }
            }
        }
    }

    /* ---------------- Counter support -------------- */

    /**
     * A padded cell for distributing counts.  Adapted from LongAdder
     * and Striped64.  See their internal docs for explanation.
     */
    @sun.misc.Contended
    static final class CounterCell {
        volatile long value;

        CounterCell(long x) {
            value = x;
        }
    }

    final long sumCount() {
        CounterCell[] as = counterCells;
        CounterCell a;
        long sum = baseCount;
        if (as != null) {
            for (int i = 0; i < as.length; ++i) {
                if ((a = as[i]) != null) {
                    sum += a.value;
                }
            }
        }
        return sum;
    }

    // See LongAdder version for explanation
    private final void fullAddCount(long x, boolean wasUncontended) {
        int h;
        if ((h = ThreadLocalRandom.getProbe()) == 0) {
            ThreadLocalRandom.localInit();      // force initialization
            h = ThreadLocalRandom.getProbe();
            wasUncontended = true;
        }
        boolean collide = false;                // True if last slot nonempty
        for (; ; ) {
            CounterCell[] as;
            CounterCell a;
            int n;
            long v;
            if ((as = counterCells) != null && (n = as.length) > 0) {
                if ((a = as[(n - 1) & h]) == null) {
                    if (cellsBusy == 0) {            // Try to attach new Cell
                        CounterCell r = new CounterCell(x); // Optimistic create
                        if (cellsBusy == 0 &&
                            U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                            boolean created = false;
                            try {               // Recheck under lock
                                CounterCell[] rs;
                                int m, j;
                                if ((rs = counterCells) != null &&
                                    (m = rs.length) > 0 &&
                                    rs[j = (m - 1) & h] == null) {
                                    rs[j] = r;
                                    created = true;
                                }
                            } finally {
                                cellsBusy = 0;
                            }
                            if (created) {
                                break;
                            }
                            continue;           // Slot is now non-empty
                        }
                    }
                    collide = false;
                } else if (!wasUncontended)       // CAS already known to fail
                {
                    wasUncontended = true;      // Continue after rehash
                } else if (U.compareAndSwapLong(a, CELLVALUE, v = a.value, v + x)) {
                    break;
                } else if (counterCells != as || n >= NCPU) {
                    collide = false;            // At max size or stale
                } else if (!collide) {
                    collide = true;
                } else if (cellsBusy == 0 &&
                    U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                    try {
                        if (counterCells == as) {// Expand table unless stale
                            CounterCell[] rs = new CounterCell[n << 1];
                            for (int i = 0; i < n; ++i)
                                rs[i] = as[i];
                            counterCells = rs;
                        }
                    } finally {
                        cellsBusy = 0;
                    }
                    collide = false;
                    continue;                   // Retry with expanded table
                }
                h = ThreadLocalRandom.advanceProbe(h);
            } else if (cellsBusy == 0 && counterCells == as &&
                U.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                boolean init = false;
                try {                           // Initialize table
                    if (counterCells == as) {
                        CounterCell[] rs = new CounterCell[2];
                        rs[h & 1] = new CounterCell(x);
                        counterCells = rs;
                        init = true;
                    }
                } finally {
                    cellsBusy = 0;
                }
                if (init) {
                    break;
                }
            } else if (U.compareAndSwapLong(this, BASECOUNT, v = baseCount, v + x)) {
                break;                          // Fall back on using base
            }
        }
    }

    /* ---------------- Conversion from/to TreeBins -------------- */

    /**
     * Replaces all linked nodes in bin at given index unless table is
     * too small, in which case resizes instead.
     */
    private final void treeifyBin(Node<V>[] tab, int index) {
        Node<V> b;
        int n, sc;
        if (tab != null) {
            if ((n = tab.length) < MIN_TREEIFY_CAPACITY) {
                tryPresize(n << 1);
            } else if ((b = tabAt(tab, index)) != null && b.hash >= 0) {
                synchronized (b) {
                    if (tabAt(tab, index) == b) {
                        TreeNode<V> hd = null, tl = null;
                        for (Node<V> e = b; e != null; e = e.next) {
                            TreeNode<V> p =
                                new TreeNode<V>(e.hash, e.key, e.val, null, null);
                            if ((p.prev = tl) == null) {
                                hd = p;
                            } else {
                                tl.next = p;
                            }
                            tl = p;
                        }
                        setTabAt(tab, index, new TreeBin<V>(hd));
                    }
                }
            }
        }
    }

    /**
     * Returns a list on non-TreeNodes replacing those in given list.
     */
    static <V> Node<V> untreeify(Node<V> b) {
        Node<V> hd = null, tl = null;
        for (Node<V> q = b; q != null; q = q.next) {
            Node<V> p = new Node<V>(q.hash, q.key, q.val, null);
            if (tl == null) {
                hd = p;
            } else {
                tl.next = p;
            }
            tl = p;
        }
        return hd;
    }

    /* ---------------- TreeNodes -------------- */

    /**
     * Nodes for use in TreeBins
     */
    static final class TreeNode<V> extends Node<V> {
        TreeNode<V> parent;  // red-black tree links
        TreeNode<V> left;
        TreeNode<V> right;
        TreeNode<V> prev;    // needed to unlink next upon deletion
        boolean red;

        TreeNode(int hash, byte[] key, V val, Node<V> next,
            TreeNode<V> parent) {
            super(hash, key, val, next);
            this.parent = parent;
        }

        Node<V> find(int h, Object k) {
            return findTreeNode(h, k, null);
        }

        /**
         * Returns the TreeNode (or null if not found) for the given key
         * starting at given root.
         */
        final TreeNode<V> findTreeNode(int h, Object k, Class<?> kc) {
            if (k != null) {
                TreeNode<V> p = this;
                do {
                    int ph, dir;
                    byte[] pk;
                    TreeNode<V> q;
                    TreeNode<V> pl = p.left, pr = p.right;
                    if ((ph = p.hash) > h) {
                        p = pl;
                    } else if (ph < h) {
                        p = pr;
                    } else if ((pk = p.key) == k || (pk != null && keyEquals(k, pk))) {
                        return p;
                    } else if (pl == null) {
                        p = pr;
                    } else if (pr == null) {
                        p = pl;
                    } else if ((kc != null ||
                        (kc = comparableClassFor(k)) != null) &&
                        (dir = compareComparables(kc, k, pk)) != 0) {
                        p = (dir < 0) ? pl : pr;
                    } else if ((q = pr.findTreeNode(h, k, kc)) != null) {
                        return q;
                    } else {
                        p = pl;
                    }
                }
                while (p != null);
            }
            return null;
        }
    }

    /* ---------------- TreeBins -------------- */

    /**
     * TreeNodes used at the heads of bins. TreeBins do not hold user
     * keys or values, but instead point to list of TreeNodes and
     * their root. They also maintain a parasitic read-write lock
     * forcing writers (who hold bin lock) to wait for readers (who do
     * not) to complete before tree restructuring operations.
     */
    static final class TreeBin<V> extends Node<V> {
        TreeNode<V> root;
        volatile TreeNode<V> first;
        volatile Thread waiter;
        volatile int lockState;
        // values for lockState
        static final int WRITER = 1; // set while holding write lock
        static final int WAITER = 2; // set when waiting for write lock
        static final int READER = 4; // increment value for setting read lock

        /**
         * Tie-breaking utility for ordering insertions when equal
         * hashCodes and non-comparable. We don't require a total
         * order, just a consistent insertion rule to maintain
         * equivalence across rebalancings. Tie-breaking further than
         * necessary simplifies testing a bit.
         */
        static int tieBreakOrder(Object a, Object b) {
            int d;
            if (a == null || b == null ||
                (d = a.getClass().getName().
                    compareTo(b.getClass().getName())) == 0) {
                d = (System.identityHashCode(a) <= System.identityHashCode(b) ?
                    -1 : 1);
            }
            return d;
        }

        /**
         * Creates bin with initial set of nodes headed by b.
         */
        TreeBin(TreeNode<V> b) {
            super(TREEBIN, null, null, null);
            this.first = b;
            TreeNode<V> r = null;
            for (TreeNode<V> x = b, next; x != null; x = next) {
                next = (TreeNode<V>) x.next;
                x.left = x.right = null;
                if (r == null) {
                    x.parent = null;
                    x.red = false;
                    r = x;
                } else {
                    byte[] k = x.key;
                    int h = x.hash;
                    Class<?> kc = null;
                    for (TreeNode<V> p = r; ; ) {
                        int dir, ph;
                        byte[] pk = p.key;
                        if ((ph = p.hash) > h) {
                            dir = -1;
                        } else if (ph < h) {
                            dir = 1;
                        } else if ((kc == null &&
                            (kc = comparableClassFor(k)) == null) ||
                            (dir = compareComparables(kc, k, pk)) == 0) {
                            dir = tieBreakOrder(k, pk);
                        }
                        TreeNode<V> xp = p;
                        if ((p = (dir <= 0) ? p.left : p.right) == null) {
                            x.parent = xp;
                            if (dir <= 0) {
                                xp.left = x;
                            } else {
                                xp.right = x;
                            }
                            r = balanceInsertion(r, x);
                            break;
                        }
                    }
                }
            }
            this.root = r;
            assert checkInvariants(root);
        }

        /**
         * Acquires write lock for tree restructuring.
         */
        private final void lockRoot() {
            if (!U.compareAndSwapInt(this, LOCKSTATE, 0, WRITER)) {
                contendedLock(); // offload to separate method
            }
        }

        /**
         * Releases write lock for tree restructuring.
         */
        private final void unlockRoot() {
            lockState = 0;
        }

        /**
         * Possibly blocks awaiting root lock.
         */
        private final void contendedLock() {
            boolean waiting = false;
            for (int s; ; ) {
                if (((s = lockState) & ~WAITER) == 0) {
                    if (U.compareAndSwapInt(this, LOCKSTATE, s, WRITER)) {
                        if (waiting) {
                            waiter = null;
                        }
                        return;
                    }
                } else if ((s & WAITER) == 0) {
                    if (U.compareAndSwapInt(this, LOCKSTATE, s, s | WAITER)) {
                        waiting = true;
                        waiter = Thread.currentThread();
                    }
                } else if (waiting) {
                    LockSupport.park(this);
                }
            }
        }

        /**
         * Returns matching node or null if none. Tries to search
         * using tree comparisons from root, but continues linear
         * search when lock not available.
         */
        final Node<V> find(int h, Object k) {
            if (k != null) {
                for (Node<V> e = first; e != null; ) {
                    int s;
                    byte[] ek;
                    if (((s = lockState) & (WAITER | WRITER)) != 0) {
                        if (e.hash == h &&
                            ((ek = e.key) == k || (ek != null && keyEquals(k, ek)))) {
                            return e;
                        }
                        e = e.next;
                    } else if (U.compareAndSwapInt(this, LOCKSTATE, s,
                        s + READER)) {
                        TreeNode<V> r, p;
                        try {
                            p = ((r = root) == null ? null :
                                r.findTreeNode(h, k, null));
                        } finally {
                            Thread w;
                            if (U.getAndAddInt(this, LOCKSTATE, -READER) ==
                                (READER | WAITER) && (w = waiter) != null) {
                                LockSupport.unpark(w);
                            }
                        }
                        return p;
                    }
                }
            }
            return null;
        }

        /**
         * Finds or adds a node.
         *
         * @return null if added
         */
        final TreeNode<V> putTreeVal(int h, byte[] k, V v) {
            Class<?> kc = null;
            boolean searched = false;
            for (TreeNode<V> p = root; ; ) {
                int dir, ph;
                byte[] pk;
                if (p == null) {
                    first = root = new TreeNode<V>(h, k, v, null, null);
                    break;
                } else if ((ph = p.hash) > h) {
                    dir = -1;
                } else if (ph < h) {
                    dir = 1;
                } else if ((pk = p.key) == k || (pk != null && keyEquals(k, pk))) {
                    return p;
                } else if ((kc == null &&
                    (kc = comparableClassFor(k)) == null) ||
                    (dir = compareComparables(kc, k, pk)) == 0) {
                    if (!searched) {
                        TreeNode<V> q, ch;
                        searched = true;
                        if (((ch = p.left) != null &&
                            (q = ch.findTreeNode(h, k, kc)) != null) ||
                            ((ch = p.right) != null &&
                                (q = ch.findTreeNode(h, k, kc)) != null)) {
                            return q;
                        }
                    }
                    dir = tieBreakOrder(k, pk);
                }

                TreeNode<V> xp = p;
                if ((p = (dir <= 0) ? p.left : p.right) == null) {
                    TreeNode<V> x, f = first;
                    first = x = new TreeNode<V>(h, k, v, f, xp);
                    if (f != null) {
                        f.prev = x;
                    }
                    if (dir <= 0) {
                        xp.left = x;
                    } else {
                        xp.right = x;
                    }
                    if (!xp.red) {
                        x.red = true;
                    } else {
                        lockRoot();
                        try {
                            root = balanceInsertion(root, x);
                        } finally {
                            unlockRoot();
                        }
                    }
                    break;
                }
            }
            assert checkInvariants(root);
            return null;
        }

        /**
         * Removes the given node, that must be present before this
         * call.  This is messier than typical red-black deletion code
         * because we cannot swap the contents of an interior node
         * with a leaf successor that is pinned by "next" pointers
         * that are accessible independently of lock. So instead we
         * swap the tree linkages.
         *
         * @return true if now too small, so should be untreeified
         */
        final boolean removeTreeNode(TreeNode<V> p) {
            TreeNode<V> next = (TreeNode<V>) p.next;
            TreeNode<V> pred = p.prev;  // unlink traversal pointers
            TreeNode<V> r, rl;
            if (pred == null) {
                first = next;
            } else {
                pred.next = next;
            }
            if (next != null) {
                next.prev = pred;
            }
            if (first == null) {
                root = null;
                return true;
            }
            if ((r = root) == null || r.right == null || // too small
                (rl = r.left) == null || rl.left == null) {
                return true;
            }
            lockRoot();
            try {
                TreeNode<V> replacement;
                TreeNode<V> pl = p.left;
                TreeNode<V> pr = p.right;
                if (pl != null && pr != null) {
                    TreeNode<V> s = pr, sl;
                    while ((sl = s.left) != null) // find successor
                        s = sl;
                    boolean c = s.red;
                    s.red = p.red;
                    p.red = c; // swap colors
                    TreeNode<V> sr = s.right;
                    TreeNode<V> pp = p.parent;
                    if (s == pr) { // p was s's direct parent
                        p.parent = s;
                        s.right = p;
                    } else {
                        TreeNode<V> sp = s.parent;
                        if ((p.parent = sp) != null) {
                            if (s == sp.left) {
                                sp.left = p;
                            } else {
                                sp.right = p;
                            }
                        }
                        if ((s.right = pr) != null) {
                            pr.parent = s;
                        }
                    }
                    p.left = null;
                    if ((p.right = sr) != null) {
                        sr.parent = p;
                    }
                    if ((s.left = pl) != null) {
                        pl.parent = s;
                    }
                    if ((s.parent = pp) == null) {
                        r = s;
                    } else if (p == pp.left) {
                        pp.left = s;
                    } else {
                        pp.right = s;
                    }
                    if (sr != null) {
                        replacement = sr;
                    } else {
                        replacement = p;
                    }
                } else if (pl != null) {
                    replacement = pl;
                } else if (pr != null) {
                    replacement = pr;
                } else {
                    replacement = p;
                }
                if (replacement != p) {
                    TreeNode<V> pp = replacement.parent = p.parent;
                    if (pp == null) {
                        r = replacement;
                    } else if (p == pp.left) {
                        pp.left = replacement;
                    } else {
                        pp.right = replacement;
                    }
                    p.left = p.right = p.parent = null;
                }

                root = (p.red) ? r : balanceDeletion(r, replacement);

                if (p == replacement) {  // detach pointers
                    TreeNode<V> pp;
                    if ((pp = p.parent) != null) {
                        if (p == pp.left) {
                            pp.left = null;
                        } else if (p == pp.right) {
                            pp.right = null;
                        }
                        p.parent = null;
                    }
                }
            } finally {
                unlockRoot();
            }
            assert checkInvariants(root);
            return false;
        }

        /* ------------------------------------------------------------ */
        // Red-black tree methods, all adapted from CLR

        static <V> TreeNode<V> rotateLeft(TreeNode<V> root,
            TreeNode<V> p) {
            TreeNode<V> r, pp, rl;
            if (p != null && (r = p.right) != null) {
                if ((rl = p.right = r.left) != null) {
                    rl.parent = p;
                }
                if ((pp = r.parent = p.parent) == null) {
                    (root = r).red = false;
                } else if (pp.left == p) {
                    pp.left = r;
                } else {
                    pp.right = r;
                }
                r.left = p;
                p.parent = r;
            }
            return root;
        }

        static <V> TreeNode<V> rotateRight(TreeNode<V> root,
            TreeNode<V> p) {
            TreeNode<V> l, pp, lr;
            if (p != null && (l = p.left) != null) {
                if ((lr = p.left = l.right) != null) {
                    lr.parent = p;
                }
                if ((pp = l.parent = p.parent) == null) {
                    (root = l).red = false;
                } else if (pp.right == p) {
                    pp.right = l;
                } else {
                    pp.left = l;
                }
                l.right = p;
                p.parent = l;
            }
            return root;
        }

        static <V> TreeNode<V> balanceInsertion(TreeNode<V> root,
            TreeNode<V> x) {
            x.red = true;
            for (TreeNode<V> xp, xpp, xppl, xppr; ; ) {
                if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (!xp.red || (xpp = xp.parent) == null) {
                    return root;
                }
                if (xp == (xppl = xpp.left)) {
                    if ((xppr = xpp.right) != null && xppr.red) {
                        xppr.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.right) {
                            root = rotateLeft(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateRight(root, xpp);
                            }
                        }
                    }
                } else {
                    if (xppl != null && xppl.red) {
                        xppl.red = false;
                        xp.red = false;
                        xpp.red = true;
                        x = xpp;
                    } else {
                        if (x == xp.left) {
                            root = rotateRight(root, x = xp);
                            xpp = (xp = x.parent) == null ? null : xp.parent;
                        }
                        if (xp != null) {
                            xp.red = false;
                            if (xpp != null) {
                                xpp.red = true;
                                root = rotateLeft(root, xpp);
                            }
                        }
                    }
                }
            }
        }

        static <V> TreeNode<V> balanceDeletion(TreeNode<V> root,
            TreeNode<V> x) {
            for (TreeNode<V> xp, xpl, xpr; ; ) {
                if (x == null || x == root) {
                    return root;
                } else if ((xp = x.parent) == null) {
                    x.red = false;
                    return x;
                } else if (x.red) {
                    x.red = false;
                    return root;
                } else if ((xpl = xp.left) == x) {
                    if ((xpr = xp.right) != null && xpr.red) {
                        xpr.red = false;
                        xp.red = true;
                        root = rotateLeft(root, xp);
                        xpr = (xp = x.parent) == null ? null : xp.right;
                    }
                    if (xpr == null) {
                        x = xp;
                    } else {
                        TreeNode<V> sl = xpr.left, sr = xpr.right;
                        if ((sr == null || !sr.red) &&
                            (sl == null || !sl.red)) {
                            xpr.red = true;
                            x = xp;
                        } else {
                            if (sr == null || !sr.red) {
                                if (sl != null) {
                                    sl.red = false;
                                }
                                xpr.red = true;
                                root = rotateRight(root, xpr);
                                xpr = (xp = x.parent) == null ?
                                    null : xp.right;
                            }
                            if (xpr != null) {
                                xpr.red = (xp == null) ? false : xp.red;
                                if ((sr = xpr.right) != null) {
                                    sr.red = false;
                                }
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateLeft(root, xp);
                            }
                            x = root;
                        }
                    }
                } else { // symmetric
                    if (xpl != null && xpl.red) {
                        xpl.red = false;
                        xp.red = true;
                        root = rotateRight(root, xp);
                        xpl = (xp = x.parent) == null ? null : xp.left;
                    }
                    if (xpl == null) {
                        x = xp;
                    } else {
                        TreeNode<V> sl = xpl.left, sr = xpl.right;
                        if ((sl == null || !sl.red) &&
                            (sr == null || !sr.red)) {
                            xpl.red = true;
                            x = xp;
                        } else {
                            if (sl == null || !sl.red) {
                                if (sr != null) {
                                    sr.red = false;
                                }
                                xpl.red = true;
                                root = rotateLeft(root, xpl);
                                xpl = (xp = x.parent) == null ?
                                    null : xp.left;
                            }
                            if (xpl != null) {
                                xpl.red = (xp == null) ? false : xp.red;
                                if ((sl = xpl.left) != null) {
                                    sl.red = false;
                                }
                            }
                            if (xp != null) {
                                xp.red = false;
                                root = rotateRight(root, xp);
                            }
                            x = root;
                        }
                    }
                }
            }
        }

        /**
         * Recursive invariant check
         */
        static <V> boolean checkInvariants(TreeNode<V> t) {
            TreeNode<V> tp = t.parent, tl = t.left, tr = t.right,
                tb = t.prev, tn = (TreeNode<V>) t.next;
            if (tb != null && tb.next != t) {
                return false;
            }
            if (tn != null && tn.prev != t) {
                return false;
            }
            if (tp != null && t != tp.left && t != tp.right) {
                return false;
            }
            if (tl != null && (tl.parent != t || tl.hash > t.hash)) {
                return false;
            }
            if (tr != null && (tr.parent != t || tr.hash < t.hash)) {
                return false;
            }
            if (t.red && tl != null && tl.red && tr != null && tr.red) {
                return false;
            }
            if (tl != null && !checkInvariants(tl)) {
                return false;
            }
            if (tr != null && !checkInvariants(tr)) {
                return false;
            }
            return true;
        }

        private static final sun.misc.Unsafe U;
        private static final long LOCKSTATE;

        static {
            try {
                U = getUnsafe();
                Class<?> k = TreeBin.class;
                LOCKSTATE = U.objectFieldOffset
                    (k.getDeclaredField("lockState"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }
    }

    /* ----------------Table Traversal -------------- */

    /**
     * Records the table, its length, and current traversal index for a
     * traverser that must process a region of a forwarded table before
     * proceeding with current table.
     */
    static final class TableStack<V> {
        int length;
        int index;
        Node<V>[] tab;
        TableStack<V> next;
    }

    /**
     * Encapsulates traversal for methods such as containsValue; also
     * serves as a base class for other iterators and spliterators.
     * <p>
     * Method advance visits once each still-valid node that was
     * reachable upon iterator construction. It might miss some that
     * were added to a bin after the bin was visited, which is OK wrt
     * consistency guarantees. Maintaining this property in the face
     * of possible ongoing resizes requires a fair amount of
     * bookkeeping state that is difficult to optimize away amidst
     * volatile accesses.  Even so, traversal maintains reasonable
     * throughput.
     * <p>
     * Normally, iteration proceeds bin-by-bin traversing lists.
     * However, if the table has been resized, then all future steps
     * must traverse both the bin at the current index as well as at
     * (index + baseSize); and so on for further resizings. To
     * paranoically cope with potential sharing by users of iterators
     * across threads, iteration terminates if a bounds checks fails
     * for a table read.
     */
    static class Traverser<V> {
        Node<V>[] tab;        // current table; updated if resized
        Node<V> next;         // the next entry to use
        TableStack<V> stack, spare; // to save/restore on ForwardingNodes
        int index;              // index of bin to use next
        int baseIndex;          // current index of initial table
        int baseLimit;          // index bound for initial table
        final int baseSize;     // initial table size

        Traverser(Node<V>[] tab, int size, int index, int limit) {
            this.tab = tab;
            this.baseSize = size;
            this.baseIndex = this.index = index;
            this.baseLimit = limit;
            this.next = null;
        }

        /**
         * Advances if possible, returning next valid node, or null if none.
         */
        final Node<V> advance() {
            Node<V> e;
            if ((e = next) != null) {
                e = e.next;
            }
            for (; ; ) {
                Node<V>[] t;
                int i, n;  // must use locals in checks
                if (e != null) {
                    return next = e;
                }
                if (baseIndex >= baseLimit || (t = tab) == null ||
                    (n = t.length) <= (i = index) || i < 0) {
                    return next = null;
                }
                if ((e = tabAt(t, i)) != null && e.hash < 0) {
                    if (e instanceof ForwardingNode) {
                        tab = ((ForwardingNode<V>) e).nextTable;
                        e = null;
                        pushState(t, i, n);
                        continue;
                    } else if (e instanceof TreeBin) {
                        e = ((TreeBin<V>) e).first;
                    } else {
                        e = null;
                    }
                }
                if (stack != null) {
                    recoverState(n);
                } else if ((index = i + baseSize) >= n) {
                    index = ++baseIndex; // visit upper slots if present
                }
            }
        }

        /**
         * Saves traversal state upon encountering a forwarding node.
         */
        private void pushState(Node<V>[] t, int i, int n) {
            TableStack<V> s = spare;  // reuse if possible
            if (s != null) {
                spare = s.next;
            } else {
                s = new TableStack<V>();
            }
            s.tab = t;
            s.length = n;
            s.index = i;
            s.next = stack;
            stack = s;
        }

        /**
         * Possibly pops traversal state.
         *
         * @param n length of current table
         */
        private void recoverState(int n) {
            TableStack<V> s;
            int len;
            while ((s = stack) != null && (index += (len = s.length)) >= n) {
                n = len;
                index = s.index;
                tab = s.tab;
                s.tab = null;
                TableStack<V> next = s.next;
                s.next = spare; // save for reuse
                stack = next;
                spare = s;
            }
            if (s == null && (index += baseSize) >= n) {
                index = ++baseIndex;
            }
        }
    }

    /**
     * Base of key, value, and entry Iterators. Adds fields to
     * Traverser to support iterator.remove.
     */
    static class BaseIterator<V> extends Traverser<V> {
        final ByteArrayConcurrentHashMap<V> map;
        Node<V> lastReturned;

        BaseIterator(Node<V>[] tab, int size, int index, int limit,
            ByteArrayConcurrentHashMap<V> map) {
            super(tab, size, index, limit);
            this.map = map;
            advance();
        }

        public final boolean hasNext() {
            return next != null;
        }

        public final boolean hasMoreElements() {
            return next != null;
        }

        public final void remove() {
            Node<V> p;
            if ((p = lastReturned) == null) {
                throw new IllegalStateException();
            }
            lastReturned = null;
            map.replaceNode(p.key, null, null);
        }
    }

    static final class KeyIterator<V> extends BaseIterator<V> implements Iterator<byte[]>, Enumeration<byte[]> {
        KeyIterator(Node<V>[] tab, int index, int size, int limit,
            ByteArrayConcurrentHashMap<V> map) {
            super(tab, index, size, limit, map);
        }

        public final byte[] next() {
            Node<V> p;
            if ((p = next) == null) {
                throw new NoSuchElementException();
            }
            byte[] k = p.key;
            lastReturned = p;
            advance();
            return k;
        }

        public final byte[] nextElement() {
            return next();
        }
    }

    static final class ValueIterator<V> extends BaseIterator<V>
        implements Iterator<V>, Enumeration<V> {
        ValueIterator(Node<V>[] tab, int index, int size, int limit,
            ByteArrayConcurrentHashMap<V> map) {
            super(tab, index, size, limit, map);
        }

        public final V next() {
            Node<V> p;
            if ((p = next) == null) {
                throw new NoSuchElementException();
            }
            V v = p.val;
            lastReturned = p;
            advance();
            return v;
        }

        public final V nextElement() {
            return next();
        }
    }

    static final class EntryIterator<V> extends BaseIterator<V>
        implements Iterator<Map.Entry<byte[], V>> {
        EntryIterator(Node<V>[] tab, int index, int size, int limit,
            ByteArrayConcurrentHashMap<V> map) {
            super(tab, index, size, limit, map);
        }

        public final Map.Entry<byte[], V> next() {
            Node<V> p;
            if ((p = next) == null) {
                throw new NoSuchElementException();
            }
            byte[] k = p.key;
            V v = p.val;
            lastReturned = p;
            advance();
            return new MapEntry<V>(k, v, map);
        }
    }

    /**
     * Exported Entry for EntryIterator
     */
    static final class MapEntry<V> implements Map.Entry<byte[], V> {
        final byte[] key; // non-null
        V val;       // non-null
        final ByteArrayConcurrentHashMap<V> map;

        MapEntry(byte[] key, V val, ByteArrayConcurrentHashMap<V> map) {
            this.key = key;
            this.val = val;
            this.map = map;
        }

        public byte[] getKey() {
            return key;
        }

        public V getValue() {
            return val;
        }

        public int hashCode() {
            return Arrays.hashCode(key) ^ val.hashCode();
        }

        public String toString() {
            return Arrays.toString(key) + "=" + val;
        }

        public boolean equals(Object o) {
            Object k, v;
            Map.Entry<?, ?> e;
            return ((o instanceof Map.Entry) &&
                (k = (e = (Map.Entry<?, ?>) o).getKey()) != null &&
                (v = e.getValue()) != null &&
                (k == key || keyEquals(k, key)) &&
                (v == val || v.equals(val)));
        }

        /**
         * Sets our entry's value and writes through to the map. The
         * value to return is somewhat arbitrary here. Since we do not
         * necessarily track asynchronous changes, the most recent
         * "previous" value could be different from what we return (or
         * could even have been removed, in which case the put will
         * re-establish). We do not and cannot guarantee more.
         */
        public V setValue(V value) {
            if (value == null) {
                throw new NullPointerException();
            }
            V v = val;
            val = value;
            map.put(key, value);
            return v;
        }
    }

    static final class KeySpliterator<V> extends Traverser<V>
        implements Spliterator<byte[]> {
        long est;               // size estimate

        KeySpliterator(Node<V>[] tab, int size, int index, int limit,
            long est) {
            super(tab, size, index, limit);
            this.est = est;
        }

        public Spliterator<byte[]> trySplit() {
            int i, f, h;
            return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
                new KeySpliterator<V>(tab, baseSize, baseLimit = h,
                    f, est >>>= 1);
        }

        public void forEachRemaining(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            for (Node<V> p; (p = advance()) != null; )
                action.accept(p.key);
        }

        public boolean tryAdvance(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V> p;
            if ((p = advance()) == null) {
                return false;
            }
            action.accept(p.key);
            return true;
        }

        public long estimateSize() {
            return est;
        }

        public int characteristics() {
            return Spliterator.DISTINCT | Spliterator.CONCURRENT |
                Spliterator.NONNULL;
        }
    }

    static final class ValueSpliterator<V> extends Traverser<V>
        implements Spliterator<V> {
        long est;               // size estimate

        ValueSpliterator(Node<V>[] tab, int size, int index, int limit,
            long est) {
            super(tab, size, index, limit);
            this.est = est;
        }

        public Spliterator<V> trySplit() {
            int i, f, h;
            return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
                new ValueSpliterator<V>(tab, baseSize, baseLimit = h,
                    f, est >>>= 1);
        }

        public void forEachRemaining(Consumer<? super V> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            for (Node<V> p; (p = advance()) != null; )
                action.accept(p.val);
        }

        public boolean tryAdvance(Consumer<? super V> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V> p;
            if ((p = advance()) == null) {
                return false;
            }
            action.accept(p.val);
            return true;
        }

        public long estimateSize() {
            return est;
        }

        public int characteristics() {
            return Spliterator.CONCURRENT | Spliterator.NONNULL;
        }
    }

    static final class EntrySpliterator<V> extends Traverser<V>
        implements Spliterator<Map.Entry<byte[], V>> {
        final ByteArrayConcurrentHashMap<V> map; // To export MapEntry
        long est;               // size estimate

        EntrySpliterator(Node<V>[] tab, int size, int index, int limit,
            long est, ByteArrayConcurrentHashMap<V> map) {
            super(tab, size, index, limit);
            this.map = map;
            this.est = est;
        }

        public Spliterator<Map.Entry<byte[], V>> trySplit() {
            int i, f, h;
            return (h = ((i = baseIndex) + (f = baseLimit)) >>> 1) <= i ? null :
                new EntrySpliterator<V>(tab, baseSize, baseLimit = h,
                    f, est >>>= 1, map);
        }

        public void forEachRemaining(Consumer<? super Map.Entry<byte[], V>> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            for (Node<V> p; (p = advance()) != null; )
                action.accept(new MapEntry<V>(p.key, p.val, map));
        }

        public boolean tryAdvance(Consumer<? super Map.Entry<byte[], V>> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V> p;
            if ((p = advance()) == null) {
                return false;
            }
            action.accept(new MapEntry<V>(p.key, p.val, map));
            return true;
        }

        public long estimateSize() {
            return est;
        }

        public int characteristics() {
            return Spliterator.DISTINCT | Spliterator.CONCURRENT |
                Spliterator.NONNULL;
        }
    }

    // Parallel bulk operations

    /**
     * Computes initial batch value for bulk tasks. The returned value
     * is approximately exp2 of the number of times (minus one) to
     * split task by two before executing leaf action. This value is
     * faster to compute and more convenient to use as a guide to
     * splitting than is the depth, since it is used while dividing by
     * two anyway.
     */
    final int batchFor(long b) {
        long n;
        if (b == Long.MAX_VALUE || (n = sumCount()) <= 1L || n < b) {
            return 0;
        }
        int sp = ForkJoinPool.getCommonPoolParallelism() << 2; // slack of 4
        return (b <= 0L || (n /= b) >= sp) ? sp : (int) n;
    }

    /**
     * Performs the given action for each (key, value).
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param action               the action
     * @since 1.8
     */
    public void forEach(long parallelismThreshold,
        BiConsumer<? super byte[], ? super V> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        new ForEachMappingTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                action).invoke();
    }

    /**
     * Performs the given action for each non-null transformation
     * of each (key, value).
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case the action is not applied)
     * @param action               the action
     * @param <U>                  the return type of the transformer
     * @since 1.8
     */
    public <U> void forEach(long parallelismThreshold,
        BiFunction<? super byte[], ? super V, ? extends U> transformer,
        Consumer<? super U> action) {
        if (transformer == null || action == null) {
            throw new NullPointerException();
        }
        new ForEachTransformedMappingTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                transformer, action).invoke();
    }

    /**
     * Returns a non-null result from applying the given search
     * function on each (key, value), or null if none.  Upon
     * success, further element processing is suppressed and the
     * results of any other parallel invocations of the search
     * function are ignored.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param searchFunction       a function returning a non-null
     *                             result on success, else null
     * @param <U>                  the return type of the search function
     * @return a non-null result from applying the given search
     * function on each (key, value), or null if none
     * @since 1.8
     */
    public <U> U search(long parallelismThreshold,
        BiFunction<? super byte[], ? super V, ? extends U> searchFunction) {
        if (searchFunction == null) {
            throw new NullPointerException();
        }
        return new SearchMappingsTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                searchFunction, new AtomicReference<U>()).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all (key, value) pairs using the given reducer to
     * combine values, or null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case it is not combined)
     * @param reducer              a commutative associative combining function
     * @param <U>                  the return type of the transformer
     * @return the result of accumulating the given transformation
     * of all (key, value) pairs
     * @since 1.8
     */
    public <U> U reduce(long parallelismThreshold,
        BiFunction<? super byte[], ? super V, ? extends U> transformer,
        BiFunction<? super U, ? super U, ? extends U> reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceMappingsTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all (key, value) pairs using the given reducer to
     * combine values, and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all (key, value) pairs
     * @since 1.8
     */
    public double reduceToDouble(long parallelismThreshold,
        ToDoubleBiFunction<? super byte[], ? super V> transformer,
        double basis,
        DoubleBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceMappingsToDoubleTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all (key, value) pairs using the given reducer to
     * combine values, and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all (key, value) pairs
     * @since 1.8
     */
    public long reduceToLong(long parallelismThreshold,
        ToLongBiFunction<? super byte[], ? super V> transformer,
        long basis,
        LongBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceMappingsToLongTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all (key, value) pairs using the given reducer to
     * combine values, and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all (key, value) pairs
     * @since 1.8
     */
    public int reduceToInt(long parallelismThreshold,
        ToIntBiFunction<? super byte[], ? super V> transformer,
        int basis,
        IntBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceMappingsToIntTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Performs the given action for each key.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param action               the action
     * @since 1.8
     */
    public void forEachKey(long parallelismThreshold,
        Consumer<? super byte[]> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        new ForEachKeyTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                action).invoke();
    }

    /**
     * Performs the given action for each non-null transformation
     * of each key.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case the action is not applied)
     * @param action               the action
     * @param <U>                  the return type of the transformer
     * @since 1.8
     */
    public <U> void forEachKey(long parallelismThreshold,
        Function<? super byte[], ? extends U> transformer,
        Consumer<? super U> action) {
        if (transformer == null || action == null) {
            throw new NullPointerException();
        }
        new ForEachTransformedKeyTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                transformer, action).invoke();
    }

    /**
     * Returns a non-null result from applying the given search
     * function on each key, or null if none. Upon success,
     * further element processing is suppressed and the results of
     * any other parallel invocations of the search function are
     * ignored.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param searchFunction       a function returning a non-null
     *                             result on success, else null
     * @param <U>                  the return type of the search function
     * @return a non-null result from applying the given search
     * function on each key, or null if none
     * @since 1.8
     */
    public <U> U searchKeys(long parallelismThreshold,
        Function<? super byte[], ? extends U> searchFunction) {
        if (searchFunction == null) {
            throw new NullPointerException();
        }
        return new SearchKeysTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                searchFunction, new AtomicReference<U>()).invoke();
    }

    /**
     * Returns the result of accumulating all keys using the given
     * reducer to combine values, or null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating all keys using the given
     * reducer to combine values, or null if none
     * @since 1.8
     */
    public byte[] reduceKeys(long parallelismThreshold,
        BiFunction<? super byte[], ? super byte[], ? extends byte[]> reducer) {
        if (reducer == null) {
            throw new NullPointerException();
        }
        return new ReduceKeysTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all keys using the given reducer to combine values, or
     * null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case it is not combined)
     * @param reducer              a commutative associative combining function
     * @param <U>                  the return type of the transformer
     * @return the result of accumulating the given transformation
     * of all keys
     * @since 1.8
     */
    public <U> U reduceKeys(long parallelismThreshold,
        Function<? super byte[], ? extends U> transformer,
        BiFunction<? super U, ? super U, ? extends U> reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceKeysTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all keys using the given reducer to combine values, and
     * the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all keys
     * @since 1.8
     */
    public double reduceKeysToDouble(long parallelismThreshold,
        ToDoubleFunction<? super byte[]> transformer,
        double basis,
        DoubleBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceKeysToDoubleTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all keys using the given reducer to combine values, and
     * the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all keys
     * @since 1.8
     */
    public long reduceKeysToLong(long parallelismThreshold,
        ToLongFunction<? super byte[]> transformer,
        long basis,
        LongBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceKeysToLongTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all keys using the given reducer to combine values, and
     * the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all keys
     * @since 1.8
     */
    public int reduceKeysToInt(long parallelismThreshold,
        ToIntFunction<? super byte[]> transformer,
        int basis,
        IntBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceKeysToIntTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Performs the given action for each value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param action               the action
     * @since 1.8
     */
    public void forEachValue(long parallelismThreshold,
        Consumer<? super V> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        new ForEachValueTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                action).invoke();
    }

    /**
     * Performs the given action for each non-null transformation
     * of each value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case the action is not applied)
     * @param action               the action
     * @param <U>                  the return type of the transformer
     * @since 1.8
     */
    public <U> void forEachValue(long parallelismThreshold,
        Function<? super V, ? extends U> transformer,
        Consumer<? super U> action) {
        if (transformer == null || action == null) {
            throw new NullPointerException();
        }
        new ForEachTransformedValueTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                transformer, action).invoke();
    }

    /**
     * Returns a non-null result from applying the given search
     * function on each value, or null if none.  Upon success,
     * further element processing is suppressed and the results of
     * any other parallel invocations of the search function are
     * ignored.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param searchFunction       a function returning a non-null
     *                             result on success, else null
     * @param <U>                  the return type of the search function
     * @return a non-null result from applying the given search
     * function on each value, or null if none
     * @since 1.8
     */
    public <U> U searchValues(long parallelismThreshold,
        Function<? super V, ? extends U> searchFunction) {
        if (searchFunction == null) {
            throw new NullPointerException();
        }
        return new SearchValuesTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                searchFunction, new AtomicReference<U>()).invoke();
    }

    /**
     * Returns the result of accumulating all values using the
     * given reducer to combine values, or null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating all values
     * @since 1.8
     */
    public V reduceValues(long parallelismThreshold,
        BiFunction<? super V, ? super V, ? extends V> reducer) {
        if (reducer == null) {
            throw new NullPointerException();
        }
        return new ReduceValuesTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all values using the given reducer to combine values, or
     * null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case it is not combined)
     * @param reducer              a commutative associative combining function
     * @param <U>                  the return type of the transformer
     * @return the result of accumulating the given transformation
     * of all values
     * @since 1.8
     */
    public <U> U reduceValues(long parallelismThreshold,
        Function<? super V, ? extends U> transformer,
        BiFunction<? super U, ? super U, ? extends U> reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceValuesTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all values using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all values
     * @since 1.8
     */
    public double reduceValuesToDouble(long parallelismThreshold,
        ToDoubleFunction<? super V> transformer,
        double basis,
        DoubleBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceValuesToDoubleTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all values using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all values
     * @since 1.8
     */
    public long reduceValuesToLong(long parallelismThreshold,
        ToLongFunction<? super V> transformer,
        long basis,
        LongBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceValuesToLongTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all values using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all values
     * @since 1.8
     */
    public int reduceValuesToInt(long parallelismThreshold,
        ToIntFunction<? super V> transformer,
        int basis,
        IntBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceValuesToIntTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Performs the given action for each entry.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param action               the action
     * @since 1.8
     */
    public void forEachEntry(long parallelismThreshold,
        Consumer<? super Map.Entry<byte[], V>> action) {
        if (action == null) {
            throw new NullPointerException();
        }
        new ForEachEntryTask<V>(null, batchFor(parallelismThreshold), 0, 0, table,
            action).invoke();
    }

    /**
     * Performs the given action for each non-null transformation
     * of each entry.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case the action is not applied)
     * @param action               the action
     * @param <U>                  the return type of the transformer
     * @since 1.8
     */
    public <U> void forEachEntry(long parallelismThreshold,
        Function<Map.Entry<byte[], V>, ? extends U> transformer,
        Consumer<? super U> action) {
        if (transformer == null || action == null) {
            throw new NullPointerException();
        }
        new ForEachTransformedEntryTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                transformer, action).invoke();
    }

    /**
     * Returns a non-null result from applying the given search
     * function on each entry, or null if none.  Upon success,
     * further element processing is suppressed and the results of
     * any other parallel invocations of the search function are
     * ignored.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param searchFunction       a function returning a non-null
     *                             result on success, else null
     * @param <U>                  the return type of the search function
     * @return a non-null result from applying the given search
     * function on each entry, or null if none
     * @since 1.8
     */
    public <U> U searchEntries(long parallelismThreshold,
        Function<Map.Entry<byte[], V>, ? extends U> searchFunction) {
        if (searchFunction == null) {
            throw new NullPointerException();
        }
        return new SearchEntriesTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                searchFunction, new AtomicReference<U>()).invoke();
    }

    /**
     * Returns the result of accumulating all entries using the
     * given reducer to combine values, or null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating all entries
     * @since 1.8
     */
    public Map.Entry<byte[], V> reduceEntries(long parallelismThreshold,
        BiFunction<Map.Entry<byte[], V>, Map.Entry<byte[], V>, ? extends Map.Entry<byte[], V>> reducer) {
        if (reducer == null) {
            throw new NullPointerException();
        }
        return new ReduceEntriesTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all entries using the given reducer to combine values,
     * or null if none.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element, or null if there is no transformation (in
     *                             which case it is not combined)
     * @param reducer              a commutative associative combining function
     * @param <U>                  the return type of the transformer
     * @return the result of accumulating the given transformation
     * of all entries
     * @since 1.8
     */
    public <U> U reduceEntries(long parallelismThreshold,
        Function<Map.Entry<byte[], V>, ? extends U> transformer,
        BiFunction<? super U, ? super U, ? extends U> reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceEntriesTask<V, U>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all entries using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all entries
     * @since 1.8
     */
    public double reduceEntriesToDouble(long parallelismThreshold,
        ToDoubleFunction<Map.Entry<byte[], V>> transformer,
        double basis,
        DoubleBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceEntriesToDoubleTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all entries using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all entries
     * @since 1.8
     */
    public long reduceEntriesToLong(long parallelismThreshold,
        ToLongFunction<Map.Entry<byte[], V>> transformer,
        long basis,
        LongBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceEntriesToLongTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }

    /**
     * Returns the result of accumulating the given transformation
     * of all entries using the given reducer to combine values,
     * and the given basis as an identity value.
     *
     * @param parallelismThreshold the (estimated) number of elements
     *                             needed for this operation to be executed in parallel
     * @param transformer          a function returning the transformation
     *                             for an element
     * @param basis                the identity (initial default value) for the reduction
     * @param reducer              a commutative associative combining function
     * @return the result of accumulating the given transformation
     * of all entries
     * @since 1.8
     */
    public int reduceEntriesToInt(long parallelismThreshold,
        ToIntFunction<Map.Entry<byte[], V>> transformer,
        int basis,
        IntBinaryOperator reducer) {
        if (transformer == null || reducer == null) {
            throw new NullPointerException();
        }
        return new MapReduceEntriesToIntTask<V>
            (null, batchFor(parallelismThreshold), 0, 0, table,
                null, transformer, basis, reducer).invoke();
    }


    /* ----------------Views -------------- */

    /**
     * Base class for views.
     */
    abstract static class CollectionView<V, E>
        implements Collection<E>, java.io.Serializable {
        private static final long serialVersionUID = 7249069246763182397L;
        final ByteArrayConcurrentHashMap<V> map;

        CollectionView(ByteArrayConcurrentHashMap<V> map) {
            this.map = map;
        }

        /**
         * Returns the map backing this view.
         *
         * @return the map backing this view
         */
        public ByteArrayConcurrentHashMap<V> getMap() {
            return map;
        }

        /**
         * Removes all of the elements from this view, by removing all
         * the mappings from the map backing this view.
         */
        public final void clear() {
            map.clear();
        }

        public final int size() {
            return map.size();
        }

        public final boolean isEmpty() {
            return map.isEmpty();
        }

        // implementations below rely on concrete classes supplying these
        // abstract methods

        /**
         * Returns an iterator over the elements in this collection.
         * <p>
         * <p>The returned iterator is
         * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
         *
         * @return an iterator over the elements in this collection
         */
        public abstract Iterator<E> iterator();

        public abstract boolean contains(Object o);

        public abstract boolean remove(Object o);

        private static final String oomeMsg = "Required array size too large";

        public final Object[] toArray() {
            long sz = map.mappingCount();
            if (sz > MAX_ARRAY_SIZE) {
                throw new OutOfMemoryError(oomeMsg);
            }
            int n = (int) sz;
            Object[] r = new Object[n];
            int i = 0;
            for (E e : this) {
                if (i == n) {
                    if (n >= MAX_ARRAY_SIZE) {
                        throw new OutOfMemoryError(oomeMsg);
                    }
                    if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1) {
                        n = MAX_ARRAY_SIZE;
                    } else {
                        n += (n >>> 1) + 1;
                    }
                    r = Arrays.copyOf(r, n);
                }
                r[i++] = e;
            }
            return (i == n) ? r : Arrays.copyOf(r, i);
        }

        @SuppressWarnings("unchecked")
        public final <T> T[] toArray(T[] a) {
            long sz = map.mappingCount();
            if (sz > MAX_ARRAY_SIZE) {
                throw new OutOfMemoryError(oomeMsg);
            }
            int m = (int) sz;
            T[] r = (a.length >= m) ? a :
                (T[]) java.lang.reflect.Array
                    .newInstance(a.getClass().getComponentType(), m);
            int n = r.length;
            int i = 0;
            for (E e : this) {
                if (i == n) {
                    if (n >= MAX_ARRAY_SIZE) {
                        throw new OutOfMemoryError(oomeMsg);
                    }
                    if (n >= MAX_ARRAY_SIZE - (MAX_ARRAY_SIZE >>> 1) - 1) {
                        n = MAX_ARRAY_SIZE;
                    } else {
                        n += (n >>> 1) + 1;
                    }
                    r = Arrays.copyOf(r, n);
                }
                r[i++] = (T) e;
            }
            if (a == r && i < n) {
                r[i] = null; // null-terminate
                return r;
            }
            return (i == n) ? r : Arrays.copyOf(r, i);
        }

        /**
         * Returns a string representation of this collection.
         * The string representation consists of the string representations
         * of the collection's elements in the order they are returned by
         * its iterator, enclosed in square brackets ({@code "[]"}).
         * Adjacent elements are separated by the characters {@code ", "}
         * (comma and space).  Elements are converted to strings as by
         * {@link String#valueOf(Object)}.
         *
         * @return a string representation of this collection
         */
        public final String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            Iterator<E> it = iterator();
            if (it.hasNext()) {
                for (; ; ) {
                    Object e = it.next();
                    sb.append(e == this ? "(this Collection)" : e);
                    if (!it.hasNext()) {
                        break;
                    }
                    sb.append(',').append(' ');
                }
            }
            return sb.append(']').toString();
        }

        public final boolean containsAll(Collection<?> c) {
            if (c != this) {
                for (Object e : c) {
                    if (e == null || !contains(e)) {
                        return false;
                    }
                }
            }
            return true;
        }

        public final boolean removeAll(Collection<?> c) {
            if (c == null) {
                throw new NullPointerException();
            }
            boolean modified = false;
            for (Iterator<E> it = iterator(); it.hasNext(); ) {
                if (c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        public final boolean retainAll(Collection<?> c) {
            if (c == null) {
                throw new NullPointerException();
            }
            boolean modified = false;
            for (Iterator<E> it = iterator(); it.hasNext(); ) {
                if (!c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

    }

    /**
     * A view of a ByteArrayConcurrentHashMap as a {@link Set} of keys, in
     * which additions may optionally be enabled by mapping to a
     * common value.  This class cannot be directly instantiated.
     * See {@link #keySet() keySet()},
     * {@link #keySet(Object) keySet(V)},
     * {@link #newKeySet() newKeySet()},
     * {@link #newKeySet(int) newKeySet(int)}.
     *
     * @since 1.8
     */
    public static class KeySetView<V> extends CollectionView<V, byte[]> implements Set<byte[]> {

        private final V value;

        KeySetView(ByteArrayConcurrentHashMap<V> map, V value) {  // non-public
            super(map);
            this.value = value;
        }

        /**
         * Returns the default mapped value for additions,
         * or {@code null} if additions are not supported.
         *
         * @return the default mapped value for additions, or {@code null}
         * if not supported
         */
        public V getMappedValue() {
            return value;
        }

        /**
         * {@inheritDoc}
         *
         * @throws NullPointerException if the specified key is null
         */
        public boolean contains(Object o) {
            return map.containsKey(o);
        }

        /**
         * Removes the key from this map view, by removing the key (and its
         * corresponding value) from the backing map.  This method does
         * nothing if the key is not in the map.
         *
         * @param o the key to be removed from the backing map
         * @return {@code true} if the backing map contained the specified key
         * @throws NullPointerException if the specified key is null
         */
        public boolean remove(Object o) {
            return map.remove(o) != null;
        }

        /**
         * @return an iterator over the keys of the backing map
         */
        public Iterator<byte[]> iterator() {
            Node<V>[] t;
            ByteArrayConcurrentHashMap<V> m = map;
            int f = (t = m.table) == null ? 0 : t.length;
            return new KeyIterator<V>(t, f, 0, f, m);
        }

        /**
         * Adds the specified key to this set view by mapping the key to
         * the default mapped value in the backing map, if defined.
         *
         * @param e key to be added
         * @return {@code true} if this set changed as a result of the call
         * @throws NullPointerException          if the specified key is null
         * @throws UnsupportedOperationException if no default mapped value
         *                                       for additions was provided
         */
        public boolean add(byte[] e) {
            V v;
            if ((v = value) == null) {
                throw new UnsupportedOperationException();
            }
            return map.putVal(e, v, true) == null;
        }

        /**
         * Adds all of the elements in the specified collection to this set,
         * as if by calling {@link #add} on each one.
         *
         * @param c the elements to be inserted into this set
         * @return {@code true} if this set changed as a result of the call
         * @throws NullPointerException          if the collection or any of its
         *                                       elements are {@code null}
         * @throws UnsupportedOperationException if no default mapped value
         *                                       for additions was provided
         */
        public boolean addAll(Collection<? extends byte[]> c) {
            boolean added = false;
            V v;
            if ((v = value) == null) {
                throw new UnsupportedOperationException();
            }
            for (byte[] e : c) {
                if (map.putVal(e, v, true) == null) {
                    added = true;
                }
            }
            return added;
        }

        public int hashCode() {
            int h = 0;
            for (byte[] e : this)
                h += Arrays.hashCode(e);
            return h;
        }

        public boolean equals(Object o) {
            Set<?> c;
            return ((o instanceof Set) &&
                ((c = (Set<?>) o) == this ||
                    (containsAll(c) && c.containsAll(this))));
        }

        public Spliterator<byte[]> spliterator() {
            Node<V>[] t;
            ByteArrayConcurrentHashMap<V> m = map;
            long n = m.sumCount();
            int f = (t = m.table) == null ? 0 : t.length;
            return new KeySpliterator<V>(t, f, 0, f, n < 0L ? 0L : n);
        }

        public void forEach(Consumer<? super byte[]> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V>[] t;
            if ((t = map.table) != null) {
                Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
                for (Node<V> p; (p = it.advance()) != null; )
                    action.accept(p.key);
            }
        }
    }

    /**
     * A view of a ByteArrayConcurrentHashMap as a {@link Collection} of
     * values, in which additions are disabled. This class cannot be
     * directly instantiated. See {@link #values()}.
     */
    static final class ValuesView<V> extends CollectionView<V, V> implements Collection<V> {

        ValuesView(ByteArrayConcurrentHashMap<V> map) {
            super(map);
        }

        public final boolean contains(Object o) {
            return map.containsValue(o);
        }

        public final boolean remove(Object o) {
            if (o != null) {
                for (Iterator<V> it = iterator(); it.hasNext(); ) {
                    if (o.equals(it.next())) {
                        it.remove();
                        return true;
                    }
                }
            }
            return false;
        }

        public final Iterator<V> iterator() {
            ByteArrayConcurrentHashMap<V> m = map;
            Node<V>[] t;
            int f = (t = m.table) == null ? 0 : t.length;
            return new ValueIterator<V>(t, f, 0, f, m);
        }

        public final boolean add(V e) {
            throw new UnsupportedOperationException();
        }

        public final boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException();
        }

        public Spliterator<V> spliterator() {
            Node<V>[] t;
            ByteArrayConcurrentHashMap<V> m = map;
            long n = m.sumCount();
            int f = (t = m.table) == null ? 0 : t.length;
            return new ValueSpliterator<V>(t, f, 0, f, n < 0L ? 0L : n);
        }

        public void forEach(Consumer<? super V> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V>[] t;
            if ((t = map.table) != null) {
                Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
                for (Node<V> p; (p = it.advance()) != null; )
                    action.accept(p.val);
            }
        }
    }

    /**
     * A view of a ByteArrayConcurrentHashMap as a {@link Set} of (key, value)
     * entries.  This class cannot be directly instantiated. See
     * {@link #entrySet()}.
     */
    static final class EntrySetView<V> extends CollectionView<V, Map.Entry<byte[], V>>
        implements Set<Map.Entry<byte[], V>>, java.io.Serializable {
        private static final long serialVersionUID = 2249069246763182397L;

        EntrySetView(ByteArrayConcurrentHashMap<V> map) {
            super(map);
        }

        public boolean contains(Object o) {
            Object k, v, r;
            Map.Entry<?, ?> e;
            return ((o instanceof Map.Entry) &&
                (k = (e = (Map.Entry<?, ?>) o).getKey()) != null &&
                (r = map.get(k)) != null &&
                (v = e.getValue()) != null &&
                (v == r || v.equals(r)));
        }

        public boolean remove(Object o) {
            Object k, v;
            Map.Entry<?, ?> e;
            return ((o instanceof Map.Entry) &&
                (k = (e = (Map.Entry<?, ?>) o).getKey()) != null &&
                (v = e.getValue()) != null &&
                map.remove(k, v));
        }

        /**
         * @return an iterator over the entries of the backing map
         */
        public Iterator<Map.Entry<byte[], V>> iterator() {
            ByteArrayConcurrentHashMap<V> m = map;
            Node<V>[] t;
            int f = (t = m.table) == null ? 0 : t.length;
            return new EntryIterator<V>(t, f, 0, f, m);
        }

        public boolean add(Entry<byte[], V> e) {
            return map.putVal(e.getKey(), e.getValue(), false) == null;
        }

        public boolean addAll(Collection<? extends Entry<byte[], V>> c) {
            boolean added = false;
            for (Entry<byte[], V> e : c) {
                if (add(e)) {
                    added = true;
                }
            }
            return added;
        }

        public final int hashCode() {
            int h = 0;
            Node<V>[] t;
            if ((t = map.table) != null) {
                Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
                for (Node<V> p; (p = it.advance()) != null; ) {
                    h += p.hashCode();
                }
            }
            return h;
        }

        public final boolean equals(Object o) {
            Set<?> c;
            return ((o instanceof Set) &&
                ((c = (Set<?>) o) == this ||
                    (containsAll(c) && c.containsAll(this))));
        }

        public Spliterator<Map.Entry<byte[], V>> spliterator() {
            Node<V>[] t;
            ByteArrayConcurrentHashMap<V> m = map;
            long n = m.sumCount();
            int f = (t = m.table) == null ? 0 : t.length;
            return new EntrySpliterator<V>(t, f, 0, f, n < 0L ? 0L : n, m);
        }

        public void forEach(Consumer<? super Map.Entry<byte[], V>> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            Node<V>[] t;
            if ((t = map.table) != null) {
                Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
                for (Node<V> p; (p = it.advance()) != null; )
                    action.accept(new MapEntry<V>(p.key, p.val, map));
            }
        }

    }

    // -------------------------------------------------------

    /**
     * Base class for bulk tasks. Repeats some fields and code from
     * class Traverser, because we need to subclass CountedCompleter.
     */
    @SuppressWarnings("serial")
    abstract static class BulkTask<V, R> extends CountedCompleter<R> {
        Node<V>[] tab;        // same as Traverser
        Node<V> next;
        TableStack<V> stack, spare;
        int index;
        int baseIndex;
        int baseLimit;
        final int baseSize;
        int batch;              // split control

        BulkTask(BulkTask<V, ?> par, int b, int i, int f, Node<V>[] t) {
            super(par);
            this.batch = b;
            this.index = this.baseIndex = i;
            if ((this.tab = t) == null) {
                this.baseSize = this.baseLimit = 0;
            } else if (par == null) {
                this.baseSize = this.baseLimit = t.length;
            } else {
                this.baseLimit = f;
                this.baseSize = par.baseSize;
            }
        }

        /**
         * Same as Traverser version
         */
        final Node<V> advance() {
            Node<V> e;
            if ((e = next) != null) {
                e = e.next;
            }
            for (; ; ) {
                Node<V>[] t;
                int i, n;
                if (e != null) {
                    return next = e;
                }
                if (baseIndex >= baseLimit || (t = tab) == null ||
                    (n = t.length) <= (i = index) || i < 0) {
                    return next = null;
                }
                if ((e = tabAt(t, i)) != null && e.hash < 0) {
                    if (e instanceof ForwardingNode) {
                        tab = ((ForwardingNode<V>) e).nextTable;
                        e = null;
                        pushState(t, i, n);
                        continue;
                    } else if (e instanceof TreeBin) {
                        e = ((TreeBin<V>) e).first;
                    } else {
                        e = null;
                    }
                }
                if (stack != null) {
                    recoverState(n);
                } else if ((index = i + baseSize) >= n) {
                    index = ++baseIndex;
                }
            }
        }

        private void pushState(Node<V>[] t, int i, int n) {
            TableStack<V> s = spare;
            if (s != null) {
                spare = s.next;
            } else {
                s = new TableStack<V>();
            }
            s.tab = t;
            s.length = n;
            s.index = i;
            s.next = stack;
            stack = s;
        }

        private void recoverState(int n) {
            TableStack<V> s;
            int len;
            while ((s = stack) != null && (index += (len = s.length)) >= n) {
                n = len;
                index = s.index;
                tab = s.tab;
                s.tab = null;
                TableStack<V> next = s.next;
                s.next = spare; // save for reuse
                stack = next;
                spare = s;
            }
            if (s == null && (index += baseSize) >= n) {
                index = ++baseIndex;
            }
        }
    }

    /*
     * Task classes. Coded in a regular but ugly format/style to
     * simplify checks that each variant differs in the right way from
     * others. The null screenings exist because compilers cannot tell
     * that we've already null-checked task arguments, so we force
     * simplest hoisted bypass to help avoid convoluted traps.
     */
    @SuppressWarnings("serial")
    static final class ForEachKeyTask<V>
        extends BulkTask<V, Void> {
        final Consumer<? super byte[]> action;

        ForEachKeyTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Consumer<? super byte[]> action) {
            super(p, b, i, f, t);
            this.action = action;
        }

        public final void compute() {
            final Consumer<? super byte[]> action;
            if ((action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachKeyTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            action).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    action.accept(p.key);
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachValueTask<V>
        extends BulkTask<V, Void> {
        final Consumer<? super V> action;

        ForEachValueTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Consumer<? super V> action) {
            super(p, b, i, f, t);
            this.action = action;
        }

        public final void compute() {
            final Consumer<? super V> action;
            if ((action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachValueTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            action).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    action.accept(p.val);
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachEntryTask<V>
        extends BulkTask<V, Void> {
        final Consumer<? super Entry<byte[], V>> action;

        ForEachEntryTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Consumer<? super Entry<byte[], V>> action) {
            super(p, b, i, f, t);
            this.action = action;
        }

        public final void compute() {
            final Consumer<? super Entry<byte[], V>> action;
            if ((action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachEntryTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            action).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    action.accept(p);
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachMappingTask<V>
        extends BulkTask<V, Void> {
        final BiConsumer<? super byte[], ? super V> action;

        ForEachMappingTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                BiConsumer<? super byte[], ? super V> action) {
            super(p, b, i, f, t);
            this.action = action;
        }

        public final void compute() {
            final BiConsumer<? super byte[], ? super V> action;
            if ((action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachMappingTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            action).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    action.accept(p.key, p.val);
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachTransformedKeyTask<V, U>
        extends BulkTask<V, Void> {
        final Function<? super byte[], ? extends U> transformer;
        final Consumer<? super U> action;

        ForEachTransformedKeyTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<? super byte[], ? extends U> transformer, Consumer<? super U> action) {
            super(p, b, i, f, t);
            this.transformer = transformer;
            this.action = action;
        }

        public final void compute() {
            final Function<? super byte[], ? extends U> transformer;
            final Consumer<? super U> action;
            if ((transformer = this.transformer) != null &&
                (action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachTransformedKeyTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            transformer, action).fork();
                }
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.key)) != null) {
                        action.accept(u);
                    }
                }
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachTransformedValueTask<V, U>
        extends BulkTask<V, Void> {
        final Function<? super V, ? extends U> transformer;
        final Consumer<? super U> action;

        ForEachTransformedValueTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<? super V, ? extends U> transformer, Consumer<? super U> action) {
            super(p, b, i, f, t);
            this.transformer = transformer;
            this.action = action;
        }

        public final void compute() {
            final Function<? super V, ? extends U> transformer;
            final Consumer<? super U> action;
            if ((transformer = this.transformer) != null &&
                (action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachTransformedValueTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            transformer, action).fork();
                }
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.val)) != null) {
                        action.accept(u);
                    }
                }
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachTransformedEntryTask<V, U>
        extends BulkTask<V, Void> {
        final Function<Map.Entry<byte[], V>, ? extends U> transformer;
        final Consumer<? super U> action;

        ForEachTransformedEntryTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<Map.Entry<byte[], V>, ? extends U> transformer, Consumer<? super U> action) {
            super(p, b, i, f, t);
            this.transformer = transformer;
            this.action = action;
        }

        public final void compute() {
            final Function<Map.Entry<byte[], V>, ? extends U> transformer;
            final Consumer<? super U> action;
            if ((transformer = this.transformer) != null &&
                (action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachTransformedEntryTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            transformer, action).fork();
                }
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p)) != null) {
                        action.accept(u);
                    }
                }
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ForEachTransformedMappingTask<V, U>
        extends BulkTask<V, Void> {
        final BiFunction<? super byte[], ? super V, ? extends U> transformer;
        final Consumer<? super U> action;

        ForEachTransformedMappingTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                BiFunction<? super byte[], ? super V, ? extends U> transformer,
                Consumer<? super U> action) {
            super(p, b, i, f, t);
            this.transformer = transformer;
            this.action = action;
        }

        public final void compute() {
            final BiFunction<? super byte[], ? super V, ? extends U> transformer;
            final Consumer<? super U> action;
            if ((transformer = this.transformer) != null &&
                (action = this.action) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    new ForEachTransformedMappingTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            transformer, action).fork();
                }
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.key, p.val)) != null) {
                        action.accept(u);
                    }
                }
                propagateCompletion();
            }
        }
    }

    @SuppressWarnings("serial")
    static final class SearchKeysTask<V, U>
        extends BulkTask<V, U> {
        final Function<? super byte[], ? extends U> searchFunction;
        final AtomicReference<U> result;

        SearchKeysTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<? super byte[], ? extends U> searchFunction,
                AtomicReference<U> result) {
            super(p, b, i, f, t);
            this.searchFunction = searchFunction;
            this.result = result;
        }

        public final U getRawResult() {
            return result.get();
        }

        public final void compute() {
            final Function<? super byte[], ? extends U> searchFunction;
            final AtomicReference<U> result;
            if ((searchFunction = this.searchFunction) != null &&
                (result = this.result) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    if (result.get() != null) {
                        return;
                    }
                    addToPendingCount(1);
                    new SearchKeysTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            searchFunction, result).fork();
                }
                while (result.get() == null) {
                    U u;
                    Node<V> p;
                    if ((p = advance()) == null) {
                        propagateCompletion();
                        break;
                    }
                    if ((u = searchFunction.apply(p.key)) != null) {
                        if (result.compareAndSet(null, u)) {
                            quietlyCompleteRoot();
                        }
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class SearchValuesTask<V, U>
        extends BulkTask<V, U> {
        final Function<? super V, ? extends U> searchFunction;
        final AtomicReference<U> result;

        SearchValuesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<? super V, ? extends U> searchFunction,
                AtomicReference<U> result) {
            super(p, b, i, f, t);
            this.searchFunction = searchFunction;
            this.result = result;
        }

        public final U getRawResult() {
            return result.get();
        }

        public final void compute() {
            final Function<? super V, ? extends U> searchFunction;
            final AtomicReference<U> result;
            if ((searchFunction = this.searchFunction) != null &&
                (result = this.result) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    if (result.get() != null) {
                        return;
                    }
                    addToPendingCount(1);
                    new SearchValuesTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            searchFunction, result).fork();
                }
                while (result.get() == null) {
                    U u;
                    Node<V> p;
                    if ((p = advance()) == null) {
                        propagateCompletion();
                        break;
                    }
                    if ((u = searchFunction.apply(p.val)) != null) {
                        if (result.compareAndSet(null, u)) {
                            quietlyCompleteRoot();
                        }
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class SearchEntriesTask<V, U>
        extends BulkTask<V, U> {
        final Function<Entry<byte[], V>, ? extends U> searchFunction;
        final AtomicReference<U> result;

        SearchEntriesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                Function<Entry<byte[], V>, ? extends U> searchFunction,
                AtomicReference<U> result) {
            super(p, b, i, f, t);
            this.searchFunction = searchFunction;
            this.result = result;
        }

        public final U getRawResult() {
            return result.get();
        }

        public final void compute() {
            final Function<Entry<byte[], V>, ? extends U> searchFunction;
            final AtomicReference<U> result;
            if ((searchFunction = this.searchFunction) != null &&
                (result = this.result) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    if (result.get() != null) {
                        return;
                    }
                    addToPendingCount(1);
                    new SearchEntriesTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            searchFunction, result).fork();
                }
                while (result.get() == null) {
                    U u;
                    Node<V> p;
                    if ((p = advance()) == null) {
                        propagateCompletion();
                        break;
                    }
                    if ((u = searchFunction.apply(p)) != null) {
                        if (result.compareAndSet(null, u)) {
                            quietlyCompleteRoot();
                        }
                        return;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class SearchMappingsTask<V, U>
        extends BulkTask<V, U> {
        final BiFunction<? super byte[], ? super V, ? extends U> searchFunction;
        final AtomicReference<U> result;

        SearchMappingsTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                BiFunction<? super byte[], ? super V, ? extends U> searchFunction,
                AtomicReference<U> result) {
            super(p, b, i, f, t);
            this.searchFunction = searchFunction;
            this.result = result;
        }

        public final U getRawResult() {
            return result.get();
        }

        public final void compute() {
            final BiFunction<? super byte[], ? super V, ? extends U> searchFunction;
            final AtomicReference<U> result;
            if ((searchFunction = this.searchFunction) != null &&
                (result = this.result) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    if (result.get() != null) {
                        return;
                    }
                    addToPendingCount(1);
                    new SearchMappingsTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            searchFunction, result).fork();
                }
                while (result.get() == null) {
                    U u;
                    Node<V> p;
                    if ((p = advance()) == null) {
                        propagateCompletion();
                        break;
                    }
                    if ((u = searchFunction.apply(p.key, p.val)) != null) {
                        if (result.compareAndSet(null, u)) {
                            quietlyCompleteRoot();
                        }
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ReduceKeysTask<V>
        extends BulkTask<V, byte[]> {
        final BiFunction<? super byte[], ? super byte[], ? extends byte[]> reducer;
        byte[] result;
        ReduceKeysTask<V> rights, nextRight;

        ReduceKeysTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                ReduceKeysTask<V> nextRight,
                BiFunction<? super byte[], ? super byte[], ? extends byte[]> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.reducer = reducer;
        }

        public final byte[] getRawResult() {
            return result;
        }

        public final void compute() {
            final BiFunction<? super byte[], ? super byte[], ? extends byte[]> reducer;
            if ((reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new ReduceKeysTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, reducer)).fork();
                }
                byte[] r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    byte[] u = p.key;
                    r = (r == null) ? u : u == null ? r : reducer.apply(r, u);
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    ReduceKeysTask<V>
                        t = (ReduceKeysTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        byte[] tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ReduceValuesTask<V>
        extends BulkTask<V, V> {
        final BiFunction<? super V, ? super V, ? extends V> reducer;
        V result;
        ReduceValuesTask<V> rights, nextRight;

        ReduceValuesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                ReduceValuesTask<V> nextRight,
                BiFunction<? super V, ? super V, ? extends V> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.reducer = reducer;
        }

        public final V getRawResult() {
            return result;
        }

        public final void compute() {
            final BiFunction<? super V, ? super V, ? extends V> reducer;
            if ((reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new ReduceValuesTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, reducer)).fork();
                }
                V r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    V v = p.val;
                    r = (r == null) ? v : reducer.apply(r, v);
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    ReduceValuesTask<V>
                        t = (ReduceValuesTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        V tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class ReduceEntriesTask<V>
        extends BulkTask<V, Map.Entry<byte[], V>> {
        final BiFunction<Map.Entry<byte[], V>, Map.Entry<byte[], V>, ? extends Map.Entry<byte[], V>> reducer;
        Map.Entry<byte[], V> result;
        ReduceEntriesTask<V> rights, nextRight;

        ReduceEntriesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                ReduceEntriesTask<V> nextRight,
                BiFunction<Entry<byte[], V>, Map.Entry<byte[], V>, ? extends Map.Entry<byte[], V>> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.reducer = reducer;
        }

        public final Map.Entry<byte[], V> getRawResult() {
            return result;
        }

        public final void compute() {
            final BiFunction<Map.Entry<byte[], V>, Map.Entry<byte[], V>, ? extends Map.Entry<byte[], V>> reducer;
            if ((reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new ReduceEntriesTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, reducer)).fork();
                }
                Map.Entry<byte[], V> r = null;
                for (Node<V> p; (p = advance()) != null; )
                    r = (r == null) ? p : reducer.apply(r, p);
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    ReduceEntriesTask<V>
                        t = (ReduceEntriesTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        Map.Entry<byte[], V> tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceKeysTask<V, U>
        extends BulkTask<V, U> {
        final Function<? super byte[], ? extends U> transformer;
        final BiFunction<? super U, ? super U, ? extends U> reducer;
        U result;
        MapReduceKeysTask<V, U> rights, nextRight;

        MapReduceKeysTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceKeysTask<V, U> nextRight,
                Function<? super byte[], ? extends U> transformer,
                BiFunction<? super U, ? super U, ? extends U> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.reducer = reducer;
        }

        public final U getRawResult() {
            return result;
        }

        public final void compute() {
            final Function<? super byte[], ? extends U> transformer;
            final BiFunction<? super U, ? super U, ? extends U> reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceKeysTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, reducer)).fork();
                }
                U r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.key)) != null) {
                        r = (r == null) ? u : reducer.apply(r, u);
                    }
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceKeysTask<V, U>
                        t = (MapReduceKeysTask<V, U>) c,
                        s = t.rights;
                    while (s != null) {
                        U tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceValuesTask<V, U>
        extends BulkTask<V, U> {
        final Function<? super V, ? extends U> transformer;
        final BiFunction<? super U, ? super U, ? extends U> reducer;
        U result;
        MapReduceValuesTask<V, U> rights, nextRight;

        MapReduceValuesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceValuesTask<V, U> nextRight,
                Function<? super V, ? extends U> transformer,
                BiFunction<? super U, ? super U, ? extends U> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.reducer = reducer;
        }

        public final U getRawResult() {
            return result;
        }

        public final void compute() {
            final Function<? super V, ? extends U> transformer;
            final BiFunction<? super U, ? super U, ? extends U> reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceValuesTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, reducer)).fork();
                }
                U r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.val)) != null) {
                        r = (r == null) ? u : reducer.apply(r, u);
                    }
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceValuesTask<V, U>
                        t = (MapReduceValuesTask<V, U>) c,
                        s = t.rights;
                    while (s != null) {
                        U tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceEntriesTask<V, U>
        extends BulkTask<V, U> {
        final Function<Map.Entry<byte[], V>, ? extends U> transformer;
        final BiFunction<? super U, ? super U, ? extends U> reducer;
        U result;
        MapReduceEntriesTask<V, U> rights, nextRight;

        MapReduceEntriesTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceEntriesTask<V, U> nextRight,
                Function<Map.Entry<byte[], V>, ? extends U> transformer,
                BiFunction<? super U, ? super U, ? extends U> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.reducer = reducer;
        }

        public final U getRawResult() {
            return result;
        }

        public final void compute() {
            final Function<Map.Entry<byte[], V>, ? extends U> transformer;
            final BiFunction<? super U, ? super U, ? extends U> reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceEntriesTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, reducer)).fork();
                }
                U r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p)) != null) {
                        r = (r == null) ? u : reducer.apply(r, u);
                    }
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceEntriesTask<V, U>
                        t = (MapReduceEntriesTask<V, U>) c,
                        s = t.rights;
                    while (s != null) {
                        U tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceMappingsTask<V, U>
        extends BulkTask<V, U> {
        final BiFunction<? super byte[], ? super V, ? extends U> transformer;
        final BiFunction<? super U, ? super U, ? extends U> reducer;
        U result;
        MapReduceMappingsTask<V, U> rights, nextRight;

        MapReduceMappingsTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceMappingsTask<V, U> nextRight,
                BiFunction<? super byte[], ? super V, ? extends U> transformer,
                BiFunction<? super U, ? super U, ? extends U> reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.reducer = reducer;
        }

        public final U getRawResult() {
            return result;
        }

        public final void compute() {
            final BiFunction<? super byte[], ? super V, ? extends U> transformer;
            final BiFunction<? super U, ? super U, ? extends U> reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceMappingsTask<V, U>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, reducer)).fork();
                }
                U r = null;
                for (Node<V> p; (p = advance()) != null; ) {
                    U u;
                    if ((u = transformer.apply(p.key, p.val)) != null) {
                        r = (r == null) ? u : reducer.apply(r, u);
                    }
                }
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceMappingsTask<V, U>
                        t = (MapReduceMappingsTask<V, U>) c,
                        s = t.rights;
                    while (s != null) {
                        U tr, sr;
                        if ((sr = s.result) != null) {
                            t.result = (((tr = t.result) == null) ? sr :
                                reducer.apply(tr, sr));
                        }
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceKeysToDoubleTask<V>
        extends BulkTask<V, Double> {
        final ToDoubleFunction<? super byte[]> transformer;
        final DoubleBinaryOperator reducer;
        final double basis;
        double result;
        MapReduceKeysToDoubleTask<V> rights, nextRight;

        MapReduceKeysToDoubleTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceKeysToDoubleTask<V> nextRight,
                ToDoubleFunction<? super byte[]> transformer,
                double basis,
                DoubleBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Double getRawResult() {
            return result;
        }

        public final void compute() {
            final ToDoubleFunction<? super byte[]> transformer;
            final DoubleBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                double r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceKeysToDoubleTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.key));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceKeysToDoubleTask<V>
                        t = (MapReduceKeysToDoubleTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsDouble(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceValuesToDoubleTask<V>
        extends BulkTask<V, Double> {
        final ToDoubleFunction<? super V> transformer;
        final DoubleBinaryOperator reducer;
        final double basis;
        double result;
        MapReduceValuesToDoubleTask<V> rights, nextRight;

        MapReduceValuesToDoubleTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceValuesToDoubleTask<V> nextRight,
                ToDoubleFunction<? super V> transformer,
                double basis,
                DoubleBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Double getRawResult() {
            return result;
        }

        public final void compute() {
            final ToDoubleFunction<? super V> transformer;
            final DoubleBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                double r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceValuesToDoubleTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceValuesToDoubleTask<V>
                        t = (MapReduceValuesToDoubleTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsDouble(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceEntriesToDoubleTask<V>
        extends BulkTask<V, Double> {
        final ToDoubleFunction<Map.Entry<byte[], V>> transformer;
        final DoubleBinaryOperator reducer;
        final double basis;
        double result;
        MapReduceEntriesToDoubleTask<V> rights, nextRight;

        MapReduceEntriesToDoubleTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceEntriesToDoubleTask<V> nextRight,
                ToDoubleFunction<Map.Entry<byte[], V>> transformer,
                double basis,
                DoubleBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Double getRawResult() {
            return result;
        }

        public final void compute() {
            final ToDoubleFunction<Map.Entry<byte[], V>> transformer;
            final DoubleBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                double r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceEntriesToDoubleTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsDouble(r, transformer.applyAsDouble(p));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceEntriesToDoubleTask<V>
                        t = (MapReduceEntriesToDoubleTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsDouble(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceMappingsToDoubleTask<V>
        extends BulkTask<V, Double> {
        final ToDoubleBiFunction<? super byte[], ? super V> transformer;
        final DoubleBinaryOperator reducer;
        final double basis;
        double result;
        MapReduceMappingsToDoubleTask<V> rights, nextRight;

        MapReduceMappingsToDoubleTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceMappingsToDoubleTask<V> nextRight,
                ToDoubleBiFunction<? super byte[], ? super V> transformer,
                double basis,
                DoubleBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Double getRawResult() {
            return result;
        }

        public final void compute() {
            final ToDoubleBiFunction<? super byte[], ? super V> transformer;
            final DoubleBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                double r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceMappingsToDoubleTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsDouble(r, transformer.applyAsDouble(p.key, p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceMappingsToDoubleTask<V>
                        t = (MapReduceMappingsToDoubleTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsDouble(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceKeysToLongTask<V>
        extends BulkTask<V, Long> {
        final ToLongFunction<? super byte[]> transformer;
        final LongBinaryOperator reducer;
        final long basis;
        long result;
        MapReduceKeysToLongTask<V> rights, nextRight;

        MapReduceKeysToLongTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceKeysToLongTask<V> nextRight,
                ToLongFunction<? super byte[]> transformer,
                long basis,
                LongBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Long getRawResult() {
            return result;
        }

        public final void compute() {
            final ToLongFunction<? super byte[]> transformer;
            final LongBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                long r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceKeysToLongTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsLong(r, transformer.applyAsLong(p.key));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceKeysToLongTask<V>
                        t = (MapReduceKeysToLongTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsLong(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceValuesToLongTask<V>
        extends BulkTask<V, Long> {
        final ToLongFunction<? super V> transformer;
        final LongBinaryOperator reducer;
        final long basis;
        long result;
        MapReduceValuesToLongTask<V> rights, nextRight;

        MapReduceValuesToLongTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceValuesToLongTask<V> nextRight,
                ToLongFunction<? super V> transformer,
                long basis,
                LongBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Long getRawResult() {
            return result;
        }

        public final void compute() {
            final ToLongFunction<? super V> transformer;
            final LongBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                long r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceValuesToLongTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsLong(r, transformer.applyAsLong(p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceValuesToLongTask<V>
                        t = (MapReduceValuesToLongTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsLong(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceEntriesToLongTask<V>
        extends BulkTask<V, Long> {
        final ToLongFunction<Map.Entry<byte[], V>> transformer;
        final LongBinaryOperator reducer;
        final long basis;
        long result;
        MapReduceEntriesToLongTask<V> rights, nextRight;

        MapReduceEntriesToLongTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceEntriesToLongTask<V> nextRight,
                ToLongFunction<Map.Entry<byte[], V>> transformer,
                long basis,
                LongBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Long getRawResult() {
            return result;
        }

        public final void compute() {
            final ToLongFunction<Map.Entry<byte[], V>> transformer;
            final LongBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                long r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceEntriesToLongTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsLong(r, transformer.applyAsLong(p));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceEntriesToLongTask<V>
                        t = (MapReduceEntriesToLongTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsLong(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceMappingsToLongTask<V>
        extends BulkTask<V, Long> {
        final ToLongBiFunction<? super byte[], ? super V> transformer;
        final LongBinaryOperator reducer;
        final long basis;
        long result;
        MapReduceMappingsToLongTask<V> rights, nextRight;

        MapReduceMappingsToLongTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceMappingsToLongTask<V> nextRight,
                ToLongBiFunction<? super byte[], ? super V> transformer,
                long basis,
                LongBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Long getRawResult() {
            return result;
        }

        public final void compute() {
            final ToLongBiFunction<? super byte[], ? super V> transformer;
            final LongBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                long r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceMappingsToLongTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsLong(r, transformer.applyAsLong(p.key, p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceMappingsToLongTask<V>
                        t = (MapReduceMappingsToLongTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsLong(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceKeysToIntTask<V>
        extends BulkTask<V, Integer> {
        final ToIntFunction<? super byte[]> transformer;
        final IntBinaryOperator reducer;
        final int basis;
        int result;
        MapReduceKeysToIntTask<V> rights, nextRight;

        MapReduceKeysToIntTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceKeysToIntTask<V> nextRight,
                ToIntFunction<? super byte[]> transformer,
                int basis,
                IntBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Integer getRawResult() {
            return result;
        }

        public final void compute() {
            final ToIntFunction<? super byte[]> transformer;
            final IntBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                int r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceKeysToIntTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsInt(r, transformer.applyAsInt(p.key));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceKeysToIntTask<V>
                        t = (MapReduceKeysToIntTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsInt(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceValuesToIntTask<V>
        extends BulkTask<V, Integer> {
        final ToIntFunction<? super V> transformer;
        final IntBinaryOperator reducer;
        final int basis;
        int result;
        MapReduceValuesToIntTask<V> rights, nextRight;

        MapReduceValuesToIntTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceValuesToIntTask<V> nextRight,
                ToIntFunction<? super V> transformer,
                int basis,
                IntBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Integer getRawResult() {
            return result;
        }

        public final void compute() {
            final ToIntFunction<? super V> transformer;
            final IntBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                int r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceValuesToIntTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsInt(r, transformer.applyAsInt(p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceValuesToIntTask<V>
                        t = (MapReduceValuesToIntTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsInt(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceEntriesToIntTask<V>
        extends BulkTask<V, Integer> {
        final ToIntFunction<Map.Entry<byte[], V>> transformer;
        final IntBinaryOperator reducer;
        final int basis;
        int result;
        MapReduceEntriesToIntTask<V> rights, nextRight;

        MapReduceEntriesToIntTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceEntriesToIntTask<V> nextRight,
                ToIntFunction<Map.Entry<byte[], V>> transformer,
                int basis,
                IntBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Integer getRawResult() {
            return result;
        }

        public final void compute() {
            final ToIntFunction<Map.Entry<byte[], V>> transformer;
            final IntBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                int r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceEntriesToIntTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsInt(r, transformer.applyAsInt(p));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceEntriesToIntTask<V>
                        t = (MapReduceEntriesToIntTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsInt(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    @SuppressWarnings("serial")
    static final class MapReduceMappingsToIntTask<V>
        extends BulkTask<V, Integer> {
        final ToIntBiFunction<? super byte[], ? super V> transformer;
        final IntBinaryOperator reducer;
        final int basis;
        int result;
        MapReduceMappingsToIntTask<V> rights, nextRight;

        MapReduceMappingsToIntTask
            (BulkTask<V, ?> p, int b, int i, int f, Node<V>[] t,
                MapReduceMappingsToIntTask<V> nextRight,
                ToIntBiFunction<? super byte[], ? super V> transformer,
                int basis,
                IntBinaryOperator reducer) {
            super(p, b, i, f, t);
            this.nextRight = nextRight;
            this.transformer = transformer;
            this.basis = basis;
            this.reducer = reducer;
        }

        public final Integer getRawResult() {
            return result;
        }

        public final void compute() {
            final ToIntBiFunction<? super byte[], ? super V> transformer;
            final IntBinaryOperator reducer;
            if ((transformer = this.transformer) != null &&
                (reducer = this.reducer) != null) {
                int r = this.basis;
                for (int i = baseIndex, f, h; batch > 0 &&
                    (h = ((f = baseLimit) + i) >>> 1) > i; ) {
                    addToPendingCount(1);
                    (rights = new MapReduceMappingsToIntTask<V>
                        (this, batch >>>= 1, baseLimit = h, f, tab,
                            rights, transformer, r, reducer)).fork();
                }
                for (Node<V> p; (p = advance()) != null; )
                    r = reducer.applyAsInt(r, transformer.applyAsInt(p.key, p.val));
                result = r;
                CountedCompleter<?> c;
                for (c = firstComplete(); c != null; c = c.nextComplete()) {
                    @SuppressWarnings("unchecked")
                    MapReduceMappingsToIntTask<V>
                        t = (MapReduceMappingsToIntTask<V>) c,
                        s = t.rights;
                    while (s != null) {
                        t.result = reducer.applyAsInt(t.result, s.result);
                        s = t.rights = s.nextRight;
                    }
                }
            }
        }
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe U;
    private static final long SIZECTL;
    private static final long TRANSFERINDEX;
    private static final long BASECOUNT;
    private static final long CELLSBUSY;
    private static final long CELLVALUE;
    private static final long ABASE;
    private static final int ASHIFT;

    private static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            Unsafe unsafe = (Unsafe) f.get(null);
            f.setAccessible(false);
            return unsafe;
        } catch (Exception e) {
            System.out.println("Unsafe object is unavailable");
            System.exit(1);
            return null;
        }
    }

    static {
        try {
            U = getUnsafe();
            Class<?> k = ByteArrayConcurrentHashMap.class;
            SIZECTL = U.objectFieldOffset
                (k.getDeclaredField("sizeCtl"));
            TRANSFERINDEX = U.objectFieldOffset
                (k.getDeclaredField("transferIndex"));
            BASECOUNT = U.objectFieldOffset
                (k.getDeclaredField("baseCount"));
            CELLSBUSY = U.objectFieldOffset
                (k.getDeclaredField("cellsBusy"));
            Class<?> ck = CounterCell.class;
            CELLVALUE = U.objectFieldOffset
                (ck.getDeclaredField("value"));
            Class<?> ak = Node[].class;
            ABASE = U.arrayBaseOffset(ak);
            int scale = U.arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0) {
                throw new Error("data type scale not a power of two");
            }
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }
    }
}
