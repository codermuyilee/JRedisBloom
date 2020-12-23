package com.redislabs.bloom.filter;

import com.redislabs.bloom.utils.Command;
import com.redislabs.bloom.utils.InsertOptions;
import com.redislabs.bloom.utils.Keywords;
import com.redislabs.bloom.utils.TopKCommand;
import com.redislabs.client.base.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * BloomFilter is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class BloomFilter extends Client {


    /**
     * Create a new client to ReBloom
     *
     * @param pool Jedis connection pool to be used
     */
    public BloomFilter(Pool<Jedis> pool) {
        super(pool);
    }


    /**
     * Create a new client to ReBloom
     *
     * @param host     the redis host
     * @param port     the redis port
     * @param timeout  connection timeout
     * @param poolSize the poolSize of JedisPool
     */
    public BloomFilter(String host, int port, int timeout, int poolSize, String password) {
        super(host, port, timeout, poolSize, password);
    }

    public BloomFilter(String host, int port, String password) {
        this(host, port, 500, 100, password);
    }

    /**
     * Create a new client to ReBloom
     *
     * @param host the redis host
     * @param port the redis port
     */
    public BloomFilter(String host, int port) {
        this(host, port, 500, 100, null);
    }

    /**
     * Reserve a bloom filter.
     *
     * @param name         The key of the filter
     * @param initCapacity Optimize for this many items
     * @param errorRate    The desired rate of false positives
     *                     <p>
     *                     Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     *                     is called.
     */
    public void createFilter(String name, long initCapacity, double errorRate) {
        try (Jedis conn = _conn()) {
            String rep = sendCommand(conn, Command.RESERVE, SafeEncoder.encode(name), Protocol.toByteArray(errorRate), Protocol.toByteArray(initCapacity)).getStatusCodeReply();
            if (!rep.equals("OK")) {
                throw new JedisException(rep);
            }
        }
    }

    /**
     * Adds an item to the filter
     *
     * @param name  The name of the filter
     * @param value The value to add to the filter
     * @return true if the item was not previously in the filter.
     */
    public boolean add(String name, String value) {
        return add(name, SafeEncoder.encode(value));
    }

    /**
     * Like {@link #add(String, String)}, but allows you to store non-string items
     *
     * @param name  Name of the filter
     * @param value Value to add to the filter
     * @return true if the item was not previously in the filter
     */
    public boolean add(String name, byte[] value) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, Command.ADD, SafeEncoder.encode(name), value).getIntegerReply() != 0;
        }
    }

    /**
     * add one or more items to the bloom filter, by default creating it if it does not yet exist
     *
     * @param name    The name of the filter
     * @param options {@link InsertOptions}
     * @param items   items to add to the filter
     * @return
     */
    public boolean[] insert(String name, InsertOptions options, String... items) {
        final List<byte[]> args = new ArrayList<>();
        args.addAll(options.getOptions());
        args.add(Keywords.ITEMS.getRaw());
        for (String item : items) {
            args.add(SafeEncoder.encode(item));
        }
        return sendMultiCommand(Command.INSERT, SafeEncoder.encode(name), args.toArray(new byte[args.size()][]));
    }

    @SafeVarargs
    private final boolean[] sendMultiCommand(Command cmd, byte[] name, byte[]... values) {
        byte[][] args = new byte[values.length + 1][];
        args[0] = name;
        System.arraycopy(values, 0, args, 1, values.length);
        List<Long> reps;
        try (Jedis conn = _conn()) {
            reps = sendCommand(conn, cmd, args).getIntegerMultiBulkReply();
        }
        boolean[] ret = new boolean[values.length];
        for (int i = 0; i < reps.size(); i++) {
            ret[i] = reps.get(i) != 0;
        }

        return ret;
    }


    /**
     * Add one or more items to a filter
     *
     * @param name   Name of the filter
     * @param values values to add to the filter.
     * @return An array of booleans of the same length as the number of values.
     * Each boolean values indicates whether the corresponding element was previously in the
     * filter or not. A true value means the item did not previously exist, whereas a
     * false value means it may have previously existed.
     * @see #add(String, byte[])
     */
    public boolean[] addMulti(String name, byte[]... values) {
        return sendMultiCommand(Command.MADD, SafeEncoder.encode(name), values);
    }

    /**
     * Add one or more items to a filter
     *
     * @param name   Name of the filter
     * @param values values to add to the filter.
     * @return An array of booleans of the same length as the number of values.
     * Each boolean values indicates whether the corresponding element was previously in the
     * filter or not. A true value means the item did not previously exist, whereas a
     * false value means it may have previously existed.
     * @see #add(String, String)
     */
    public boolean[] addMulti(String name, String... values) {
        return addMulti(name, SafeEncoder.encodeMany(values));
    }

    /**
     * Check if an item exists in the filter
     *
     * @param name  Name (key) of the filter
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter
     */
    public boolean exists(String name, String value) {
        return exists(name, SafeEncoder.encode(value));
    }

    /**
     * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
     *
     * @param name  Key of the filter to check
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter.
     */
    public boolean exists(String name, byte[] value) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, Command.EXISTS, SafeEncoder.encode(name), value).getIntegerReply() != 0;
        }
    }

    /**
     * Check if one or more items exist in the filter
     *
     * @param name   Name of the filter to check
     * @param values values to check for
     * @return An array of booleans. A true value means the corresponding value may exist, false means it does not exist
     */
    public boolean[] existsMulti(String name, byte[]... values) {
        return sendMultiCommand(Command.MEXISTS, SafeEncoder.encode(name), values);
    }

    public boolean[] existsMulti(String name, String... values) {
        return sendMultiCommand(Command.MEXISTS, SafeEncoder.encode(name), SafeEncoder.encodeMany(values));
    }

    /**
     * Get information about the filter
     *
     * @param name
     * @return Return information
     */
    public Map<String, Object> info(String name) {
        try (Jedis conn = _conn()) {
            List<Object> values = sendCommand(conn, Command.INFO, SafeEncoder.encode(name)).getObjectMultiBulkReply();

            Map<String, Object> infoMap = new HashMap<>(values.size() / 2);
            for (int i = 0; i < values.size(); i += 2) {
                Object val = values.get(i + 1);
                if (val instanceof byte[]) {
                    val = SafeEncoder.encode((byte[]) val);
                }
                infoMap.put(SafeEncoder.encode((byte[]) values.get(i)), val);
            }
            return infoMap;
        }
    }

    /**
     * TOPK.INIT key topk width depth decay
     * <p>
     * Reserve a topk filter.
     *
     * @param key   The key of the filter
     * @param topk
     * @param width
     * @param depth
     * @param decay Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     *              is called.
     */
    public void topkCreateFilter(String key, long topk, long width, long depth, double decay) {
        try (Jedis conn = _conn()) {
            String rep = sendCommand(conn, TopKCommand.RESERVE, SafeEncoder.encode(key), Protocol.toByteArray(topk),
                    Protocol.toByteArray(width), Protocol.toByteArray(depth), Protocol.toByteArray(decay))
                    .getStatusCodeReply();

            if (!rep.equals("OK")) {
                throw new JedisException(rep);
            }
        }
    }

    /**
     * TOPK.ADD key item [item ...]
     * <p>
     * Adds an item to the filter
     *
     * @param key   The key of the filter
     * @param items The items to add to the filter
     * @return list of items dropped from the list.
     */
    public List<String> topkAdd(String key, String... items) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, key, TopKCommand.ADD, items).getMultiBulkReply();
        }
    }

    /**
     * TOPK.INCRBY key item increment [item increment ...]
     * <p>
     * Adds an item to the filter
     *
     * @param key  The key of the filter
     * @param item The item to increment
     * @return item dropped from the list.
     */
    public String topkIncrBy(String key, String item, long increment) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, TopKCommand.INCRBY, SafeEncoder.encode(key), SafeEncoder.encode(item), Protocol.toByteArray(increment))
                    .getMultiBulkReply().get(0);
        }
    }

    /**
     * TOPK.QUERY key item [item ...]
     * <p>
     * Checks whether an item is one of Top-K items.
     *
     * @param key   The key of the filter
     * @param items The items to check in the list
     * @return list of indicator for each item requested
     */
    public List<Boolean> topkQuery(String key, String... items) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, key, TopKCommand.QUERY, items)
                    .getIntegerMultiBulkReply()
                    .stream().map(s -> s != 0)
                    .collect(Collectors.toList());
        }
    }

    /**
     * TOPK.COUNT key item [item ...]
     * <p>
     * Returns count for an item.
     *
     * @param key   The key of the filter
     * @param items The items to check in the list
     * @return list of counters per item.
     */
    public List<Long> topkCount(String key, String... items) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, key, TopKCommand.COUNT, items)
                    .getIntegerMultiBulkReply();
        }
    }

    /**
     * TOPK.LIST key
     * <p>
     * Return full list of items in Top K list.
     *
     * @param key The key of the filter
     * @return list of items in the list.
     */
    public List<String> topkList(String key) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, TopKCommand.LIST, SafeEncoder.encode(key))
                    .getMultiBulkReply();
        }
    }


}
