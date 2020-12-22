package com.redislabs.cuckoo.filter;

import com.redislabs.bloom.utils.InsertOptions;
import com.redislabs.bloom.utils.Keywords;
import com.redislabs.client.base.Client;
import com.redislabs.cuckoo.utils.Command;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BloomFilter is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class CuckooFilter extends Client {


    /**
     * Create a new client to ReBloom
     *
     * @param pool Jedis connection pool to be used
     */
    public CuckooFilter(Pool<Jedis> pool) {
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
    public CuckooFilter(String host, int port, int timeout, int poolSize, String password) {
        super(host, port, timeout, poolSize, password);
    }

    public CuckooFilter(String host, int port, String password) {
        this(host, port, 500, 100, password);
    }

    /**
     * Create a new client to ReBloom
     *
     * @param host the redis host
     * @param port the redis port
     */
    public CuckooFilter(String host, int port) {
        this(host, port, 500, 100, null);
    }

    /**
     * Reserve a bloom filter.
     *
     * @param name         The key of the filter
     * @param initCapacity Optimize for this many items
     *                     <p>
     *                     Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     *                     is called.
     */
    public void createFilter(String name, long initCapacity) {
        try (Jedis conn = _conn()) {
            String rep = sendCommand(conn, Command.RESERVE, SafeEncoder.encode(name), Protocol.toByteArray(initCapacity)).getStatusCodeReply();
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
     * Adds an item to the filter
     *
     * @param name  The name of the filter
     * @param value The value to add to the filter
     * @return true if the item was not previously in the filter.
     */
    public boolean addNX(String name, String value) {
        return addNX(name, SafeEncoder.encode(value));
    }

    /**
     * Like {@link #add(String, String)}, but allows you to store non-string items
     *
     * @param name  Name of the filter
     * @param value Value to add to the filter
     * @return true if the item was not previously in the filter
     */
    public boolean addNX(String name, byte[] value) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, Command.ADDNX, SafeEncoder.encode(name), value).getIntegerReply() != 0;
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
     * Check if an item exists in the filter
     *
     * @param name  Name (key) of the filter
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter
     */
    public boolean del(String name, String value) {
        return exists(name, SafeEncoder.encode(value));
    }

    /**
     * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
     *
     * @param name  Key of the filter to check
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter.
     */
    public boolean del(String name, byte[] value) {
        try (Jedis conn = _conn()) {
            return sendCommand(conn, Command.DEL, SafeEncoder.encode(name), value).getIntegerReply() != 0;
        }
    }

    /**
     * Remove the filter
     *
     * @param name
     * @return true if delete the filter, false is not delete the filter
     */
    public boolean delete(String name) {
        try (Jedis conn = _conn()) {
            return conn.del(name) != 0;
        }
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


}
