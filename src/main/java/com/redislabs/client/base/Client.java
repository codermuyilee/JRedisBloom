package com.redislabs.client.base;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;

/**
 * BloomFilter is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class Client implements Closeable {
    private final Pool<Jedis> pool;

    /**
     * Create a new client to ReBloom
     *
     * @param pool Jedis connection pool to be used
     */
    public Client(Pool<Jedis> pool) {
        this.pool = pool;
    }


    /**
     * Create a new client to ReBloom
     *
     * @param host     the redis host
     * @param port     the redis port
     * @param timeout  connection timeout
     * @param poolSize the poolSize of JedisPool
     */
    public Client(String host, int port, int timeout, int poolSize, String password) {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(poolSize);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);
        conf.setFairness(true);

        pool = new JedisPool(conf, host, port, timeout, password);
    }

    public Client(String host, int port, String password) {
        this(host, port, 500, 100, password);
    }

    /**
     * Create a new client to ReBloom
     *
     * @param host the redis host
     * @param port the redis port
     */
    public Client(String host, int port) {
        this(host, port, 500, 100, null);
    }


    @Override
    public void close() {
        this.pool.close();
    }

    public Jedis _conn() {
        return pool.getResource();
    }

    public Connection sendCommand(Jedis conn, String key, ProtocolCommand command, String... args) {
        byte[][] fullArgs = new byte[args.length + 1][];
        fullArgs[0] = SafeEncoder.encode(key);
        System.arraycopy(SafeEncoder.encodeMany(args), 0, fullArgs, 1, args.length);
        return sendCommand(conn, command, fullArgs);
    }

    public Connection sendCommand(Jedis conn, ProtocolCommand command, byte[]... args) {
        Connection client = conn.getClient();
        client.sendCommand(command, args);
        return client;
    }
}
