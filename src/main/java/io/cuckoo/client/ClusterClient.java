package io.cuckoo.client;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Client;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author TommyYang on 2018/12/17
 */
public class ClusterClient extends JedisCluster {

    public ClusterClient(HostAndPort node) {
        super(node);
    }

    public ClusterClient(HostAndPort node, int timeout) {
        super(node, timeout);
    }

    public ClusterClient(HostAndPort node, int timeout, int maxAttempts) {
        super(node, timeout, maxAttempts);
    }

    public ClusterClient(HostAndPort node, GenericObjectPoolConfig poolConfig) {
        super(node, poolConfig);
    }

    public ClusterClient(HostAndPort node, int timeout, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, poolConfig);
    }

    public ClusterClient(HostAndPort node, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, timeout, maxAttempts, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ClusterClient(HostAndPort node, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(node, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> nodes) {
        super(nodes);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout) {
        super(nodes, timeout);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout, int maxAttempts) {
        super(nodes, timeout, maxAttempts);
    }

    public ClusterClient(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig) {
        super(nodes, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> nodes, int timeout, GenericObjectPoolConfig poolConfig) {
        super(nodes, timeout, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int timeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, timeout, maxAttempts, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    public ClusterClient(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, String clientName, GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode, connectionTimeout, soTimeout, maxAttempts, password, clientName, poolConfig);
    }


    private void sendCommand(Connection conn, String key, ProtocolCommand command, String ...args) {
        String[] fullArgs = new String[args.length + 1];
        fullArgs[0] = key;
        System.arraycopy(args, 0, fullArgs, 1, args.length);
        conn.sendCommand(command, fullArgs);
    }

    /**
     * Reserve a bloom filter.
     * @param name The key of the filter
     * @param initCapacity Optimize for this many items
     * @param errorRate The desired rate of false positives
     *
     * @return true if the filter create success, false if the filter create error.
     *
     * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
     * is called.
     */
    public boolean createFilter(String name, long initCapacity, double errorRate) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.INIT, name, errorRate + "", initCapacity + "");
                return conn.getStatusCodeReply().equals("OK");
            }
        }).run(name);
    }

    /**
     * Adds an item to the filter
     * @param name The name of the filter
     * @param value The value to add to the filter
     * @return true if the item was not previously in the filter.
     */
    public boolean add(String name, String value) {
      return add(name, SafeEncoder.encode(value));
    }

    /**
     * Like {@link #add(String, String)}, but allows you to store non-string items
     * @param name Name of the filter
     * @param value Value to add to the filter
     * @return true if the item was not previously in the filter
     */
    public boolean add(String name, byte[] value) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.ADD, name.getBytes(), value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }


    /**
     * Check if an item exists in the filter
     * @param name Name (key) of the filter
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter
     */
    public boolean exists(String name, String value) {
        return exists(name, SafeEncoder.encode(value));
    }

    /**
     * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
     * @param name Key of the filter to check
     * @param value Value to check for
     * @return true if the item may exist in the filter, false if the item does not exist in the filter.
     */
    public boolean exists(String name, byte[] value) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                conn.sendCommand(Command.EXISTS, name.getBytes(), value);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }


    /**
     * Remove the filter
     * @param name
     * @return true if delete the filter, false is not delete the filter
     */
    public boolean delete(String name) {
        return (new JedisClusterCommand<Boolean>(this.connectionHandler, this.maxAttempts) {
            public Boolean execute(Jedis connection) {
                Connection conn = connection.getClient();
                ((Client) conn).del(name);
                return conn.getIntegerReply() != 0;
            }
        }).run(name);
    }

    @SafeVarargs
    private final <T> boolean[] sendMultiCommand(Connection conn, Command cmd, T name, T... value) {
        ArrayList<T> arr = new ArrayList<>();
        arr.add(name);
        arr.addAll(Arrays.asList(value));
        List<Long> reps;
        if (name instanceof String) {
            conn.sendCommand(cmd, arr.toArray((String[]) value));
            reps = conn.getIntegerMultiBulkReply();
        } else {
            conn.sendCommand(cmd, arr.toArray((byte[][]) value));
            reps = conn.getIntegerMultiBulkReply();
        }
        boolean[] ret = new boolean[value.length];
        for (int i = 0; i < reps.size(); i++) {
            ret[i] = reps.get(i) != 0;
        }

        return ret;
    }

}
