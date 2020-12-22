package io.cuckoo.client;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Client is the main ReBloom client class, wrapping connection management and all ReBloom commands
 */
public class Client implements Closeable {



  private final Pool<Jedis> pool;
  
  /**
   * Create a new client to ReBloom
   * @param pool Jedis connection pool to be used 
   */
  public Client(Pool<Jedis> pool){
    this.pool = pool;
  }
 
  
  /**
   * Create a new client to ReBloom
   * @param host the redis host
   * @param port the redis port
   * @param timeout connection timeout
   * @param poolSize the poolSize of JedisPool
   */
  public Client(String host, int port, int timeout, int poolSize,String password) {
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

  public Client(String host, int port,String password) {
    this(host, port, 30000, 100,password);
  }

  /**
   * Create a new client to ReBloom
   * @param host the redis host
   * @param port the redis port
   */
  public Client(String host, int port) {
    this(host, port, 30000, 100,null);
  }

  /**
   * Reserve a bloom filter.
   * @param name The key of the filter
   * @param initCapacity Optimize for this many items
   * @param errorRate The desired rate of false positives
   *
   * Note that if a filter is not reserved, a new one is created when {@link #add(String, byte[])}
   * is called.
   */
  public void createFilter(String name, String initCapacity, int errorRate) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, Command.INIT, SafeEncoder.encode(name),initCapacity.getBytes(), Protocol.toByteArray(errorRate)).getStatusCodeReply();
      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }
  }

  /**
   * Adds an item to the filter
   * @param name The name of the filter
   * @param value The value to add to the filter
   * @return true if the item was not previously in the filter.
   */
  public void add(String name, String value) {
     add(name, value.hashCode(),value.charAt(0));
  }

  /**
   * Like {@link #add(String, String)}, but allows you to store non-string items
   * @param name Name of the filter
   * @param value Value to add to the filter
   * @return true if the item was not previously in the filter
   */
  private void add(String name, int value,int fp) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, Command.ADD, SafeEncoder.encode(name), Protocol.toByteArray(value),Protocol.toByteArray(fp)).getStatusCodeReply();
      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }

    }



  }

//
//  private void add(String name, int value,int fp) {
//    try (Jedis conn = _conn()) {
//      String rep = sendCommand(conn, Command.ADD, SafeEncoder.encode(name), Protocol.toByteArray(value),Protocol.toByteArray(fp)).getStatusCodeReply();
//      if (!rep.equals("OK")) {
//        throw new JedisException(rep);
//      }
//
//    }
//  }




  /**
   * Check if an item exists in the filter
   * @param name Name (key) of the filter
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter
   */
  public boolean exists(String name, String value) {
    return exists(name, value.hashCode(),value.charAt(0));
  }

  /**
   * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
   * @param name Key of the filter to check
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter.
   */
  private boolean exists(String name, int value,int fp) {
    try (Jedis conn = _conn()) {
      return "1".equals(sendCommand(conn, Command.EXISTS, SafeEncoder.encode(name), Protocol.toByteArray(value), Protocol.toByteArray(fp)).getStatusCodeReply());
    }
  }


  /**
   * Remove the filter
   * @param name
   * @return true if delete the filter, false is not delete the filter
   */
  public boolean delete(String name) {
      try(Jedis conn = _conn()){
          return conn.del(name) != 0;
      }
  }


  /**
   * Check if an item exists in the filter
   * @param name Name (key) of the filter
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter
   */
  public void remove(String name, String value) {
     remove(name, value.hashCode(),value.charAt(0));
  }

  /**
   * Check if an item exists in the filter. Similar to {@link #exists(String, String)}
   * @param name Key of the filter to check
   * @param value Value to check for
   * @return true if the item may exist in the filter, false if the item does not exist in the filter.
   */
  private void remove(String name, int value,int fp) {
    try (Jedis conn = _conn()) {
      String rep = sendCommand(conn, Command.REMOVE, SafeEncoder.encode(name), Protocol.toByteArray(value),Protocol.toByteArray(fp)).getStatusCodeReply();
      if (!rep.equals("OK")) {
        throw new JedisException(rep);
      }
    }


  }

  @Override
  public void close(){
    this.pool.close();
  }

  Jedis _conn() {
    return pool.getResource();
  }

  private Connection sendCommand(Jedis conn, String key, ProtocolCommand command, String ...args) {
    byte[][] fullArgs = new byte[args.length + 1][];
    fullArgs[0] = SafeEncoder.encode(key);
    System.arraycopy( SafeEncoder.encodeMany(args), 0, fullArgs, 1, args.length);
    return sendCommand(conn, command, fullArgs);
  }

  private Connection sendCommand(Jedis conn, ProtocolCommand command, byte[]... args) {
    Connection client = conn.getClient();
    client.sendCommand(command, args);
    return client;
  }
}
