package com.redislabs.cuckoo.utils;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Command implements ProtocolCommand {
    RESERVE("CF.RESERVE"),
    ADD("CF.ADD"),
    ADDNX("CF.ADDNX"),
    EXISTS("CF.EXISTS"),
    INSERT("CF.INSERT"),
    INSERTNX("CF.INSERTNX"),
    DEL("CF.DEL"),
    INFO("CF.INFO");

    private final byte[] raw;

    Command(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
