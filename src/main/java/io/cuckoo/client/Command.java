package io.cuckoo.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Command implements ProtocolCommand {
    INIT("CF.INIT"),
    ADD("CF.ADD"),
    EXISTS("CF.CHECK"),
    REMOVE("CF.REM");

    private final byte[] raw;

    Command(String alt) {
        raw = SafeEncoder.encode(alt);
    }

    public byte[] getRaw() {
        return raw;
    }
}
