package io.cuckoo.client;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertThrows;

/**
 * @author Mark Nunberg
 */
public class ClientTest {
    static final int port;
    static {
        String tmpPort = System.getenv("REBLOOM_TEST_PORT");
        if (tmpPort != null && !tmpPort.isEmpty()) {
            port = Integer.parseInt(tmpPort);
        } else {
            port = 6379;
        }
    }

    Client cl = null;

    @Before
    public void clearDb() {
        cl = new Client("10.62.124.185", port,"Qwer12~");
       // cl._conn().flushDB();
    }

//    @Test
//    public void reserveBasic() {
//        cl.createFilter("myBloom", 100, 0.001);
//        assertTrue(cl.add("myBloom", "val1"));
//        assertTrue(cl.exists("myBloom", "val1"));
//        assertFalse(cl.exists("myBloom", "val2"));
//    }
//
//    @Test(expected = JedisException.class)
//    public void reserveValidateZeroCapacity() {
//        cl.createFilter("myBloom", 0, 0.001);
//    }
//
//    @Test(expected = JedisException.class)
//    public void reserveValidateZeroError() {
//        cl.createFilter("myBloom", 100, 0);
//    }
//
//    @Test(expected = JedisException.class)
//    public void reserveAlreadyExists() {
//        cl.createFilter("myBloom", 100, 0.1);
//        cl.createFilter("myBloom", 100, 0.1);
//    }

    @Test
    public void addExistsString() {
    //    cl.createFilter("aaa1", "64K",  1);

        for (int i=0;i<10;i++){
            System.out.println("第"+i+"次");
            cl.add("aaa1", "teteteet2342teteteet2342teteteet23421231etetetetetetetetwteteteteteet234234242");
        }

//        assertTrue("存在",cl.exists("aaa", "foo"));
//        assertTrue("不存在",!cl.exists("aaa", "tt"));
//        cl.remove("aaa", "foo");
//        System.out.println(cl.exists("aaa", "foo"));


//        assertFalse(cl.exists("aaa", "bar"));
//        assertFalse(cl.add("aaa", "foo"));
    }

//    @Test
//    public void addExistsByte() {
//        assertTrue(cl.add("newFilter", "foo".getBytes()));
//        assertFalse(cl.add("newFilter", "foo".getBytes()));
//        assertTrue(cl.exists("newFilter", "foo".getBytes()));
//        assertFalse(cl.exists("newFilter", "bar".getBytes()));
//    }

    public void testExistsNonExist() {
      assertFalse(cl.exists("nonExist", "foo"));
    }





    @Test
    public void createFilter() {
      cl.createFilter("aaa", "64K",  1);
    }

}