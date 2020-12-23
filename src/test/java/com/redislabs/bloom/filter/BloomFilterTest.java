package com.redislabs.bloom.filter;

import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli
 * @date 2020/12/23
 */
public class BloomFilterTest {

    BloomFilter filter = null;

    @Before
    public void clearDb() {
        filter = new BloomFilter("10.62.124.185", 6379, "xxxxx");
    }


    String filterName="t_bloom_filter";

    @Test
    public void createFilter() {
        filter.createFilter(filterName,64,0.01);
    }

    @Test
    public void add() {
        boolean result = filter.add(filterName, "xxx1");
        System.out.println("add:"+result);

    }
}