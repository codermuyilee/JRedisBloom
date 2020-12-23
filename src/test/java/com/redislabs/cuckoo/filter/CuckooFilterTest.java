package com.redislabs.cuckoo.filter;

import com.redislabs.bloom.utils.InsertOptions;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yangli
 * @date 2020/12/23
 */
public class CuckooFilterTest {

    CuckooFilter filter = null;

    @Before
    public void clearDb() {
        filter = new CuckooFilter("10.62.124.185", 6379, "xxx");
    }


    String filterName="t_filter";

    @Test
    public void createFilter() {
        filter.createFilter(filterName,64);

    }

    @Test
    public void add() {
        boolean result = filter.add(filterName, "xxx");
        System.out.println("add:"+result);
    }

    @Test
    public void addNX() {
        boolean result = filter.addNX(filterName, "xxx");
        System.out.println("addnx:"+result);
    }

    @Test
    public void insert() {
        InsertOptions insertOptions=new InsertOptions();
        insertOptions.capacity(10);
        boolean[] result = filter.insert(filterName,insertOptions, "xxx1");
        System.out.println("exists:"+result);
    }


    @Test
    public void insertnx() {
        InsertOptions insertOptions=new InsertOptions();
        insertOptions.capacity(10);
        boolean[] result = filter.insertnx("insert_nx_filter",insertOptions, "xxx1");
        System.out.println("insertnx:"+result[0]);
    }


    @Test
    public void exists() {
        boolean result = filter.exists(filterName, "xxx");
        System.out.println("exists:"+result);
    }

    @Test
    public void del() {
        boolean result = filter.del(filterName, "xxx");
        System.out.println("del:"+result);
    }
}