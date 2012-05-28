package clojure_accumulo.iterators;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClojureCombinerTest {
    private static final String TABLE_NAME = "foo";
    private Connector conn;

    @Before
    public void setUp() throws Exception {
        Instance instance = new MockInstance();
        conn = instance.getConnector("user", "password");

        TableOperations tableOperations = conn.tableOperations();
        tableOperations.create(TABLE_NAME);
        tableOperations.removeIterator(TABLE_NAME, "vers", EnumSet.allOf(IteratorUtil.IteratorScope.class));

        BatchWriter writer = conn.createBatchWriter("foo", 1024, 0, 1);

        Mutation m = new Mutation("a");
        m.put("1", "b", 1L, "a-b1");
        m.put("1", "b", 2L, "a-b2");
        m.put("1", "b", 3L, "a-b3");
        writer.addMutation(m);

        m = new Mutation("d");
        m.put("1", "e", 1L, "d-e");
        writer.addMutation(m);

        writer.close();
    }

    @After
    public void tearDown() throws Exception {
        TableOperations tableOperations = conn.tableOperations();
        tableOperations.delete(TABLE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationRequiresFn() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.addScanIterator(new IteratorSetting(10, "combiner", "clojure_accumulo.iterators.ClojureCombiner"));
        s.iterator();
    }

    @Test
    public void testCombineMany() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.setRange(new Range("a"));

        // Join values with ',' in chronological order
        IteratorSetting is = new IteratorSetting(10, "combiner", "clojure_accumulo.iterators.ClojureCombiner");
        is.addOption("f",
                "(require '[clojure.string :as string])" +
                "(import '[org.apache.accumulo.core.data KeyValue Value])" +
                "(defn kvs [^KeyValue kv] (String. (.get (val kv))))" +
                "(fn [y x]" +
                  "(if (instance? KeyValue y)" +
                      "(string/join \",\" [(kvs x) (kvs y)])" +
                      "(string/join \",\" [(kvs x) y])))");
        is.addOption("columns", "1");
        s.addScanIterator(is);

        List<String> list = new ArrayList<String>();
        for (Map.Entry<Key, Value> e : s) {
            list.add(new String(e.getValue().get()));
        }

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("\"a-b1,a-b2,a-b3\"", list.get(0));
    }

    @Test
    public void testCombineManyWithInit() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.setRange(new Range("a"));

        IteratorSetting is = new IteratorSetting(10, "combiner", "clojure_accumulo.iterators.ClojureCombiner");
        is.addOption("f",
                "(import '[org.apache.accumulo.core.data Value])" +
                "(fn [x _]" +
                  "(inc x))");
        is.addOption("val", "0");
        is.addOption("columns", "1");
        s.addScanIterator(is);

        List<String> list = new ArrayList<String>();
        for (Map.Entry<Key, Value> e : s) {
            list.add(new String(e.getValue().get()));
        }

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("3", list.get(0));
    }

    @Test
    public void testCombineOne() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.setRange(new Range("d"));

        IteratorSetting is = new IteratorSetting(10, "combiner", "clojure_accumulo.iterators.ClojureCombiner");
        is.addOption("f", "(fn [_ _] (throw (Exception.)))");
        is.addOption("columns", "1");
        s.addScanIterator(is);

        List<String> list = new ArrayList<String>();
        for (Map.Entry<Key, Value> e : s) {
            list.add(new String(e.getValue().get()));
        }

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("d-e", list.get(0));
    }

    @Test
    public void testCombineOneWithInit() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.setRange(new Range("d"));

        IteratorSetting is = new IteratorSetting(10, "combiner", "clojure_accumulo.iterators.ClojureCombiner");
        is.addOption("f",
                "(import '[org.apache.accumulo.core.data Value])" +
                "(fn [x _]" +
                  "(inc x))");
        is.addOption("val", "0");
        is.addOption("columns", "1");
        s.addScanIterator(is);

        List<String> list = new ArrayList<String>();
        for (Map.Entry<Key, Value> e : s) {
            list.add(new String(e.getValue().get()));
        }

        Assert.assertEquals(1, list.size());
        Assert.assertEquals("1", list.get(0));
    }
}
