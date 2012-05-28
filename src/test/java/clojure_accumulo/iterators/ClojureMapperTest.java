package clojure_accumulo.iterators;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClojureMapperTest {
    private static final String TABLE_NAME = "foo";
    private Connector conn;

    @Before
    public void setUp() throws Exception {
        Instance instance = new MockInstance();
        conn = instance.getConnector("user", "password");

        TableOperations tableOperations = conn.tableOperations();
        tableOperations.create(TABLE_NAME);

        BatchWriter writer = conn.createBatchWriter("foo", 1024, 0, 1);

        Mutation m = new Mutation("a");
        m.put("", "b", "a-b");
        m.put("", "c", "a-c");
        writer.addMutation(m);

        m = new Mutation("d");
        m.put("", "e", "d-e");
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
        s.addScanIterator(new IteratorSetting(10, "mapper", "clojure_accumulo.iterators.ClojureMapper"));
        s.iterator();
    }

    @Test
    public void testMapValues() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());

        IteratorSetting is = new IteratorSetting(10, "mapper", "clojure_accumulo.iterators.ClojureMapper");
        is.addOption("f",
                "(require '[clojure.string :as string])" +
                "(import '[org.apache.accumulo.core.data Value])" +
                "(fn [[_ ^Value v]] " +
                   "(Value. (.getBytes (string/upper-case (String. (.get v))))))");
        s.addScanIterator(is);

        Set<String> expected = new HashSet<String>();
        expected.add("A-B");
        expected.add("A-C");
        expected.add("D-E");

        Set<String> actual = new HashSet<String>();
        for (Map.Entry<Key, Value> e : s) {
            actual.add(new String(e.getValue().get()));
        }

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testMapKeys() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());

        IteratorSetting is = new IteratorSetting(10, "mapper", "clojure_accumulo.iterators.ClojureMapper");
        is.addOption("f",
                "(require '[clojure.string :as string])" +
                "(import '[org.apache.accumulo.core.data Key])" +
                "(fn [[^Key k v]] " +
                  "[(Key. (string/upper-case (str (.getRow k))) " +
                         "(str (.getColumnFamily k)) " +
                         "(str (.getColumnQualifier k)))" +
                     "v])");
        s.addScanIterator(is);

        Set<String> expected = new HashSet<String>();
        expected.add("A");
        expected.add("D");

        Set<String> actual = new HashSet<String>();
        for (Map.Entry<Key, Value> e : s) {
            actual.add(e.getKey().getRow().toString());
        }

        Assert.assertEquals(expected, actual);
    }
}
