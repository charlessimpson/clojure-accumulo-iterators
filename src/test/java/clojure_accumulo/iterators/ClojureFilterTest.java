package clojure_accumulo.iterators;

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
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClojureFilterTest {
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
        m.put("", "b", "");
        m.put("", "c", "");
        writer.addMutation(m);

        m = new Mutation("d");
        m.put("", "e", "");
        writer.addMutation(m);

        writer.close();
    }

    @After
    public void tearDown() throws Exception {
        TableOperations tableOperations = conn.tableOperations();
        tableOperations.delete(TABLE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidationRequiresPredicate() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());
        s.addScanIterator(new IteratorSetting(10, "filter", "clojure_accumulo.iterators.ClojureFilter"));
        s.iterator();
    }

    @Test
    public void testFilter() throws Exception {
        Scanner s = conn.createScanner(TABLE_NAME, new Authorizations());

        IteratorSetting is = new IteratorSetting(10, "filter", "clojure_accumulo.iterators.ClojureFilter");
        is.addOption("pred",
                "(import '[org.apache.accumulo.core.data Key])" +
                "(fn [[^Key k v]] " +
                  "(= (str (.getRow k)) \"a\"))");
        s.addScanIterator(is);

        for (Map.Entry<Key, Value> e : s) {
            Assert.assertEquals("a", e.getKey().getRow().toString());
        }
    }
}
