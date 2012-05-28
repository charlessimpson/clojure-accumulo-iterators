package clojure_accumulo.iterators;

import java.util.HashMap;

import clojure.lang.IFn;
import clojure.lang.Namespace;
import clojure.lang.Symbol;
import junit.framework.Assert;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class ClojureIteratorUtilsTest {

    @Test
    public void testToValueSerializesClojure() {
        Object obj = ClojureIteratorUtils.eval("[1 2 3]");
        Value v = ClojureIteratorUtils.toValue(obj);

        Assert.assertEquals("[1 2 3]", new String(v.get()));
    }

    @Test
    public void testToValuePassesValuesThrough() {
        Value v1 = new Value();
        Value v2 = ClojureIteratorUtils.toValue(v1);

        Assert.assertSame(v1, v2);
    }

    @Test
    public void testEvalAllowsRefs() {
        IFn obj = (IFn) ClojureIteratorUtils.eval(
                "(require '[clojure.string :as string])" +
                        "(fn [s] (string/upper-case s))"
        );

        String s = (String) obj.invoke("a");
        Assert.assertEquals("A", s);
    }

    @Test
    public void testEvalAllowsImports() {
        IFn obj = (IFn) ClojureIteratorUtils.eval(
                "(import '[java.util HashMap])" +
                        "(fn [] (HashMap.))"
        );

        HashMap m = (HashMap) obj.invoke();
        Assert.assertEquals(new HashMap(), m);
    }
}
