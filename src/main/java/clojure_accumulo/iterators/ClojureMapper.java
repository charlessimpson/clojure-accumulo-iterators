package clojure_accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import clojure.lang.IFn;
import clojure.lang.IPersistentCollection;
import clojure.lang.RT;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * Basically, a server-side implementation of "map".
 *
 * If the mapping function f returns a two-element vector where the first
 * element is an instance of Key, then it is used as the Key and the second
 * element is used as the Value.  Otherwise, the original key is retained and
 * the entire returned value is the value.
 *
 * For example,
 *
 * (fn [[k v]]
 *   [(Key. (clojure.string/upper-case (.getRow k)))
 *    v])
 *
 * will return the newly upper-cased key from the iterator,
 *
 * (fn [[k v]]
 *   1)
 *
 * will retain the original key k and return "1" from the iterator as the value.
 */
public final class ClojureMapper extends WrappingIterator implements OptionDescriber {
    private static final String fnOption = "f";
    private IFn fn;
    private Key key;
    private Value value;

    private void doMap() {
        Key k = getSource().getTopKey();
        Value v = getSource().getTopValue();

        KeyValue kv = new KeyValue(k, v.get());
        Object ret = fn.invoke(kv);

        if (ret instanceof IPersistentCollection &&
                RT.count(ret) == 2 &&
                RT.first(ret) instanceof Key) {
            key = (Key) RT.first(ret);
            value = ClojureIteratorUtils.toValue(RT.second(ret));
        } else {
            key = k;
            value = ClojureIteratorUtils.toValue(ret);
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureMapper");
        }

        String fnString = options.get(fnOption);
        if (fnString == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureMapper");
        }

        Object obj = ClojureIteratorUtils.eval(fnString);
        if (!(obj instanceof IFn)) {
            throw new IllegalArgumentException(fnOption + " must compile to something that implements IFn");
        }

        fn = (IFn) obj;
    }

    @Override
    public Key getTopKey() {
        if (key == null && getSource().getTopKey() != null) {
            doMap();
        }

        return key;
    }

    @Override
    public Value getTopValue() {
        if (value == null && getSource().getTopValue() != null) {
            doMap();
        }

        return value;
    }

    @Override
    public void next() throws IOException {
        super.next();

        key = null;
        value = null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);

        key = null;
        value = null;
    }

    public OptionDescriber.IteratorOptions describeOptions() {
        return new IteratorOptions("cljmapper",
                "ClojureMApper allows a Clojure function to be passed in and invoked on every key-value pair",
                Collections.singletonMap(fnOption, "String containing mapper map function"),
                Collections.<String>emptyList());
    }

    public boolean validateOptions(Map<String, String> options) {
        return (options != null) && (options.get(fnOption) != null);
    }
}
