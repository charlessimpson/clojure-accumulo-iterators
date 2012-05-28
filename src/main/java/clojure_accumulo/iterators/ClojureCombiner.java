package clojure_accumulo.iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import clojure.lang.IFn;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Allow a Clojure function to implement the accept method in a Combiner. The
 * iterator takes an argument, f, which defines this function.  The iterator
 * also takes an optional argument, val, the starting value for the reduce.
 *
 * Basically, this class is a server-side implementation of "reduce" where the
 * collection is a sorted map of Keys and Values.
 */
public final class ClojureCombiner extends Combiner {
    private static final String fnOption = "f";
    private static final String valOption = "val";
    private IFn fn;
    private Object val;

    private Value reduceInternal(Key key, Iterator<Value> valueIterator) {
        Value v1;
        if (valueIterator.hasNext()) {
            v1 = valueIterator.next();
        } else {
            // I don't think that we can get here.
            return ClojureIteratorUtils.toValue(fn.invoke());
        }

        Value v2;
        if (valueIterator.hasNext()) {
            v2 = valueIterator.next();
        } else {
            return v1;
        }

        Object ret = fn.invoke(new KeyValue(key, v1.get()), new KeyValue(key, v2.get()));

        return reduceInternal(key, valueIterator, ret);
    }

    private Value reduceInternal(Key key, Iterator<Value> valueIterator, Object start) {
        Object ret = start;
        while (valueIterator.hasNext()) {
            Value v = valueIterator.next();
            KeyValue kv = new KeyValue(key, v.get());

            ret = fn.invoke(ret, kv);
        }

        return ClojureIteratorUtils.toValue(ret);
    }

    @Override
    public Value reduce(Key key, Iterator<Value> valueIterator) {
        return (val == null) ? reduceInternal(key, valueIterator) : reduceInternal(key, valueIterator, val);
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureCombiner");
        }

        String fnString = options.get(fnOption);
        if (fnString == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureCombiner");
        }

        Object obj = ClojureIteratorUtils.eval(fnString);
        if (!(obj instanceof IFn)) {
            throw new IllegalArgumentException(fnOption + " must compile to something that implements IFn");
        }
        fn = (IFn) obj;

        String valString = options.get(valOption);
        if (valString != null) {
            val = ClojureIteratorUtils.eval(valString);
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io =  super.describeOptions();

        io.addNamedOption(fnOption, "String containing combiner reduce function");
        io.addNamedOption(valOption, "Optional string containing initial reduce value");
        io.setName("cljcombiner");
        io.setDescription("ClojureCombiner allows a Clojure function to be passed in and invoked as the Combiner's reduce method");

        return io;
    }
}
