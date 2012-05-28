package clojure_accumulo.iterators;

import java.io.IOException;
import java.util.Map;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Allow a Clojure function to implement the accept method in a Filter. The
 * iterator takes a single argument, pred, which defines this function.
 *
 * pred is a function of a single argument, a map entry containing a key-value
 * pair.  If pred returns true, the pair is accepted.
 *
 * Basically, this class is a server-side implementation of "filter" where the
 * collection is a sorted map of Keys and Values.
 */
public final class ClojureFilter extends Filter {
    private static final String fnOption = "pred";
    private IFn fn;
    
    public boolean accept(Key k, Value v) {
        KeyValue kv = new KeyValue(k, v.get());
        return RT.booleanCast(fn.invoke(kv));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        if (options == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureFilter");
        }
        
        String fnString = options.get(fnOption);
        if (fnString == null) {
            throw new IllegalArgumentException(fnOption + " must be set for ClojureFilter");
        }

        Object obj = ClojureIteratorUtils.eval(fnString);
        if (!(obj instanceof IFn)) {
            throw new IllegalArgumentException(fnOption + " must compile to something that implements IFn");
        }

        fn = (IFn) obj;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io =  super.describeOptions();
        
        io.addNamedOption(fnOption, "String containing filter predicate function");
        io.setName("cljfilter");
        io.setDescription("ClojureFilter allows a Clojure function to be passed in and invoked as the Filter's accept method");
        
        return io;
    }
}
