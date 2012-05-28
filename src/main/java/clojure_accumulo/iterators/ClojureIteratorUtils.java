package clojure_accumulo.iterators;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;

import clojure.lang.Compiler;
import clojure.lang.RT;
import clojure.lang.Var;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;

public class ClojureIteratorUtils {

    /**
     * Return an object as a Value.
     *
     * If the object is already a Value, return it untouched.  Otherwise,
     * attempt to print it as a Clojure object.
     *
     * @param obj object to cast to Value
     * @return object as a Value
     */
    public static Value toValue(Object obj) {
        if (obj instanceof Value) {
            return (Value) obj;
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamWriter writer = new OutputStreamWriter(baos);

            try {
                RT.print(obj, writer);
                writer.close();
            } catch (Exception e) {
                // I can't get Util.sneakyThrow to resolve for some reason
                throw new RuntimeException(e);
            }

            return new Value(baos.toByteArray());
        }
    }

    /**
     * Evaluate a string of Clojure, returning the last evaluated expression.
     *
     * @param str some Clojure
     * @return last evaluated expression.
     */
    public static Object eval(String str) {
        Object ret;

        try {
            // Set the class loader to use Accumulo's so we get Hadoop,
            // Accumulo, and whatever is in lib/ext.
            Thread.currentThread().setContextClassLoader(AccumuloClassLoader.getClassLoader());
            Var.pushThreadBindings(RT.map(
                    RT.USE_CONTEXT_CLASSLOADER, RT.T
            ));

            // Actually compile the given code.
            ret = Compiler.load(new StringReader(str));
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            Var.popThreadBindings();
        }

        return ret;
    }
}
