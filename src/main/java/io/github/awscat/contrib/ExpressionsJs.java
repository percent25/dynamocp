package io.github.awscat.contrib;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.TypeLiteral;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.Proxy;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import helpers.LogHelper;
import helpers.ObjectHelper;


// * assert context.eval("js", "(function(record) record.x)")
// *               .execute(record).asInt() == 42;

public class ExpressionsJs {

  private final Context context;
  private final Value bindings;

  public ExpressionsJs(JsonElement e) {
    this(e, Instant.now().toString());
  }
  
  public ExpressionsJs(JsonElement e, String now) {
    context = Context.newBuilder().allowHostAccess(HostAccess.ALL).build();
    bindings = context.getBindings("js");

    Value rootObject = context.asValue(new RootObject(now));
    for (String identifier : rootObject.getMemberKeys())
      bindings.putMember(identifier, rootObject.getMember(identifier));

    bindings.putMember("e", fromJsonElement(e));


    // rootObject = new RootObject(now);
    // context = new StandardEvaluationContext(rootObject);
    // parser = new SpelExpressionParser();

    // rootObject.e = ObjectHelper.toObject(e);
  }

  public JsonElement e() {
    Value e = bindings.getMember("e");
    if (e.hasArrayElements())
      return new Gson().toJsonTree(e.as(new TypeLiteral<List<Object>>(){}));
    return new Gson().toJsonTree(e.as(Object.class));
  }

  public boolean eval(String expressionString) {
    Value value = context.eval("js", expressionString);
    return context.eval("js", "(function(s){return !!s})").execute(value).asBoolean();
  }

  public static void main(String... args) {

    ExpressionsJs js = new ExpressionsJs(json("'a'"));

    System.out.println("e="+js.e());
    System.out.println("eval="+js.eval("e?.b?.c"));
    // System.out.println("eval="+js.eval("e.a==1"));
  }

  public static void mainz(String... args) {

    // awscat.jar --filter="e?.id?.s"
    Context context = Context.newBuilder().allowHostAccess(HostAccess.ALL).build();

    Value bindings = context.getBindings("js");
    Value rootObject = context.asValue(new RootObject(Instant.now().toString()));
    for (String identifier : rootObject.getMemberKeys())
      bindings.putMember(identifier, rootObject.getMember(identifier));

    bindings.putMember("e", fromJsonElement(new JsonObject()));

    // System.out.println("eval="+context.eval("js", "fixedString(128)"));
    // System.out.println("eval="+context.eval("js", "e.abc=123"));
    
    System.out.println("eval="+context.eval("js", "e"));
    System.out.println("eval="+context.eval("js", "e='asdf'"));
    System.out.println("eval="+context.eval("js", "e"));
    System.out.println("eval="+context.eval("js", "e={}"));
    System.out.println("eval="+context.eval("js", "e"));

    System.out.println("eval="+context.eval("js", "e.id={}"));
    System.out.println("eval="+context.eval("js", "e.id.s=uuid()"));
    System.out.println("eval="+context.eval("js", "e=[1]"));

    System.out.println("### e ###="+bindings.getMember("e").as(Object.class));
    // System.out.println("### e ###="+bindings.getMember("e").as(Object.class).getClass());
    Value e = bindings.getMember("e");
    System.out.println("e="+e.as(Object.class));
    System.out.println("getClass="+e.as(Object.class).getClass());

    JsonElement out = new Gson().toJsonTree(e.as(Object.class));
    System.out.println("JsonElement out="+out);
    System.out.println("JsonElement out="+out.getClass());

    // System.out.println("eval="+context.eval("js", "this.a"));
    // System.out.println("isHostObject="+root.isHostObject());

    // System.out.println("zzz="+root.getMember("console"));

    System.exit(0);

    JsonElement jsonElement = new Gson().fromJson("{id:{s:'abc'}}", JsonElement.class);

    context.getBindings("js").putMember("e", fromJsonElement(jsonElement));
    context.getBindings("js").putMember("uuid", context.asValue(Suppliers.ofInstance(UUID.randomUUID().toString())));

    context.eval("js", "e.id.t=uuid(), e.version={a:1,b:2}");
    // jsonElement = eval(jsonElement, context, "e.version.c=3");
    // jsonElement = eval(jsonElement, context, "e.version.c=4.1");

    // context.eval("js", "(function(s){return{Item:s}})");
    // jsonElement = eval(jsonElement, context, "(function(s){ return [2,3,4,5.1,{}].length.toFixed(5) })");

     context.eval("js", "e={e:e,uuid:uuid()}");
     context.eval("js", "e={e:e,uuid:uuid()}");
     context.eval("js", "e={e:e,uuid:uuid()}");

    // System.out.println("e="+context.getBindings("js").getMember("e"));

          // Value e = context.getBindings("js").getMember("e");
          // JsonElement out = new Gson().toJsonTree(e.as(Object.class));
          // if (e.hasArrayElements())
          //   out = new Gson().toJsonTree(e.as(new TypeLiteral<List<Object>>(){}));

          // log(jsonElement+" --> "+out);
  }

      // static JsonElement eval(Context context, String js) {
      //   var eval = context.eval("js", js);
      //   // if (eval.canExecute()) {
      //   //   var value = eval.execute(fromJsonElement(context, jsonElement));
      //   //             // log("hasMembers", value.hasMembers(), "hasArrayElements", value.hasArrayElements());
      //   //             // // value.hasArrayElements();
      //   //             // // value.as(targetType)
      //   //             // if (value.hasMembers()) {
      //   //             //   log("getMemberKeys", value.getMemberKeys());
      //   //             // }
      //   //             // if (value.hasArrayElements()) {
      //   //             //   log("getArraySize", value.getArraySize());
      //   //             // }
      //   //   Object src = value.as(Object.class);
      //   //   // log(src, src.getClass());
      //   //   //     * static final TypeLiteral&lt;List&lt;String>> STRING_LIST = new TypeLiteral&lt;List&lt;String>>() {
      //   //   if (value.hasArrayElements())
      //   //     src = value.as(new TypeLiteral<List<Object>>(){});

      //   //   jsonElement = new Gson().toJsonTree(src);
      //   // }
      //   return jsonElement;
      // }

  static Object fromJsonElement(JsonElement jsonElement) {

    if (jsonElement.isJsonArray()) {
      JsonArray values = jsonElement.getAsJsonArray();
      return new ProxyArray() {
        @Override
        public Object get(long index) {
            checkIndex(index);
            return values.get((int) index);
        }
        @Override
        public void set(long index, Value value) {
            checkIndex(index);
            values.set((int) index, new Gson().toJsonTree(value.as(Object.class)));
        }
        @Override
        public boolean remove(long index) {
            checkIndex(index);
            values.remove((int) index);
            return true;
        }
        private void checkIndex(long index) {
            if (index < 0 || index > Integer.MAX_VALUE) {
                throw new ArrayIndexOutOfBoundsException();
            }
        }
        public long getSize() {
            return values.size();
        }
      };
    }

    if (jsonElement.isJsonObject()) {
      JsonObject values = jsonElement.getAsJsonObject();
      return new ProxyObject() {

        public void putMember(String key, Value value) {
          log("putMember", key, value, value.as(Object.class).getClass());
          values.add(key, new Gson().toJsonTree(value.as(Object.class)));
        }

        public boolean hasMember(String key) {
          log("hasMember", key);
            return values.has(key);
        }

        public Object getMemberKeys() {
          log("getMemberKeys");
            return new ProxyArray() {
                private final Object[] keys = values.keySet().toArray();

                public void set(long index, Value value) {
                    throw new UnsupportedOperationException();
                }

                public long getSize() {
                    return keys.length;
                }

                public Object get(long index) {
                    if (index < 0 || index > Integer.MAX_VALUE) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                    return keys[(int) index];
                }
            };
        }

        public Object getMember(String key) {
          log("getMember", key);
          return fromJsonElement(values.get(key));
        }

        @Override
        public boolean removeMember(String key) {
          log("removeMember", key);
            if (values.has(key)) {
                values.remove(key);
                return true;
            } else {
                return false;
            }
        }
      };
    }
    //###WORKAROUND
    if (jsonElement.isJsonPrimitive()) {
      if (jsonElement.getAsJsonPrimitive().isNumber()) {
        try {
          return Value.asValue(NumberFormat.getInstance().parse(jsonElement.getAsString()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    //###WORKAROUND
    return Value.asValue(new Gson().fromJson(jsonElement, Object.class));
  }

  static JsonElement json(String json) {
    return new JsonStreamParser(json).next();
  }

  static void log(Object... args) {
    new LogHelper(ExpressionsJs.class).log(args);
  }

}
