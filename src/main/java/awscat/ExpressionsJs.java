package awscat;

import java.security.*;
import java.text.*;
import java.time.*;
import java.util.*;

import com.google.common.io.*;
import com.google.gson.*;

import org.graalvm.polyglot.*;
import org.graalvm.polyglot.proxy.*;

public class ExpressionsJs {

  public class RootObject {
    private final String now;
    public RootObject(String now) {
      this.now = now;
    }
    // @HostAccess.Export
    public String now() {
      return now;
    }
    // @HostAccess.Export
    public String uuid() {
      return UUID.randomUUID().toString();
    }
    // returns a random string w/fixed length len
    public String fixedString(int len) {
      byte[] bytes = new byte[(3 * len + 3) / 4];
      new SecureRandom().nextBytes(bytes);
      String randomString = BaseEncoding.base64Url().encode(bytes).substring(0);
      return randomString.substring(0, Math.min(len, randomString.length()));
    }
    // returns a random string w/random length [1..len]
    public String randomString(int len) {
      byte[] bytes = new byte[new SecureRandom().nextInt((3 * len + 3) / 4) + 1];
      new SecureRandom().nextBytes(bytes);
      String randomString = BaseEncoding.base64Url().encode(bytes).substring(0);
      return randomString.substring(0, Math.min(len, randomString.length()));
    }
    public String toString() {
      return new Gson().toJson(this);
    }
  }
  
  private final Context context;
  private final Value bindings;

  public ExpressionsJs() {
    this(Instant.now().toString());
  }
  
  public ExpressionsJs(String now) {
    context = Context.newBuilder().allowHostAccess(HostAccess.ALL).build();
    bindings = context.getBindings("js");

    Value rootObject = context.asValue(new RootObject(now));
    for (String identifier : rootObject.getMemberKeys())
      bindings.putMember(identifier, rootObject.getMember(identifier));
  }

  // get
  public JsonElement e() {
    Value e = bindings.getMember("e");
    if (e.hasArrayElements())
      return new Gson().toJsonTree(e.as(new TypeLiteral<List<Object>>(){}));
    return new Gson().toJsonTree(e.as(Object.class));
  }

  // set
  public void e(String e) {
    e(json(e));
  }

  // set
  public void e(JsonElement e) {
    bindings.putMember("e", fromJsonElement(e));
  }

  // eval
  public boolean eval(String expressionString) {
    Value value = context.eval("js", expressionString);
    // coerce to truthy/falsey
    return context.eval("js", "(function(s){return !!s})").execute(value).asBoolean();
  }

  // @see ProxyObject.fromMap
  private Object fromJsonElement(JsonElement jsonElement) {

    if (jsonElement.isJsonArray()) {
      JsonArray values = jsonElement.getAsJsonArray();
      return new ProxyArray() {
        @Override
        public Object get(long index) {
            checkIndex(index);
            return fromJsonElement(values.get((int) index));
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
            values.add(key, new Gson().toJsonTree(value.as(Object.class)));
        }

        public boolean hasMember(String key) {
            return values.has(key);
        }

        public Object getMemberKeys() {
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
          return fromJsonElement(values.get(key));
        }

        @Override
        public boolean removeMember(String key) {
            if (values.has(key)) {
                values.remove(key);
                return true;
            } else {
                return false;
            }
        }
      };
    }

    if (jsonElement.isJsonPrimitive()) {
      if (jsonElement.getAsJsonPrimitive().isNumber()) {
        try {
          return Value.asValue(NumberFormat.getInstance().parse(jsonElement.getAsString()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    return Value.asValue(new Gson().fromJson(jsonElement, Object.class));
  }

  static JsonElement json(String json) {
    return new JsonStreamParser(json).next();
  }

  // private void debug(Object... args) {
  //   new LogHelper(this).debug(args);
  // }

  public static void main(String... args) {

    ExpressionsJs js = new ExpressionsJs();

    js.e(json("{}"));
    
    System.out.println("eval="+js.eval("e.a=1"));
    System.out.println("eval="+js.eval("e.b=4/3")); // 1.3333333333333333
    System.out.println("e="+js.e());
  }

}
