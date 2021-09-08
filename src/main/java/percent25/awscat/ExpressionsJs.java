package percent25.awscat;

import java.security.*;
import java.text.*;
import java.time.*;
import java.util.*;

import com.google.common.io.*;
import com.google.gson.*;

import org.graalvm.polyglot.*;
import org.graalvm.polyglot.proxy.*;

import helpers.DynamoHelper;

public class ExpressionsJs {

  public class RootObject {
    private final String now;
    public RootObject(String now) {
      this.now = now;
    }
    public String now() {
      return now;
    }
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
    public Object inferDynamoDbJson(Object jsonObject) {
      JsonElement jsonElement = toJsonElement(jsonObject);
      JsonElement dynamoDbJson = DynamoHelper.inferDynamoDbJson(jsonElement);
      return fromJsonElement(dynamoDbJson);
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

  // get current element
  public JsonElement e() {
    return toJsonElement(bindings.getMember("e"));
  }

  // set current element
  public void e(JsonElement e) {
    bindings.putMember("e", fromJsonElement(e));
  }

  // eval
  public boolean eval(String js) {
    Value value = context.eval("js", js);
    // coerce to truthy/falsey
    return context.eval("js", "(function(s){return !!s})").execute(value).asBoolean();
  }

  private JsonElement toJsonElement(Object object) {
    Value value = Value.asValue(object);
    if (value.hasArrayElements())
      return new Gson().toJsonTree(value.as(new TypeLiteral<List<Object>>(){}));
    return new Gson().toJsonTree(value.as(Object.class));

  }

  private Object fromJsonElement(JsonElement jsonElement) {

    // @see ProxyArray.fromArray
    if (jsonElement.isJsonArray()) {
      JsonArray array = jsonElement.getAsJsonArray();
      return new ProxyArray() {
        @Override
        public Object get(long index) {
            return fromJsonElement(array.get((int) index));
        }
        @Override
        public void set(long index, Value value) {
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            array.set((int) index, new Gson().toJsonTree(value.as(Object.class)));
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
          }
        @Override
        public boolean remove(long index) {
            return array.remove((int) index) != null;
        }
        @Override
        public long getSize() {
          return array.size();
        }
      };
    }

    // @see ProxyObject.fromMap
    if (jsonElement.isJsonObject()) {
      JsonObject object = jsonElement.getAsJsonObject();
      return new ProxyObject() {
        @Override
        public Object getMember(String key) {
          return fromJsonElement(object.get(key));
        }

        @Override
        public Object getMemberKeys() {
            return new ProxyArray() {
                private final Object[] keys = object.keySet().toArray();

                @Override
                public Object get(long index) {
                    if (index < 0 || index > Integer.MAX_VALUE) {
                        throw new ArrayIndexOutOfBoundsException();
                    }
                    return keys[(int) index];
                }

                @Override
                public void set(long index, Value value) {
                    throw new UnsupportedOperationException("set");
                }

                @Override
                public long getSize() {
                    return keys.length;
                }
            };
        }

        @Override
        public boolean hasMember(String key) {
            return object.has(key);
        }

        @Override
        public void putMember(String key, Value value) {
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            object.add(key, new Gson().toJsonTree(value.as(Object.class)));
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
            //###TODO use toJsonElement here??
          }

        @Override
        public boolean removeMember(String key) {
          return object.remove(key) != null;
        }
      };
    }

    if (jsonElement.isJsonPrimitive()) {
      if (jsonElement.getAsJsonPrimitive().isNumber()) {
        try {
          return NumberFormat.getInstance().parse(jsonElement.getAsString());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    return new Gson().fromJson(jsonElement, Object.class);
  }

  // static JsonElement json(String json) {
  //   return new JsonStreamParser(json).next();
  // }

  // private void debug(Object... args) {
  //   new LogHelper(this).debug(args);
  // }

  public static void main(String... args) {

    ExpressionsJs js = new ExpressionsJs();

    js.e(new JsonStreamParser("{}").next());
    
    System.out.println("eval="+js.eval("e.a=1"));
    System.out.println("eval="+js.eval("e.b=4/3")); // 1.3333333333333333
    System.out.println("e="+js.e());
  }

}
