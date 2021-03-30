package helpers;

import org.junit.jupiter.api.Test;

import io.github.awscat.contrib.ExpressionsJs;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

// https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions
public class ExpressionsJsTest {

  private final String now = Instant.now().toString();
  // private final JsonElement jsonElement = new JsonObject();
  // private final Expressions expressions = new Expressions(jsonElement);
  
  // the set of falsey values is static.. here are the falsey values
  private final Set<String> allFalsey = ImmutableSet.of( //
      "null", // json null
      "false", // json primitive bool
      "0", "-0", "0.0", "-0.0", // json primitive number
      "''"); // json primitive string      
      // "''", "'false'", "'0'", "'-0'", "'0.0'", "'-0.0'"); // json primitive string      

  // if it is not falsey then it is truthy.. here are some truthy values
  private final Set<String> someTruthy = ImmutableSet.of( //
      "true", // json primitive bool
      "1", "-1", "1.0", "-1.0", // json primitive number
      "'true'", // json primitive string
      "[]", "[null]", "[false]", "[0]", "['']", "['false']", "['0']", // any json array
      "{}"); // any json object

  @Test
  public void boolTest() {

    assertThat(bool(json("{id:{s:1}}"), "e.id.s==1")).isEqualTo(true);
    assertThat(bool(json("{id:{s:1}}"), "e.id.s==2")).isEqualTo(false);

    // System.out.println(parser.parseExpression("e.id?.s?.length").getValue(context));
    // System.out.println(parser.parseExpression("e.id?.s?.length>0").getValue(context));
    // System.out.println(" ### "+parser.parseExpression("e.idz!=null").getValue(context, boolean.class));

    // // System.out.println(parser.parseExpression("e.aaa={:}").getValue(context));
    // // System.out.println(parser.parseExpression("e.aaa.bbb=222").getValue(context));

    // System.out.println(parser.parseExpression("e.id.s='123'").getValue(context));
    // // System.out.println(parser.parseExpression("value={s:id.s.length}").getValue(context));
    // // System.out.println(parser.parseExpression("#this = 5.0").getValue(context));

  }

  @Test
  public void safeNavigationTest() {

    assertThrows(Exception.class, ()->{
      bool(json("null"), "e.id.s==1");
    });

    assertThrows(Exception.class, ()->{
      bool(json("null"), "e.id?.s==1");
    });

    assertThat(bool(json("null"), "e?.id.s==1")).isEqualTo(false);

    // assertThat(bool(json("null"), "e?.id?.s==1")).isEqualTo(false);
    assertThrows(Exception.class, ()->{
      bool(json("{}"), "e?.id.s==1");
    });
    assertThat(bool(json("{}"), "e?.id?.s==1")).isEqualTo(false);

    assertThat(bool(json("{id:{}}"), "e?.id?.s==1")).isEqualTo(false);
    assertThat(bool(json("{id:{s:1}}"), "e?.id?.s==1")).isEqualTo(true);

  }

  @Test
  public void outputTest() {

    // .eval("id?.s")

    // e = new Expressions(e).apply("e.id=222");
    // value = new Expressions(e).value("e?.id?.s?.length()>0");
        
    assertThat(output(json("{}"), "e")).isEqualTo(json("{}")); // identity

    // unconditional transform
    assertThat(output(json("null"), "e='abc'")).isEqualTo(json("'abc'"));
    assertThat(output(json("null"), "e='abc'").getAsString()).isEqualTo("abc");

    assertThat(output(json("{}"), "e=1")).isEqualTo(json("1"));
    assertThat(output(json("{}"), "e=1").getAsInt()).isEqualTo(1);
    assertThat(output(json("{}"), "e=1.2")).isEqualTo(json("1.2"));
    assertThat(output(json("{}"), "e=1.2").getAsDouble()).isEqualTo(1.2);

    assertThat(output(json("{}"), "e=now()").getAsString()).isEqualTo(now);
    assertThat(output(json("{}"), "e=uuid()").getAsString()).hasSize(36);

    // assertThat(eval(e, "e=randomString(24)").getAsString()).hasSize(32);

    assertThat(output(json("{}"), "e=[1,2,3]")).isEqualTo(json("[1,2,3]"));

    assertThat(output(json("{}"), "e.id={}")).isEqualTo(json("{id:{}}"));
    assertThat(output(json("{}"), "e.id={s:'foo'}")).isEqualTo(json("{id:{s:'foo'}}"));
    assertThat(output(json("{}"), "e.id={s:'bar'}")).isEqualTo(json("{id:{s:'bar'}}"));

  }

  // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
  @Test
  public void truthyTest() {

    // var truthy = ImmutableSet.of( //
    //     "true", // json primitive bool
    //     "1", "-1", "1.0", "-1.0", // json primitive number
    //     "'true'", "'not-empty'", // json primitive string
    //     "[]", "{}");

    for (String e : someTruthy) {
      for (Entry<String, String> entry : ImmutableMap.of("%s", "e", "[%s]", "e[0]", "{e:%s}", "e.e").entrySet()) {
        String fmt = entry.getKey();
        String str = entry.getValue();
        assertThat(bool(json(String.format(fmt, e)), str)).as("jsonElement=%s", e).isEqualTo(true);
      }
    }

    assertThat(bool(json("[1]"), "e")).isEqualTo(true); //###TODO put this in someTruthy ?

  }

  // https://developer.mozilla.org/en-US/docs/Glossary/Falsy
  @Test
  public void falseyTest() {

    // var falsey = ImmutableSet.of("null", // json null
    //     "false", // json primitive bool
    //     "0", "-0", "0.0", "-0.0", // json primitive number
    //     "'false'", "''", "'0'", "'-0'", "'0.0'", "'-0.0'"); // json primitive string

    for (String e : allFalsey) {
      for (Entry<String, String> entry : ImmutableMap.of("%s", "e", "[%s]", "e[0]", "{e:%s}", "e.e").entrySet()) {
        String fmt = entry.getKey();
        String str = entry.getValue();
        assertThat(bool(json(String.format(fmt, e)), str)).as("jsonElement=%s fmt=%s str=%s", e, fmt, str).isEqualTo(false);
      }
    }

  }

  @Test
  public void nullToBoolTest() {

    assertThat(bool(json("{}"), "e?.test")).isEqualTo(false);
    assertThat(bool(json("{test:'true'}"), "e?.test")).isEqualTo(true);

    // assertThat(bool(json("{test:'false'}"), "e?.test")).isEqualTo(false);

    assertThat(bool(json("{test:''}"), "e?.test")).isEqualTo(false);

    assertThat(bool(json("{test:'yeah'}"), "e?.test")).isEqualTo(true);
    assertThat(bool(json("{test:'yeah'}"), "e?.test!=null")).isEqualTo(true);

  }

  @Test
  public void testTest() {

    assertThat(bool(json("{}"), "e.test")).isEqualTo(false);

    for (String e : allFalsey)
      assertThat(bool(json(String.format("{test:%s}", e)), "e.test")).isEqualTo(false);

    // assertThat(bool(json("{test:null}"), "e.test")).isEqualTo(false);
    // assertThat(bool(json("{test:0}"), "e.test")).isEqualTo(false);
    // assertThat(bool(json("{test:0.0}"), "e.test")).isEqualTo(false);
    // assertThat(bool(json("{test:''}"), "e.test")).isEqualTo(false);
    // assertThat(bool(json("{test:'0'}"), "e.test")).isEqualTo(false);
    // assertThat(bool(json("{test:'false'}"), "e.test")).isEqualTo(false);

    assertThat(bool(json("{test:'true'}"), "e.test")).isEqualTo(true);
    assertThat(bool(json("{test:'testing123'}"), "e.test")).isEqualTo(true);

  }

  @Test
  public void versionTest() {

    // tricky: JsonNull -> JsonNull
    // assertThat(output(json("null"), "e.version = (e.version?e.version:0) + 1")).isEqualTo(json("{version:1}"));

    // hmm..
    // assertThat(output(json("'abc'"), "e?.version = (e?.version?:0) + 1")).isEqualTo(json("'abc'"));

    assertThat(output(json("{}"), "e.version = e.version??0 + 1")).isEqualTo(json("{version:1}")); // spel equivalent is a syntax error
    assertThat(output(json("{}"), "e.version = (e.version??0) + 1")).isEqualTo(json("{version:1}"));
    assertThat(output(json("{version:1}"), "e.version = e.version + 1")).isEqualTo(json("{version:2}"));

  }

  @Test
  public void randomStringTest() {
    for (int i = 0; i < 10; ++i)
      System.out.println("e="+output(json("null"), "e=randomString(4)"));
  }

  // aka filters
  private boolean bool(JsonElement input, String expressionString) {
    return new ExpressionsJs(input, now).eval(expressionString);
  }
  
  // aka transforms
  private JsonElement output(JsonElement input, String expressionString) {
    ExpressionsJs expressions = new ExpressionsJs(input, now);
    expressions.eval(expressionString);
    return expressions.e();
  }

  private JsonElement json(String json) {
    return new JsonStreamParser(json).next();
  }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }
  
}
