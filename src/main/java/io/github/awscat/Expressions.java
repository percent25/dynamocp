package io.github.awscat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.springframework.context.expression.MapAccessor;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.TypeConverter;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import helpers.LogHelper;
import helpers.ObjectHelper;

// class ObjectToBooleanConverter implements Converter<Object, Boolean> {

//   @Override
//   public Boolean convert(Object source) {
//     return source != null;
//   }

// }

// public interface ConverterFactory<S, R> {

// 	/**
// 	 * Get the converter to convert from S to target type T, where T is also an instance of R.
// 	 * @param <T> the target type
// 	 * @param targetType the target type to convert to
// 	 * @return a converter from S to T
// 	 */
// 	<T extends R> Converter<S, T> getConverter(Class<T> targetType);

// }


    // final class ObjectToBooleanConverterFactory implements ConverterFactory<Object, Boolean> {
    //   @Override
    //   public <T extends Boolean> Converter<Object, T> getConverter(Class<T> targetType) {
    //     return new ObjectToBoolean();
    //   }
    //   private static final class ObjectToBoolean<T extends Boolean> implements Converter<Object, T> {
    //     @Override
    //     @Nullable
    //     public T convert(Object source) {
    //       return Boolean.valueOf(source != null);
    //     }
    //   }
    // }


// https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions
public class Expressions {

  class RootObject {
    public Object e;
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
    public String randomString(int len) {
      byte[] bytes = new byte[new SecureRandom().nextInt(len/2)+len/2];
      new SecureRandom().nextBytes(bytes);
      return BaseEncoding.base64().encode(bytes);
    }
    public String toString() {
      return new Gson().toJson(this);
    }
  }
  
  private final RootObject rootObject;
  private final StandardEvaluationContext context;
  private final ExpressionParser parser;

  public Expressions(JsonElement e) {
    this(e, Instant.now().toString());
  }
  
  public Expressions(JsonElement e, String now) {
    rootObject = new RootObject(now);
    rootObject.e = ObjectHelper.toObject(e);
    context = new StandardEvaluationContext(rootObject);
    context.addPropertyAccessor(new SafeNavigationMapAccessor());

    var conversionService = new DefaultConversionService() {
      @Override
      protected Object convertNullSource(TypeDescriptor sourceType, TypeDescriptor targetType) {
        if (targetType.getObjectType() == Boolean.class)
          return false;
        return null;
      }
    };

    context.setTypeConverter(new StandardTypeConverter(conversionService));

    // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
    conversionService.addConverter(new Converter<Boolean, Boolean>() {
      @Override
      public Boolean convert(Boolean source) {
        return source;
      }
    });
    
    // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
    conversionService.addConverter(new Converter<Number, Boolean>() {
      @Override
      public Boolean convert(Number source) {
        return BigDecimal.ZERO.compareTo(new BigDecimal(source.toString())) != 0;
      }
    });

    // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
    conversionService.addConverter(new Converter<String, Boolean>() {
      @Override
      public Boolean convert(String source) {
        if ("".equals(source))
          return false;
        if ("false".equals(source))
          return false;
        try {
          return BigDecimal.ZERO.compareTo(new BigDecimal(source.toString())) != 0;
        } catch (Exception e) {
          // dont-care
        }
        return true;
      }
    });

    // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
    conversionService.addConverter(new Converter<Collection<?>, Boolean>() {
      @Override
      public Boolean convert(Collection<?> source) {
        return true;
      }
    });

    // https://developer.mozilla.org/en-US/docs/Glossary/Truthy
    conversionService.addConverter(new Converter<Object, Boolean>() {
      @Override
      public Boolean convert(Object source) {
        return true;
      }
    });

    parser = new SpelExpressionParser();
  }

  public JsonElement e() {
    return new Gson().toJsonTree(rootObject.e);
  }

  public boolean bool(String expressionString) {
    var value = parser.parseExpression(expressionString).getValue(context, Boolean.class);
    trace("bool", expressionString, value);
    // ### falsey
    if (value == null)
      return false;
    // ### falsey
    return value;
  }

  public Object eval(String expressionString) {
    var value = parser.parseExpression(expressionString).getValue(context);
    trace("eval", expressionString, value);
    return value;
  }

  public static void main(String... args) {

        // e = new Expressions(e).apply("e.id=222");
    // value = new Expressions(e).value("e?.id?.s?.length()>0");

    var expressions = new Expressions(new JsonObject());

    System.out.println(expressions.eval("e?.id?.s"));
    System.out.println(expressions.eval("e.now=now()"));
    System.out.println(expressions.e());

    // var theRoot = new RootObject();
    // theRoot.e = ObjectHelper.toObject(new Gson().fromJson("{id:{s:'abc'},test:'yes'}", JsonElement.class));
    // StandardEvaluationContext context = new StandardEvaluationContext(theRoot);
    // context.addPropertyAccessor(new SafeMapAccessor());
    //###TODO DefaultConversionService extends GenericConversionService ?!?

    // --expression="id.s=#uuid()"
    // final ExpressionParser parser = new SpelExpressionParser();

    // System.out.println(parser.parseExpression("e").getValue(context));
    // System.out.println(parser.parseExpression("now()").getValue(context));
    // System.out.println(parser.parseExpression("uuid()").getValue(context));
    // System.out.println(parser.parseExpression("e.id?.s?.length").getValue(context));
    // System.out.println(parser.parseExpression("e.id?.s?.length>0").getValue(context));
    // System.out.println(" ### "+parser.parseExpression("e.idz!=null").getValue(context, boolean.class));

    // // System.out.println(parser.parseExpression("e.aaa={:}").getValue(context));
    // // System.out.println(parser.parseExpression("e.aaa.bbb=222").getValue(context));

    // System.out.println(parser.parseExpression("e.id.s='123'").getValue(context));
    // // System.out.println(parser.parseExpression("value={s:id.s.length}").getValue(context));
    // // System.out.println(parser.parseExpression("#this = 5.0").getValue(context));

    // System.out.println("e=" + theRoot.e);
  }

  private void trace(Object... args) {
    new LogHelper(this).trace(args);
  }

}
