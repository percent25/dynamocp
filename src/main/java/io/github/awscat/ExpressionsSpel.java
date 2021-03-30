package io.github.awscat;

import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;

import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterRegistry;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;

import helpers.LogHelper;
import helpers.ObjectHelper;

/**
 * "In JavaScript, a truthy value is a value that is considered true when
 * encountered in a Boolean context. All values are truthy unless they are
 * defined as falsy (i.e., except for false, 0, -0, 0n, "", null, undefined, and
 * NaN).""
 * 
 * https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions
 */
public class ExpressionsSpel {

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
    // returns a random string w/fixed length len
    public String fixedString(int len) {
      byte[] bytes = new byte[(3 * len + 3) / 4];
      new SecureRandom().nextBytes(bytes);
      String randomString = BaseEncoding.base64().encode(bytes).substring(0);
      return randomString.substring(0, Math.min(len, randomString.length()));
    }
    // returns a random string w/random length [1..len]
    public String randomString(int len) {
      byte[] bytes = new byte[new SecureRandom().nextInt((3 * len + 3) / 4) + 1];
      new SecureRandom().nextBytes(bytes);
      String randomString = BaseEncoding.base64().encode(bytes).substring(0);
      return randomString.substring(0, Math.min(len, randomString.length()));
    }
    public String toString() {
      return new Gson().toJson(this);
    }
  }
  
  private final RootObject rootObject;
  private final StandardEvaluationContext context;
  private final ExpressionParser parser;

  public ExpressionsSpel(JsonElement e) {
    this(e, Instant.now().toString());
  }
  
  public ExpressionsSpel(JsonElement e, String now) {
    rootObject = new RootObject(now);
    context = new StandardEvaluationContext(rootObject);
    parser = new SpelExpressionParser();

    rootObject.e = ObjectHelper.toObject(e);

    context.addPropertyAccessor(new SafeNavigationMapAccessor());

    ConfigurableConversionService conversionService = new DefaultConversionService() {
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
          return BigDecimal.ZERO.compareTo(new BigDecimal(source)) != 0;
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

  }

  public JsonElement e() {
    return new Gson().toJsonTree(rootObject.e);
  }

  public boolean eval(String expressionString) {
    Boolean value = parser.parseExpression(expressionString).getValue(context, Boolean.class);
    trace("bool", expressionString, value);
    // ### falsey
    if (value == null)
      return false;
    // ### falsey
    return value;
  }

  // public Object eval(String expressionString) {
  //   Object value = parser.parseExpression(expressionString).getValue(context);
  //   trace("eval", expressionString, value);
  //   return value;
  // }

  private void trace(Object... args) {
    new LogHelper(this).trace(args);
  }

  public static void addGsonConverters(ConverterRegistry converterRegistry) {
    converterRegistry.addConverter(new Converter<JsonElement, Object>() {
      @Override
      public Object convert(JsonElement source) {
        return ObjectHelper.toObject(source);
      }
    });
    converterRegistry.addConverter(new Converter<Object, JsonElement>() {
      @Override
      public JsonElement convert(Object source) {
        return new Gson().toJsonTree(source);
      }
    });
  }

}
