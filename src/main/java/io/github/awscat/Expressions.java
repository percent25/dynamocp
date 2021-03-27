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
    context = new StandardEvaluationContext(rootObject);
    parser = new SpelExpressionParser();

    rootObject.e = ObjectHelper.toObject(e);

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

  private void trace(Object... args) {
    new LogHelper(this).trace(args);
  }

}
