package io.github.awscat;

import java.util.Map;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions-operator-safe-navigation
 */
public class SafeNavigationMapAccessor extends MapAccessor {

  @Override
  public boolean canRead(EvaluationContext context, @Nullable Object target, String name) throws AccessException {
    return (target instanceof Map);
    // return (target instanceof Map && ((Map<?, ?>) target).containsKey(name));
  }

  @Override
  public TypedValue read(EvaluationContext context, @Nullable Object target, String name) throws AccessException {
    Assert.state(target instanceof Map, "Target must be of type Map");
    Map<?, ?> map = (Map<?, ?>) target;
    Object value = map.get(name);
    // if (value == null && !map.containsKey(name)) {
    // throw new MapAccessException(name);
    // }
    return new TypedValue(value);
  }

}
