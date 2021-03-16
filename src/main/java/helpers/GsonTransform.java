package helpers;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class GsonTransform {

  private final Map<String, Object> transformExpressions;
  private final Class<?> classOfUtils;

  public GsonTransform(JsonObject transformExpressions, Class<?> classOfUtils) {
    this.transformExpressions = MoreGson.toMap(transformExpressions);
    this.classOfUtils = classOfUtils;
  }

  public JsonElement transform(JsonElement in) {
    // context
    StandardEvaluationContext context = new StandardEvaluationContext(MoreGson.toObject(in));
    context.addPropertyAccessor(new MapAccessor());
    for (Method method : classOfUtils.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()))
        context.setVariable(method.getName(), method);
    }
    // transform
    ExpressionParser parser = new SpelExpressionParser();
    for (Entry<String, Object> entry : transformExpressions.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (value instanceof String)
        value = parser.parseExpression(value.toString(), new TemplateParserContext()).getValue(context);
      parser.parseExpression(key).setValue(context, value);
    }
    return new Gson().toJsonTree(context.getRootObject().getValue()).getAsJsonObject();
  }

  static class Utils {
    public static String uuid() {
      return UUID.randomUUID().toString();
    }
  };

  public static void main(String... args) throws Exception {
    JsonObject input = new Gson().fromJson("{a:1.1,b:2,id:{s:'abc'}}", JsonObject.class);
    System.out.println("" + input);
    JsonObject transform = new Gson().fromJson("{c:'#{ 2.2*a }','id.s':'#{ #uuid() }'}", JsonObject.class);
    var out = new GsonTransform(transform, Utils.class).transform(input);
    System.out.println("" + out);
  }

}