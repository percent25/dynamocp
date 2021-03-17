package helpers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ObjectHelper {

  // general
  public static Object toObject(JsonElement jsonElement) {
    if (jsonElement.isJsonArray()) {
      List<Object> list = new ArrayList<>();
      jsonElement.getAsJsonArray().forEach(value -> {
        list.add(toObject(value));
      });
      return list;
    }
    if (jsonElement.isJsonObject()) {
      Map<String, Object> object = new LinkedHashMap<>();
      jsonElement.getAsJsonObject().entrySet().forEach(entry -> {
        object.put(entry.getKey(), toObject(entry.getValue()));
      });
      return object;
    }
    Object object = new Gson().fromJson(jsonElement, Object.class);
    if (jsonElement.isJsonPrimitive()) {
      if (jsonElement.getAsJsonPrimitive().isNumber())
        object = new BigDecimal(jsonElement.toString());
    }
    return object;
  }

  // specific
  public static Map<String, Object> toMap(JsonObject jsonObject) {
    return (Map<String, Object>) toObject(jsonObject);
  }

  public static void main(String... args) {
    String json = "1"; 
    
    // round-trip json number thru Object: 1 -> 1.0
    Object object = new Gson().fromJson(json, Object.class);
    System.out.println(object); // 1.0
    System.out.println(object.getClass()); // java.lang.Double
    System.out.println(new Gson().toJson(object)); // 1.0

    // round-trip json number thru Number: 1 -> {"value":"1"}
    Number number = new Gson().fromJson(json, Number.class);
    System.out.println(number); // 1
    System.out.println(number.getClass()); // LazilyParsedNumber
    System.out.println(new Gson().toJson(number)); // {"value":"1"}
  }

}
