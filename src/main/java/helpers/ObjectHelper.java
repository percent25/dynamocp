package helpers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class ObjectHelper {

  // general
  public static Object toObject(JsonElement jsonElement) {
    if (jsonElement.isJsonArray()) {
      List<Object> list = new ArrayList<>();
      for (var element : jsonElement.getAsJsonArray())
        list.add(toObject(element));
      return list;
    }
    if (jsonElement.isJsonObject()) {
      Map<String, Object> object = new LinkedHashMap<>();
      for (var entry : jsonElement.getAsJsonObject().entrySet())
        object.put(entry.getKey(), toObject(entry.getValue()));
      return object;
    }
    Object object = new Gson().fromJson(jsonElement, Object.class);
    if (jsonElement.isJsonPrimitive()) {
      if (jsonElement.getAsJsonPrimitive().isNumber())
        object = new BigDecimal(jsonElement.toString());
    }
    return object;
  }

  public static void main(String... args) {
    // round-trip json number thru Object: 1 -> 1.0
    Object object = new Gson().fromJson("1", Object.class);
    System.out.println(object); // 1.0
    System.out.println(object.getClass()); // java.lang.Double
    System.out.println(new Gson().toJson(object)); // 1.0

    // round-trip json number thru Number: 1 -> {"value":"1"}
    Number number = new Gson().fromJson("1", Number.class);
    System.out.println(number); // 1
    System.out.println(number.getClass()); // LazilyParsedNumber
    System.out.println(new Gson().toJson(number)); // {"value":"1"}
  }

}
