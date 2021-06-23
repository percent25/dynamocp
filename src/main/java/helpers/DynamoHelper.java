package helpers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoHelper {

  private static ObjectMapper objectMapper = new ObjectMapper();

  public static JsonElement parse(Map<String, AttributeValue> item) {
    JsonElement jsonElement = new Gson().toJsonTree(Maps.transformValues(item, value -> {
      try {
        return new Gson().fromJson(objectMapper.writeValueAsString(value.toBuilder()), JsonElement.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }));
    return jsonElement;
  }

  public static Map<String, AttributeValue> render(JsonElement jsonElement) {
    try {
      Map<String, AttributeValue> item = new LinkedHashMap<String, AttributeValue>();
      for (Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
        String key = entry.getKey();
        JsonElement value = entry.getValue();
        AttributeValue attributeValue = objectMapper.readValue(value.toString(), AttributeValue.serializableBuilderClass()).build();
        item.put(key, attributeValue);
      }
      return item;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
  public static int itemSize(Map<String, AttributeValue> item) {
    int size = 0;
    for (Entry<String, AttributeValue> entry : item.entrySet()) {
      String attributeName = entry.getKey();
      AttributeValue attributeValue = entry.getValue();

      size += attributeName.length();

      // strings
      if (attributeValue.s() != null)
        size += attributeValue.s().length();

      // numbers
      if (attributeValue.n() != null)
        size += 18;

      // binary
      // TODO

      // null
      if (attributeValue.nul() != null)
        size += 1;

      // boolean
      if (attributeValue.bool() != null)
        size += 1;

      // list or map
      // TODO

    }
    return size;
  }

}
