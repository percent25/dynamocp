package helpers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.gson.*;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

// NULL java null
// BOOL java boolean

// B byte[]
// N java.lang.Number
// S java.lang String

// BS java.util.Set<byte[]> // unique, unordered
// NS java.util.Set<Number> // unique, unordered
// SS java.util.Set<String> // unique, unordered

// M map lava.util.Map<String, Object> // unordered

// L list java.util.List<Object> // ordered, duplicates

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
public class DynamoHelper {

  private static ObjectMapper objectMapper = new ObjectMapper();

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
        size += 19 + 1;

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

  // https://aws.amazon.com/blogs/developer/aws-sdk-for-java-2-0-developer-preview/
  public static JsonElement parse(Map<String, AttributeValue> item) {
    try {
      return new Gson().fromJson(objectMapper.writeValueAsString(AttributeValue.builder().m(item)), JsonElement.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    // return new Gson().toJsonTree(Maps.transformValues(item, value -> {
    //   try {
    //     return new Gson().fromJson(objectMapper.writeValueAsString(value.toBuilder()), JsonElement.class);
    //   } catch (Exception e) {
    //     throw new RuntimeException(e);
    //   }
    // }));
  }

  // https://aws.amazon.com/blogs/developer/aws-sdk-for-java-2-0-developer-preview/
  public static Map<String, AttributeValue> render(JsonElement dynamoJson) {
    try {
      Map<String, AttributeValue> item = new LinkedHashMap<String, AttributeValue>();
      for (Entry<String, JsonElement> entry : dynamoJson.getAsJsonObject().entrySet()) {
        String key = entry.getKey();
        JsonElement value = entry.getValue();
        item.put(key, objectMapper.readValue(value.toString(), AttributeValue.serializableBuilderClass()).build());
      }
      return item;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
  public static void main(String... args) throws Exception {
    System.out.println(render(new JsonStreamParser("{   id:{s:1}, mylist:{ l: [{s:1},{s:1},{s:1}] }   }").next()));
  }

}
