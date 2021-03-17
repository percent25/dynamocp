package helpers;

import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class MuchDynamo {

  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
  public static int itemSize(Map<String, AttributeValue> item) {
    int size = 0;
    for (var entry : item.entrySet()) {
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
