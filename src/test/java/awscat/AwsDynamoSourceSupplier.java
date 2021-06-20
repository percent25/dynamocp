package awscat;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;

import helpers.MoreDynamo;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public class AwsDynamoSourceSupplier implements Supplier<SourceArg> {

  @Override
  public SourceArg get() {
    return new SourceArg() {

      private DynamoDbClient client;
      private final String tableName = UUID.randomUUID().toString();

      @Override
      public void setUp() {
        client = AwsBuilder.create(DynamoDbClient.builder());

        KeySchemaElement keySchemaElement = KeySchemaElement.builder() //
            .keyType(KeyType.HASH) //
            .attributeName("id") //
            .build();

        AttributeDefinition attributeDefinition = AttributeDefinition.builder() //
          .attributeName("id") //
          .attributeType(ScalarAttributeType.S)
          .build();

        CreateTableRequest createRequest = CreateTableRequest.builder() //
            .tableName(tableName) //
            .keySchema(keySchemaElement) //
            .attributeDefinitions(attributeDefinition) //
            .billingMode(BillingMode.PAY_PER_REQUEST) //
            .build();
        log(createRequest);
        CreateTableResponse createResponse = client.createTable(createRequest);
        log(createResponse);

        client.waiter().waitUntilTableExists(b->b.tableName(tableName));
      }

      @Override
      public void load(JsonElement jsonElement) {
        Map<String, AttributeValue> item = MoreDynamo.render(jsonElement);
        PutItemRequest putItemRequest = PutItemRequest.builder() //
            .tableName(tableName) //
            .item(item) //
            .build();
        log(putItemRequest);
        PutItemResponse putItemResponse = client.putItem(putItemRequest);
        log(putItemResponse);
      }

      @Override
      public String sourceArg() {
        return String.format("dynamo:%s,limit=1", tableName);
      }

      @Override
      public void tearDown() {
        DeleteTableRequest deleteRequest = DeleteTableRequest.builder() //
            .tableName(tableName) //
            .build();
        log(deleteRequest);
        DeleteTableResponse deleteResponse = client.deleteTable(deleteRequest);
        log(deleteResponse);
      }

      private void log(Object arg) {
        System.out.println(getClass().getSimpleName() + arg);
      }
    };
  }

  public static void main(String... args) throws Exception {
    SourceArg source = new AwsDynamoSourceSupplier().get();
    source.setUp();
    try {
      source.load(jsonElement("{id:{s:abc123}}"));
      System.out.println(source.sourceArg());
    } finally {
      source.tearDown();
    }
  }

  static JsonElement jsonElement(String json) {
    return new JsonStreamParser(json).next();
  }

}
