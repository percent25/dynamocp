package awscat;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import helpers.MoreDynamo;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public class AwsDynamoSourceSupplier implements Supplier<SourceArg> {

  @Override
  public SourceArg get() {
    return new SourceArg() {

      private String endpointUrl;
      private DynamoDbClient client;

      private final String tableName = UUID.randomUUID().toString();

      @Override
      public void setUp() {
        endpointUrl = String.format("http://localhost:%s", System.getProperty("edge.port", "4566"));

        client = DynamoDbClient.builder() //
            // .httpClient(AwsCrtAsyncHttpClient.create()) //
            .endpointOverride(URI.create(endpointUrl)) //
            .region(Region.US_EAST_1) //
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))) // https://github.com/localstack/localstack/blob/master/README.md#setting-up-local-region-and-credentials-to-run-localstack
            .build();

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
        return String.format("dynamo:%s,endpoint=%s,limit=1", tableName, endpointUrl);
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
    source.load(new JsonObject());
  }

}
