# aws-dynamodb-getting-started-guide-kotlin
I ported the AWS Getting Started Guide [Java Code Examples](http://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Java.01.html) to Kotlin

## PRs Welcome
Feel free to send PRs with more idomatic Kotlin as I am not (yet!) a Kotlin expert...

## Example Code
So you get a feel for the code, here is the Java code for Creating a Table:
```java
public class MoviesCreateTable {

    public static void main(String[] args) throws Exception {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "Movies";

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition
                                                                          // key
                    new KeySchemaElement("title", KeyType.RANGE)), // Sort key
                Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
                    new AttributeDefinition("title", ScalarAttributeType.S)),
                new ProvisionedThroughput(10L, 10L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }

    }
}
```

And here is Kotlin Code
```kotlin
val dynamoDbClient = AmazonDynamoDBClientBuilder
          .standard()
          .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
          .build()

  val dynamoDb = DynamoDB(dynamoDbClient)

  fun createTable() {

    val table = dynamoDb.createTable(
            TABLE_NAME,
            listOf(
                    KeySchemaElement("year", KeyType.HASH),
                    KeySchemaElement("title", KeyType.RANGE)
            ),
            listOf(
                    AttributeDefinition("year", ScalarAttributeType.N),
                    AttributeDefinition("title", ScalarAttributeType.S)
            ),
            ProvisionedThroughput(10L, 10L)
    )

    table.waitForActive()
    println("Created Table")
  }
```


