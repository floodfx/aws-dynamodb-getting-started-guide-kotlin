package floodfx

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.google.gson.GsonBuilder
import com.google.common.io.Files
import java.io.File
import com.amazonaws.services.dynamodbv2.document.Item
import com.google.gson.reflect.TypeToken
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.utils.NameMap

// data class for parsing to/fro json
data class MovieItem(
        val year: Int? = null,
        val title: String? = null,
        val info: Info? = null
) {
  data class Info(
          val directors: List<String>? = null,
          val release_date: String? = null,
          val rating: Double? = null,
          val genres: List<String>? = null,
          val image_url: String? = null,
          val plot: String? = null,
          val rank: Int? = null,
          val running_time_secs: Long? = null,
          val actors: List<String>? = null
  )
}

class LearnDynamo {

  companion object {
    const val TABLE_NAME = "Movies"
    const val DEFAULT_MOVIE_YEAR = 2015
    const val DEFAULT_MOVIE_TITLE = "The Big New Movie"
    
    @JvmStatic fun main(args: Array<String>) {
      val ld = LearnDynamo()
      
      // call methods defined in LearnDynamo class 
      ld.getItem()    
    }
  }

  val dynamoDbClient = AmazonDynamoDBClientBuilder
          .standard()
          .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
          .build()

  val dynamoDb = DynamoDB(dynamoDbClient)

  val gson = GsonBuilder().create()

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

  fun loadData() {

    val table = dynamoDb.getTable(TABLE_NAME)

    val data = Files.toString(File("moviedata.json"), Charsets.UTF_8)

    val collectionType = object : TypeToken<Collection<MovieItem>>() {}.getType()
    val list = gson.fromJson<Collection<MovieItem>>(data, collectionType)

    list.forEach {
      val item = Item()
              .withPrimaryKey("year", it.year, "title", it.title)
              .withJSON("info", gson.toJson(it.info))
      table.putItem(item)
      println("Loaded: ${it}")
    }

    println("Loaded Data")

  }

  fun createNewItem() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val movieItem = MovieItem(
            year = DEFAULT_MOVIE_YEAR,
            title = DEFAULT_MOVIE_TITLE,
            info = MovieItem.Info(
                    plot = "Nothing happens at all.",
                    rating = 0.0
            )
    )

    table.putItem(Item()
            .withPrimaryKey("year", movieItem.year, "title", movieItem.title)
            .withJSON("info", gson.toJson(movieItem.info))
    )
    println("Created Item")
  }

  fun getItem() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val spec = GetItemSpec()
            .withPrimaryKey("year", DEFAULT_MOVIE_YEAR, "title", DEFAULT_MOVIE_TITLE)
    val outcome = table.getItem(spec)

    println("Got Item: ${outcome.toJSONPretty()}")
  }

  fun updateItem() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val spec = UpdateItemSpec()
            .withPrimaryKey("year", DEFAULT_MOVIE_YEAR, "title", DEFAULT_MOVIE_TITLE)
            .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
            .withValueMap(
                    ValueMap()
                            .withNumber(":r", 5.5)
                            .withString(":p", "Everything happens all at once.")
                            .withList(":a", listOf("Larry", "Moe", "Curly"))
            )
            .withReturnValues(ReturnValue.UPDATED_NEW)
    val outcome = table.updateItem(spec)
    println("Updated Item: ${outcome.item.toJSONPretty()}")
  }

  fun atomicIncrement() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val spec = UpdateItemSpec()
            .withPrimaryKey("year", DEFAULT_MOVIE_YEAR, "title", DEFAULT_MOVIE_TITLE)
            .withUpdateExpression("set info.rating = info.rating + :val")
            .withValueMap(ValueMap().withNumber(":val", 0.3))
            .withReturnValues(ReturnValue.UPDATED_NEW)
    val outcome = table.updateItem(spec)
    println("Atomically Updated Item: ${outcome.item.toJSONPretty()}")
  }

  fun conditionalUpdate() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val spec = UpdateItemSpec()
            .withPrimaryKey("year", DEFAULT_MOVIE_YEAR, "title", DEFAULT_MOVIE_TITLE)
            .withUpdateExpression("remove info.actors[0]")
            .withConditionExpression("size(info.actors) > :num")
            .withValueMap(ValueMap().withNumber(":num", 3))
            .withReturnValues(ReturnValue.UPDATED_NEW)
    val outcome = table.updateItem(spec)
    println("Conditionally Updated Item: ${outcome.item.toJSONPretty()}")
  }

  fun conditionalDeleteItem() {
    val table = dynamoDb.getTable(TABLE_NAME)

    val spec = DeleteItemSpec()
            .withPrimaryKey("year", DEFAULT_MOVIE_YEAR, "title", DEFAULT_MOVIE_TITLE)
            .withConditionExpression("info.rating <= :val")
            .withValueMap(ValueMap().withNumber(":val", 5.0))
    table.deleteItem(spec)
    println("Deleted")
  }

  fun queryTable() {
    val table = dynamoDb.getTable(TABLE_NAME)
    val nameMap = mapOf("#yr" to "year")
    val valueMap = mapOf(":yyyy" to 1985)
    val spec = QuerySpec()
            .withKeyConditionExpression("#yr = :yyyy")
            .withNameMap(nameMap)
            .withValueMap(valueMap)

    val printMovies: (item: Item) -> Unit = {
      println("${it.getString("title")} (${it.getInt("year")})")
    }

    println("Movies from 1985:")
    table.query(spec).forEach(printMovies)

    val valuesMap = valueMap + mapOf(
            ":yyyy" to 1992,
            ":letter1" to "A",
            ":letter2" to "L"
    )
    spec.withProjectionExpression("#yr, title, info.genres, info.actors[0]")
            .withKeyConditionExpression("#yr = :yyyy and title between :letter1 and :letter2")
            .withNameMap(nameMap)
            .withValueMap(valuesMap)

    println("\nMovies from 1992:")
    table.query(spec).forEach(printMovies)
  }

  fun scanTable() {
    val table = dynamoDb.getTable(TABLE_NAME)
    val nameMap = mapOf("#yr" to "year")
    val valueMap = mapOf(
            ":start_yr" to 1950,
            ":end_yr" to 1959
    )
    val spec = ScanSpec()
            .withProjectionExpression("#yr, title, info.rating")
            .withFilterExpression("#yr between :start_yr and :end_yr")
            .withNameMap(nameMap)
            .withValueMap(valueMap)

    val printMovies: (index: Int, item: Item) -> Unit = { index, it ->
      println("${index + 1}.\t${it.getString("title")} (${it.getInt("year")})")
    }

    println("Movies from 1950-59:")
    table.scan(spec).forEachIndexed(printMovies)
  }

  fun deleteTable() {
    val table = dynamoDb.getTable(TABLE_NAME)
    table.delete()
    table.waitForDelete()
    println("Deleted Table")
  }

}

