
import com.mongodb.client.MongoDatabase
import java.time.ZoneId
import java.util.Arrays
import java.time.Instant
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.TimeSeriesOptions
import org.bson.Document
import java.time.format.DateTimeFormatter
import java.util.ArrayList
import java.util.concurrent.TimeUnit

class Service : CandlestickManager {

    var database: MongoDatabase? = null

    constructor(hostAddress: String) {
        database = mongoSetup(hostAddress)
    }


    /*
    The Raw mongo aggregation query is uploaded in the root of the repo.
    The aggregation pipeline consists of 4 operations:
    $match : filters the mongo time series collection records based on the isin value provided

    $group : groups the data returned by the match operation into periods, in our case 1 minutes. Here we also compute additional value such as the
    high price and low price for the periods, open price, close price, openTimestamp and closeTimestamp.

    $project : the data resulted from the $group operation is passed into the $project phase,
    where we use operators to compute the final output including all the required fields.

    $sort : sorting is done on the openTimestamp field to sort 1 minute period in ascending order.

    The final result will be a list of candleStick objects of 1 min intervals for the last 30 mins(1800 seconds).
    */


    override fun getCandlesticks(isin: String): List<Candlestick> {
        val candlesticks: MutableList<Candlestick> = ArrayList()
        val formatter = DateTimeFormatter.ofPattern( "E MMM dd HH:mm:ss z uuuu" ).withZone(ZoneId.systemDefault())
        val collection = database!!.getCollection("candleStick")
        val result = collection.aggregate(
            Arrays.asList(
                Document(
                    "\$match",
                    Document("isin", isin)
                ),
                Document(
                    "\$group",
                    Document(
                        "_id",
                        Document(
                            "openTimestamp",
                            Document(
                                "\$dateTrunc",
                                Document("date", "\$time")
                                    .append("unit", "minute")
                                    .append("binSize", 1L)
                            )
                        )
                    )
                        .append(
                            "openPrice",
                            Document("\$first", "\$price")
                        )
                        .append(
                            "highPrice",
                            Document("\$max", "\$price")
                        )
                        .append(
                            "lowPrice",
                            Document("\$min", "\$price")
                        )
                        .append(
                            "closePrice",
                            Document("\$last", "\$price")
                        )
                ),
                Document(
                    "\$project",
                    Document("_id", 0L)
                        .append("openTimestamp", "\$_id.openTimestamp")
                        .append("openPrice", "\$openPrice")
                        .append("highPrice", "\$highPrice")
                        .append("lowPrice", "\$lowPrice")
                        .append("closingPrice", "\$closePrice")
                        .append(
                            "closeTimestamp",
                            Document(
                                "\$dateAdd",
                                Document("startDate", "\$_id.openTimestamp")
                                    .append("unit", "minute")
                                    .append("amount", 1L)
                            )
                        )
                ),
                Document(
                    "\$sort",
                    Document("openTimestamp", 1L)
                )
            )
        )
        for (document in result) {

            val openTimestampInstant = Instant.from(formatter.parse(document["openTimestamp"].toString()))
            val closeTimestampInstant = Instant.from(formatter.parse(document["closeTimestamp"].toString()))
            candlesticks.add(
                Candlestick(
                    openTimestampInstant,
                    closeTimestampInstant,
                    document["openPrice"].toString().toDouble(),
                    document["highPrice"].toString().toDouble(),
                    document["lowPrice"].toString().toDouble(),
                    document["closingPrice"].toString().toDouble()
                )
            )
        }
        return candlesticks
    }

    //Instrument data is added or deleted in instrument collection based on the TYPE provided in the event
    fun processInstrumentEvent(instrumentEvent: InstrumentEvent) {
        if (instrumentEvent.type.name == "ADD") {
            saveInstrument(instrumentEvent.data.isin, instrumentEvent.data.description)
        } else if (instrumentEvent.type.name == "DELETE") {
            deleteInstrument(instrumentEvent.data.isin)
        }
    }

    //quote event data is saved in a timeseries collection in mongo which is retained for only
    // 30 mins(1800 secs) based on the time field provided
    fun processQuoteEvent(quoteEvent: QuoteEvent) {
        saveQuote(quoteEvent.data.isin, quoteEvent.data.price)
    }

    // Two collections are created in mongo. One is a timeseries collection which retains records only for the last 30 mins
    // and the other one for Instruments is a normal collection.
    private fun mongoSetup(host: String): MongoDatabase {
        val mongoClient = MongoClient(host, 27017)
        val database = mongoClient.getDatabase("regDB")
        val tsOptions = TimeSeriesOptions("time")
        tsOptions.metaField("isin")
        val collOptions = CreateCollectionOptions().timeSeriesOptions(tsOptions).expireAfter(1800, TimeUnit.SECONDS)
        database.createCollection("candleStick", collOptions)
        database.createCollection("instrument")
        return database
    }

    private fun saveInstrument(isin: String, desc: String) {
        val collection = database!!.getCollection("instrument")
        val document = Document()
        document["isin"] = isin
        document["desc"] = desc
        collection.insertOne(document)
    }

    private fun deleteInstrument(isin: String) {
        val collectionInstrument = database!!.getCollection("instrument")
        val collectionCandle = database!!.getCollection("candleStick")
        val searchQuery = BasicDBObject()
        searchQuery["isin"] = isin
        collectionInstrument.deleteOne(searchQuery);
        collectionCandle.deleteOne(searchQuery)
    }

    private fun saveQuote(isin: String, price: Double) {
        val collection = database!!.getCollection("candleStick")
        val instant = Instant.now()
        val document = Document()
        document["isin"] = isin
        document["price"] = price
        document["time"] = instant
        collection.insertOne(document)
    }


}
