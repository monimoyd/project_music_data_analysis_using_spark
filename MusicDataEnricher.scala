package final_project

import org.apache.spark._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.HashMap


//class MusicDataEnricher(context: SparkContext, allDataFrameParam: DataFrame, stndIdGeoCdMapParam:HashMap[String, String], broadcastSongArtistMapParam:Broadcast[Map[String, String]], broadcastUserArtistMapParam: Broadcast[Map[String, String]], broadcastUserSubscriptionParam: Broadcast[Map[String, (Long, Long)]]) extends Serializable {
	
//val sc: SparkContext = context
//class MusicDataEnricher( allDataFrameParam: DataFrame, stndIdGeoCdMapParam:HashMap[String, String], broadcastSongArtistMapParam:Broadcast[Map[String, String]], broadcastUserArtistMapParam: Broadcast[Map[String, String]], broadcastUserSubscriptionParam: Broadcast[Map[String, (Long, Long)]]) extends Serializable {
class MusicDataEnricher( allDataFrameParam: DataFrame, broadcastStndIdGeoCdMapParam:Broadcast[Map[String, String]], broadcastSongArtistMapParam:Broadcast[Map[String, String]], broadcastUserArtistMapParam: Broadcast[Map[String, String]], broadcastUserSubscriptionParam: Broadcast[Map[String, (Long, Long)]]) extends Serializable {
	val allDataFrame: DataFrame =allDataFrameParam
 // val stndIdGeoCdMap:HashMap[String, String] = stndIdGeoCdMapParam
	val broadcastStndIdGeoCdMap:Broadcast[Map[String, String]] = broadcastStndIdGeoCdMapParam
	val broadcastSongArtistMap:Broadcast[Map[String, String]] = broadcastSongArtistMapParam
	val broadcastUserArtistMap:Broadcast[Map[String, String]] = broadcastUserArtistMapParam
	val broadcastUserSubscription:Broadcast[Map[String, (Long, Long)]] = broadcastUserSubscriptionParam

	def fillNullValueGeoCd = udf((stationId: String, geoCd: String) => {
	                       
		if (!geoCd.equals("")) geoCd
		else {
		  //  val geoCdVal = stndIdGeoCdMap.get(stationId).getOrElse("Invalid")
		  val geoCdVal = broadcastStndIdGeoCdMap.value.get(stationId).getOrElse("Invalid")
			 geoCdVal
		}
	})

def fillNullValueArtistId = udf((songId: String, artistId: String) => {
	if (!artistId.equals("")) artistId
	else {
		val artistIdVal = broadcastSongArtistMap.value.get(songId).getOrElse("Invalid")
    artistIdVal
	}
})


def findFollowers = udf((userId: String, artistId: String) => {
	val artistList = broadcastUserArtistMap.value.get(userId).getOrElse("")
if (artistList.equals("")) "0"
	else {
		var artistArray = artistList.split("&")
				if (artistArray contains artistId) "1"
					else "0"

	}

})

def findSubscribers = udf((userId: String, startTs: Long) => {

	val subscriptionTuple= broadcastUserSubscription.value.get(userId).getOrElse((0L, 0L))
  if (subscriptionTuple._1 ==0 && subscriptionTuple._2 == 0 ) "0"
	else if (startTs >= subscriptionTuple._1  && startTs <=  subscriptionTuple._2) "1"
	else "0"

})

def validateRecords = udf((userId: String, songId: String, modifiedArtistId: String, modifiedGeoCd: String, timestamp:Long, start_ts:Long, end_ts:Long) => {
	if (userId.equals("")) "0"
		else if (songId.equals("")) "0"
			else if (modifiedArtistId.equals("Invalid")) "0"
			  else if (modifiedGeoCd.equals("Invalid")) "0"
				else if (timestamp == 0) "0"
					else if(start_ts == 0) "0"
						else if (end_ts < start_ts) "0"
							else "1"
})



def enrichData():DataFrame = {
	 
 var newDataFrame = allDataFrame.withColumn("modified_Geo_cd",  fillNullValueGeoCd(allDataFrame("Station_id"), allDataFrame("Geo_cd")))
 newDataFrame = newDataFrame.withColumn("modified_Artist_id",  fillNullValueArtistId(newDataFrame("Songs_id"), newDataFrame("Artist_id")))
 newDataFrame = newDataFrame.withColumn("follower",  findFollowers(newDataFrame("User_id"), newDataFrame("modified_Artist_id")))
 newDataFrame = newDataFrame.withColumn("subscribed", findSubscribers(newDataFrame("User_id"), newDataFrame("Start_ts")))
 newDataFrame = newDataFrame.withColumn("isValid", validateRecords (newDataFrame("User_id"), newDataFrame("Songs_id"), newDataFrame("modified_Artist_id"),  newDataFrame("modified_Geo_cd"), newDataFrame("Timestamp"), newDataFrame("Start_ts"), newDataFrame("End_ts")))

 return newDataFrame
}

}