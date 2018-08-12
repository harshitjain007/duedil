package app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import app.schema.Schemas;
import app.transformations.DistinctSongs;
import app.transformations.LongestSessions;
import app.transformations.TopSongs;

public class Driver {
    public static void main(String[] args) {
        //create spark session
        SparkSession spark = SparkSession.builder()
                                         .appName("Music Insights")
                                         .getOrCreate();

        // read csv file
        Dataset<Row> songs = spark.read()
                                  .option("sep","\t")
                                  .option("ignoreLeadingWhiteSpace",true)
                                  .option("ignoreTrailingWhiteSpace",true)
                                  .schema(Schemas.listeningSchema)
                                  .csv(args[0]);

        // calculate number of distinct songs per user
        Dataset<Row> distinctSongsByUser = DistinctSongs.transform(songs);

        // calculate top 100 most played songs
        Dataset<Row> top100Songs = TopSongs.transform(songs, 100);

        // calculate top 10 longest sessions with details
        Dataset<Row> top10Sessions = LongestSessions.transform(songs, 10);

        // write the result into the given locations
        distinctSongsByUser.repartition(1).write().mode("overwrite").option("header",true).csv(args[1]);
        top100Songs.repartition(1).write().mode("overwrite").option("header",true).csv(args[2]);
        top10Sessions.repartition(1).write().mode("overwrite").json(args[3]);

        // stop spark
        spark.stop();
  }
}
