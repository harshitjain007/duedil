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
        SparkSession spark = SparkSession.builder()
                                         .appName("Music Insights")
                                         .getOrCreate();
        Dataset<Row> songs = spark.read()
                                  .option("sep","\t")
                                  .option("ignoreLeadingWhiteSpace",true)
                                  .option("ignoreTrailingWhiteSpace",true)
                                  .schema(Schemas.listeningSchema)
                                  .csv(args[0]);

        Dataset<Row> distinctSongsByUser = DistinctSongs.transform(songs);
        Dataset<Row> top100Songs = TopSongs.transform(songs, 100);
        Dataset<Row> top100Sessions = LongestSessions.transform(songs, 100);

        distinctSongsByUser.repartition(1).write().mode("overwrite").option("header",true).csv(args[1]);
        top100Songs.repartition(1).write().mode("overwrite").option("header",true).csv(args[2]);
        top100Sessions.repartition(1).write().mode("overwrite").json(args[3]);

        spark.stop();
  }
}
