package app.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class TopSongs {

    public static Dataset<Row> transform(Dataset<Row> dataset, int n){
        Dataset<Row> df =  dataset.groupBy(dataset.col("artist_name"), dataset.col("track_name"))
                                    .agg(functions.count(functions.lit(1)).alias("cnt"));

        Dataset<Row> df2 = df.sort(df.col("cnt").desc(), df.col("artist_name"), df.col("track_name"))
                             .limit(n);

        return df2;
    }
}
