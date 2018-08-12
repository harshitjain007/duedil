package app.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DistinctSongs {

    public static Dataset<Row> transform(Dataset<Row> dataset){
        return dataset.groupBy(dataset.col("user_id"))
                .agg(functions.countDistinct(dataset.col("track_name")))
                .orderBy(dataset.col("user_id"));
    }
}
