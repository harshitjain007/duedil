package app.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schemas {
    public static StructType listeningSchema = new StructType()
            .add("user_id", DataTypes.StringType, false)
            .add("timestamp", DataTypes.TimestampType, false)
            .add("artist_id", DataTypes.StringType)
            .add("artist_name", DataTypes.StringType)
            .add("track_id", DataTypes.StringType)
            .add("track_name", DataTypes.StringType, false);

    public static StructType userSessionSchema = new StructType()
            .add("user_id", DataTypes.StringType)
            .add("start", DataTypes.TimestampType)
            .add("end", DataTypes.TimestampType)
            .add("duration", DataTypes.LongType)
            .add("song_list", DataTypes.StringType);

    public static StructType userSessionSchema2 = new StructType()
            .add("user_id", DataTypes.StringType)
            .add("start", DataTypes.TimestampType)
            .add("end", DataTypes.TimestampType)
            .add("song_list", DataTypes.createArrayType(DataTypes.StringType));
}
