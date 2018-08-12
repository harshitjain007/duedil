package app.transformations;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import app.schema.Schemas;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class LongestSessions {

    private static FlatMapFunction<Row,Row> sessionCreator = new FlatMapFunction<Row, Row>(){
        private Row createRow(String uid, Timestamp t1, Timestamp t2, StringBuilder songs){
            return RowFactory.create(uid, t1, t2, t2.getTime()-t1.getTime(), songs.substring(0, songs.length()-1));
        }

        @Override
        public Iterator<Row> call(Row row) throws Exception {
            // fetch the fields
            String userId = row.getString(0);
            List<Row> list = row.getList(1);

            // sort the tuple list according to the timestamp
            ArrayList<Row> songList = new ArrayList<>(list);
            songList.sort(Comparator.comparing(o -> o.getTimestamp(0)));

            // create sessions
            Row first = songList.get(0);
            long twentyMin = 20*60*1000;
            Timestamp start = first.getTimestamp(0), end = first.getTimestamp(0);
            StringBuilder sList = new StringBuilder(first.getString(1)+"\t");

            List<Row> sessions = new ArrayList<>();
            for(int i=1;i<songList.size();++i){
                Row song = songList.get(i);
                Timestamp songTs = song.getTimestamp(0);

                if (songTs.getTime()-end.getTime() < twentyMin){
                    end = songTs;
                    sList.append(song.getString(1)).append("\t");
                } else {
                    sessions.add(createRow(userId, start, end, sList));
                    start = song.getTimestamp(0);
                    end = song.getTimestamp(0);
                    sList = new StringBuilder(song.getString(1)+"\t");
                }
            }
            sessions.add(createRow(userId, start, end, sList));

            return sessions.iterator();
        }
    };

    private static MapFunction<Row,Row> strToList = (MapFunction<Row, Row>) row -> {
        String[] songs = row.getString(4).split("\t");
        return RowFactory.create(row.getString(0),row.getTimestamp(1),row.getTimestamp(2), songs);
    };

    public static Dataset<Row> transform(Dataset<Row> dataset, int n){
        Dataset<Row> df = dataset.select(
                dataset.col("user_id"),
                functions.struct(dataset.col("timestamp"), dataset.col("track_name")).alias("ts_track")
            );

        Dataset<Row> df2 = df.groupBy("user_id").agg(functions.collect_list("ts_track"));
        Dataset<Row> df3 = df2.flatMap(sessionCreator, RowEncoder.apply(Schemas.userSessionSchema));
        Dataset<Row> df4 = df3.sort(df3.col("duration").desc(), df3.col("start")).limit(n);
        Dataset<Row> df5 = df4.map(strToList, RowEncoder.apply(Schemas.userSessionSchema2));
        return df5;
    }
}
