import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import scala.Tuple2;

public class SparkMovielensGenresAvg {

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setMaster("local").setAppName("Movielens");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");

    //copies the file to all the machines
    sc.addFile("movielens/ml-latest-small/movies.csv");

    Map<String, String[]> movieGenres = new HashMap<>();

    String localPath = SparkFiles.get("movies.csv");
    System.out.println(localPath);

    BufferedReader in = new BufferedReader(new FileReader(new File(localPath)));

    //TODO: fill in movieGenres

    /**
     * Create a hashtable with key = movie ID, value = movie genre (same ID)
     * This will be used in the following step as we need to convert movie ID to movie genre (there as stroed in two different files)
     */

    String read_line = in.readLine();
    while ((read_line = in.readLine()) != null) {
      String[] lineArr = read_line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      String movieID = lineArr[0];
      String[] genres = lineArr[2].split("\\|");

      movieGenres.put(movieID, genres);
    }
    in.close();

    JavaRDD<String> mRDD = sc.textFile("input_movielens"); //directory where the files are

    JavaPairRDD<String, Tuple2<Double, Double>> mPairRDD = mRDD.flatMapToPair(line -> {
      List< //we return a list of K,V pairs (Tuple2) from flatMapToPair
          Tuple2<String, //movieID or userID (key)
              Tuple2<Double, Double> //rating, 1.0 (value)
      >> l = new ArrayList<>(); // l -> ArrayList

      //check to see if this is the header line
      if (line.contains("userId"))
        return l.iterator(); //return empty list

      String[] lineArr = line.split(",");
      String movieID = lineArr[1];
      Double rating = Double.parseDouble(lineArr[2]);
      String[] genres = movieGenres.get(movieID);
      for (String genre : genres) {
        l.add(new Tuple2<>(genre, new Tuple2<>(rating, 1.0)));
      }
      return l.iterator();
    }).aggregateByKey(new Tuple2<Double, Double>(0.0, 0.0), (acc, value) -> new Tuple2<>(acc._1 + value._1, acc._2 + 1),
        (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2)); //TODO = ...

    JavaPairRDD<String, Double> avgPairRDD = mPairRDD.mapToPair(x->new Tuple2<>(x._1, x._2._1/x._2._2)); //TODO = ...

    //remove output directory if already there
    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
    fs.delete(new Path("output_movielens"), true); // delete dir, true for recursive
    avgPairRDD.saveAsTextFile("output_movielens");

    System.out.println("Done. See result in 'output_movielens'");

    sc.stop();
    sc.close();
  }
}