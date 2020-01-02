package com.spark.rdd.airports;

import com.spark.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class AirportsInUsa {

    /*  Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
        and output the airport's name and the city's name to out/airports_in_usa.text.

        Each row of the input file contains the following columns:
        Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
        ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

        Sample output:
        "Putnam County Airport", "Greencastle"
        "Dowagiac Municipal Airport", "Dowagiac"
        ...
      */

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("in/airports.text");

        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
                    String[] splits = line.split(Utils.COMMA_DELIMITER);
                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                }
        );
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");
    }
}
