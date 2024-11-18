package org.wqbiodata;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.ArrayList;


public final class WaterQualityBiodata {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WaterQualityBiodata <file> <output_dir>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("WaterQualityBiodata").master("local")
                .getOrCreate();

        Dataset<Row> biodata_df = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(args[0]);

        JavaRDD<Row> biodata_rdd_of_rows = biodata_df.toJavaRDD();
        JavaRDD<String> biodata_rdd_of_strings = biodata_rdd_of_rows.map(row -> {
            String rowAsString = row.toString();
            return rowAsString.substring(1, rowAsString.length() - 1);
        });


        JavaPairRDD<String, String> biodata_mapper_result = biodata_rdd_of_strings.mapToPair(row -> {
            String[] key_and_value = row.split(",", 2);
            String value_for_mapper = key_and_value[1];
            value_for_mapper = value_for_mapper.replaceAll(", ", "_ ");
            value_for_mapper = value_for_mapper.replaceAll(",", ",,,,,");
            return new Tuple2<String, String>(key_and_value[0], value_for_mapper);
        });

        // Reducer Logic Here
        JavaPairRDD<String, String> biodata_reducer_result = biodata_mapper_result.reduceByKey((str1, str2) -> {
            return str1 + ";" + str2;
        });

        StructType wqbio_schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("ID", DataTypes.StringType, false),
            DataTypes.createStructField("Start Date", DataTypes.StringType, true),
            DataTypes.createStructField("Water Depth", DataTypes.DoubleType, true),
            DataTypes.createStructField("Water Depth Unit", DataTypes.StringType, true),
            DataTypes.createStructField("U.S. State", DataTypes.StringType, true),
            DataTypes.createStructField("Latitude", DataTypes.DoubleType, true),
            DataTypes.createStructField("Longitude", DataTypes.DoubleType, true),
            DataTypes.createStructField("Algae Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Algae Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Biomass Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Biomass Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Biomass/chlorophyll ratio Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Biomass/chlorophyll ratio Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Chlorophyll Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Chlorophyll Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Cladophora Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Cladophora Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Cyanobacteria Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Cyanobacteria Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Diatoms Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Diatoms Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Macroinvertebrates Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Macroinvertebrates Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Periphyton Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Periphyton Unit", DataTypes.StringType, true),
            DataTypes.createStructField("Phycocyanin Value", DataTypes.DoubleType, true),
            DataTypes.createStructField("Phycocyanin Unit", DataTypes.StringType, true),
        });

        String start_date;
        String id;
        double water_depth_value;
        String water_depth_unit;
        String us_state;
        double latitude;
        double longitude;
        double cv1;
        String cu1;
        int cv1c;
        double cv2;
        String cu2;
        int cv2c;
        double cv3;
        String cu3;
        int cv3c;
        double cv4;
        String cu4;
        int cv4c;
        double cv5;
        String cu5;
        int cv5c;
        double cv6;
        String cu6;
        int cv6c;
        double cv7;
        String cu7;
        int cv7c;
        double cv8;
        String cu8;
        int cv8c;
        double cv9;
        String cu9;
        int cv9c;
        double cv10;
        String cu10;
        int cv10c;

        ArrayList<Row> rows_for_new_csv = new ArrayList<Row>();

        List<Tuple2<String,String>> collected_reducer_results = biodata_reducer_result.collect();
        for (Tuple2<?,?> tuple : collected_reducer_results){
            start_date = "";
            id = "";
            water_depth_value = 0.0;
            water_depth_unit = "";
            us_state = "WA";
            latitude = 0.0;
            longitude = 0.0;
            cv1 = 0.0;
            cu1 = "";
            cv1c = 0;
            cv2 = 0.0;
            cu2 = "";
            cv2c = 0;
            cv3 = 0.0;
            cu3 = "";
            cv3c = 0;
            cv4 = 0.0;
            cu4 = "";
            cv4c = 0;
            cv5 = 0.0;
            cu5 = "";
            cv5c = 0;
            cv6 = 0.0;
            cu6 = "";
            cv6c = 0;
            cv7 = 0.0;
            cu7 = "";
            cv7c = 0;
            cv8 = 0.0;
            cu8 = "";
            cv8c = 0;
            cv9 = 0.0;
            cu9 = "";
            cv9c = 0;
            cv10 = 0.0;
            cu10 = "";
            cv10c = 0;
            //System.out.println(tuple._1() + ": " + tuple._2());
            id = tuple._1().toString();
            String[] values_of_key = tuple._2().toString().split(";");
            for (int s = 0; s < values_of_key.length; s++){
                if (values_of_key[s].length() < 8){
                    break;
                }
                String[] delimited_value = values_of_key[s].split(",,,,,");
                if (delimited_value.length < 8){
                    break;
                }
                start_date = delimited_value[0];
                try {
                    water_depth_value = Double.parseDouble(delimited_value[1]);
                    water_depth_unit = delimited_value[2];
                } catch (Exception e){

                }
                //us_state = delimited_value[12];
                try {
                    latitude = Double.parseDouble(delimited_value[3]);
                    longitude = Double.parseDouble(delimited_value[4]);
                } catch (Exception e){

                }

                if (delimited_value[6].length() > 0){
                    String[] characteristic_words = delimited_value[5].split(" ");
                    if (characteristic_words[0].equals("Algae_") || characteristic_words[0].equals("\"Algae_")){
                        try{
                            cv1 = cv1 + Double.parseDouble(delimited_value[6]);
                            cu1 = delimited_value[7];
                            cv1c++;
                        } catch (Exception nfe){

                        }
                    }
                    else if (characteristic_words[0].equals("Biomass_") || characteristic_words[0].equals("\"Biomass_")){
                        try{
                            cv2 = cv2 + Double.parseDouble(delimited_value[6]);
                            cu2 = delimited_value[7];
                            cv2c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Biomass/chlorophyll") || characteristic_words[0].equals("\"Biomass/chlorophyll")){
                        try{
                            cv3 = cv3 + Double.parseDouble(delimited_value[6]);
                            cu3 = delimited_value[7];
                            cv3c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Chlorophyll") || characteristic_words[0].equals("\"Chlorophyll")){
                        try{
                            cv4 = cv4 + Double.parseDouble(delimited_value[6]);
                            cu4 = delimited_value[7];
                            cv4c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Cladophora_") || characteristic_words[0].equals("\"Cladophora_")){
                        try{
                            cv5 = cv5 + Double.parseDouble(delimited_value[6]);
                            cu5 = delimited_value[7];
                            cv5c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Cyanobacteria_") || characteristic_words[0].equals("\"Cyanobacteria_")){
                        try{
                            cv6 = cv6 + Double.parseDouble(delimited_value[6]);
                            cu6 = delimited_value[7];
                            cv6c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Diatoms") || characteristic_words[0].equals("\"Diatoms")){
                        try{
                            cv7 = cv7 + Double.parseDouble(delimited_value[6]);
                            cu7 = delimited_value[7];
                            cv7c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Macroinvertebrates") || characteristic_words[0].equals("\"Macroinvertebrates")){
                        try{
                            cv8 = cv8 + Double.parseDouble(delimited_value[6]);
                            cu8 = delimited_value[7];
                            cv8c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Periphyton") || characteristic_words[0].equals("\"Periphyton")){
                        try{
                            cv9 = cv9 + Double.parseDouble(delimited_value[6]);
                            cu9 = delimited_value[7];
                            cv9c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                    else if (characteristic_words[0].equals("Phycocyanin") || characteristic_words[0].equals("\"Phycocyanin")){
                        try{
                            cv10 = cv10 + Double.parseDouble(delimited_value[6]);
                            cu10 = delimited_value[7];
                            cv10c++;
                        } catch (Exception nfe){
                            
                        }
                    }
                }
                
            }
            if (cv1c > 0){
                cv1 = cv1 / (double) cv1c;
            }
            if (cv2c > 0){
                cv2 = cv2 / (double) cv2c;
            }
            if (cv3c > 0){
                cv3 = cv3 / (double) cv3c;
            }
            if (cv4c > 0){
                cv4 = cv4 / (double) cv4c;
            }
            if (cv5c > 0){
                cv5 = cv5 / (double) cv5c;
            }
            if (cv6c > 0){
                cv6 = cv6 / (double) cv6c;
            }
            if (cv7c > 0){
                cv7 = cv7 / (double) cv7c;
            }
            if (cv8c > 0){
                cv8 = cv8 / (double) cv8c;
            }
            if (cv9c > 0){
                cv9 = cv9 / (double) cv9c;
            }
            if (cv10c > 0){
                cv10 = cv10 / (double) cv10c;
            }
            Row new_row_for_new_csv = RowFactory.create(id, start_date,
                water_depth_value, water_depth_unit, us_state, latitude, longitude,
                cv1, cu1, cv2, cu2, cv3, cu3, cv4, cu4, cv5, cu5, cv6, cu6, cv7, cu7, cv8, cu8,
                cv9, cu9, cv10, cu10
            );
            rows_for_new_csv.add(new_row_for_new_csv);
        }
        Dataset<Row> new_csv_data = spark.createDataFrame(rows_for_new_csv, wqbio_schema);
        new_csv_data.show();
        new_csv_data.write().option("header", "true").csv(args[1]);
        spark.stop();
    }
}