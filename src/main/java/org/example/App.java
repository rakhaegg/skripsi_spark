package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class App
{
    private static Logger logger = Logger.getLogger(App.class);
    private static final Schema schema4 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"finalFeature\"," +
                    "  \"fields\": [" +
                    "    {\"name\" : \"id\",\"type\": \"long\"}," +
                    "    {\"name\" :  \"feature\", \"type\": {" +
                    "      \"type\" :\"map\"," +
                    "      \"values\": \"double\"" +
                    "    }" +
                    "    }" +
                    "  ]" +
                    "}"
    );

    public static  JavaPairRDD<Integer,Map<String,Double>> createInputRDD(String name,String partition, JavaSparkContext sc,FileSystem fileSystem) throws IOException {

        Path path = new Path("/output_2/"+name+"/"+partition);
        List<Tuple2<Integer,Map<String,Double>>> mapList= new ArrayList<>();
        try(BufferedInputStream inputStream = new BufferedInputStream(fileSystem.open(path));
            DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>())){
            Schema schema = dataFileStream.getSchema();
            GenericRecord temp = null;

            while (dataFileStream.hasNext()) {
                temp = dataFileStream.next();
                Map<Utf8,Double>input = (Map<Utf8, Double>)  temp.get("feature");
                HashMap<String,Double> newInput = new HashMap<>();
                for(Map.Entry<Utf8,Double> xxx : input.entrySet()){
                    newInput.put(xxx.getKey().toString(),xxx.getValue());
                }

                mapList.add(new Tuple2<>(Integer.parseInt(temp.get("id").toString()),newInput));
            }
        }
        return sc.parallelizePairs(mapList);
    }
    private static final Schema schema2 = new Schema.Parser().parse(
            "{" +
                    "  \"type\": \"record\"," +
                    "  \"name\": \"finalFeature\"," +
                    "  \"fields\": [" +
                    "    {\"name\" : \"id\",\"type\": \"string\"}," +
                    "    {\"name\" :  \"feature\", \"type\": {" +
                    "      \"type\" :\"map\"," +
                    "      \"values\": \"string\"" +
                    "    }" +
                    "    }" +
                    "  ]" +
                    "}"
    );
    public static void main( String[] args ) throws IOException {

        Options options = new Options();
        options.addOption("n",true,"NAME Folder");
        options.addOption("m",true,"Max Iteration");
        CommandLineParser parser = new DefaultParser();


        String name;
        int iteration;

        try {
            CommandLine cmd = parser.parse(options,args);
            logger.info("NAME FOLDER : " + cmd.getOptionValue("n"));
            logger.info("MAX ITERATION : " + cmd.getOptionValue("m"));
            name = cmd.getOptionValue("n");
            iteration = Integer.parseInt(cmd.getOptionValue("m"));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }



        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        SparkConf sparkConf = new SparkConf()
                .setAppName("master")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.rdd.compress","true")
                .set("spark.io.compression.codec","org.apache.spark.io.SnappyCompressionCodec")
                .setMaster("spark://master:7077");

        try(JavaSparkContext sc = new JavaSparkContext(sparkConf)){

            AtomicLong atomicLong = new AtomicLong();

            List<String> listAdjectiveWord = sc.textFile("hdfs://master:9000/adj.txt").collect();
            Broadcast<List<String>> stopWord = sc.broadcast(sc.textFile("hdfs://master:9000/stopwords_en.txt").collect());
            Broadcast<List<String>> adjWord = sc.broadcast(listAdjectiveWord);

            JavaPairRDD<Long,List<String>> javaPairRDD= sc.textFile("hdfs://master:9000/data/"+name+".json")
                    .mapToPair(item->{
                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree(item);
                        if (jsonNode.has("reviewText")
                                && jsonNode.has("reviewerID") &&
                                jsonNode.has("asin") &&
                                jsonNode.has("reviewerName")){
                            String reviewText;
                            reviewText = jsonNode.get("reviewText").asText();
                            String lowerData = reviewText.toLowerCase();
                            String regex = "(?u)\\b\\w\\w+\\b";
                            Pattern pattern = Pattern.compile(regex);
                            Matcher matcher = pattern.matcher(lowerData);
                            List<String> result  = new ArrayList<>();
                            while (matcher.find()) {
                                if(!matcher.group().isBlank() && !stopWord.getValue().contains(matcher.group())
                                        && adjWord.getValue().contains(matcher.group())){
                                    result.add(matcher.group());
                                }
                            }
                            atomicLong.set(atomicLong.get() + 1);
                            return new Tuple2<>(atomicLong.get(),result);
                        }
                        return null;
                    }).filter(Objects::nonNull);
            long lengthDocument =javaPairRDD.count();
            System.out.println("PANJANG  : " + lengthDocument);

            JavaPairRDD<Long,List<String>> persistJavaPairRDD =  javaPairRDD.repartition(80).persist(StorageLevel.MEMORY_AND_DISK_SER());

            Broadcast<Long> longBroadcast = sc.broadcast(lengthDocument);
            JavaPairRDD<String,Double> documentFrequency = persistJavaPairRDD.flatMap(item-> item._2.iterator())
                    .mapToPair(word -> new Tuple2<>(word,1.0))
                    .reduceByKey(Double::sum);

            Map<String,Double> qw = new HashMap<>();
            qw.putAll(documentFrequency.collectAsMap());
            listAdjectiveWord.forEach(item->{
                if(qw.containsKey(item)){
                    qw.put(item,Math.log((double) lengthDocument+1)/(qw.getOrDefault(item,0.0)+1)+1);
                }else{
                    qw.put(item,Math.log((double) lengthDocument+1)/(0+1)+1);
                }
            });
            Broadcast<Map<String,Double>> mapBroadcast = sc.broadcast(qw);


            JavaPairRDD<Long,Map<String,Double>> finalization =  persistJavaPairRDD.mapToPair(item->{
                        Long documentID = item._1;

                        Map<String,Long> hashMapAdjectiveWord = item._2
                                .stream().collect(Collectors.groupingBy(e->e,Collectors.counting()));
                        return new Tuple2<>(documentID,hashMapAdjectiveWord);
            }).mapToPair(item->{
                Map<String,Double> docFreq = mapBroadcast.getValue();

                Long length = longBroadcast.getValue();
                Long documentID  = item._1;
                AtomicReference<Double> denominator = new AtomicReference<>((double) 0);
                Map<String,Double> temp = docFreq.entrySet().stream().map((e)->{
                    String word = e.getKey();
                    double tf = 0.0;
                    if(item._2.containsKey(word)){
                        tf = (double) item._2.get(word);
                    }
                    double idf = (double) e.getValue();
                    double finalTF = tf/length;
                    denominator.set(denominator.get() + Math.pow((finalTF * idf ),2));
                    return Map.entry(word,(finalTF * idf));
                }).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));

                Double denominatorValue = Math.sqrt(denominator.get());

                Map<String,Double> finalResult = temp.entrySet().stream().map(itemTemp->{
                    Double result = itemTemp.getValue()/denominatorValue;
                    if(result.isNaN())
                        result = 0.0;
                    BigDecimal decimal = new BigDecimal(result);
                    BigDecimal round = decimal.setScale(6, RoundingMode.HALF_UP);
                    return Map.entry(itemTemp.getKey(),round.doubleValue());
                }).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
                return new Tuple2<>(documentID,finalResult);
            });

            Map<Integer,Map<String,Double>> initCentroid = new HashMap<>();
            int elementCountPosition = 0;
            JavaPairRDD<Long,Map<String,Double>> finalInput = finalization.persist(StorageLevel.MEMORY_AND_DISK_SER());
            for(Tuple2<Long,Map<String,Double>>element : finalInput.take(2)){
                initCentroid.put(elementCountPosition,element._2);
                elementCountPosition++;
            }

            Broadcast<Map<Integer,Map<String,Double>>> mapBroadCast =sc.broadcast(initCentroid);


            Map<Integer, Map<String,Double>> oldCentroid = new HashMap<>();

            for(int i = 0; i < iteration ;i++){
                Map<Integer, Map<String,Double>> temp1 = new HashMap<>();
                long startTime = System.currentTimeMillis();
                System.out.println("ITERATION : " + i);
                Broadcast<Map<Integer, Map<String, Double>>> finalBroadcastCentroid = mapBroadCast;
                JavaPairRDD<Integer,Tuple3<List<Long>,Map<String,Double>,Double>> result1 = finalInput.mapToPair(item->{
                    Map<Integer,Map<String,Double>> centroidFromBroadcast = finalBroadcastCentroid.getValue();
                    Long documentID = item._1;
                    Map<Integer,Double> distanceBasedCluster = new HashMap<>();
                    centroidFromBroadcast.forEach((keyCentroid,valueCentroid)->{
                        AtomicReference<Double> sum = new AtomicReference<>((double) 0);
                        for(Map.Entry<String,Double> word : valueCentroid.entrySet()){
                            Double inputTFIDF = item._2.get(word.getKey());
                            Double result = Math.pow((inputTFIDF -word.getValue()),2);
                            sum.set(sum.get() + result);
                        }
                        distanceBasedCluster.put(keyCentroid,Math.sqrt(sum.get()));
                    });
                    Map.Entry<Integer,Double> minEntry = distanceBasedCluster.entrySet().stream()
                            .min(Map.Entry.comparingByValue())
                            .orElse(null);
                    assert minEntry != null;
                    List<Long> list = new ArrayList<>();
                    list.add(documentID);
                    return new Tuple2<>(minEntry.getKey() , new Tuple3<>(list, item._2, Math.pow(minEntry.getValue(),2)));
                });
                JavaPairRDD<Integer,Tuple3<List<Long>,Map<String,Double>,Double>> reduce = result1.reduceByKey( (item1,item2)->{
                    item1._1().addAll(item2._1());
                    item2._2().forEach((key, value) -> item1._2().merge(key, value, Double::sum));
                    Double totalSse = item1._3() + item2._3();
                    return new Tuple3<>(item1._1(),item1._2(),totalSse);
                });
                reduce.mapValues(item->{
                    int size = item._1().size();
                    item._2().forEach((keyWord,valueWord)->{
                        item._2().put(keyWord, valueWord /size);
                    });
                    return new Tuple3<>(item._1(),item._2(),item._3());
                }).collect().forEach(item->{
                    System.out.println("CLUSTER INDEX : " + item._1());
                    System.out.println("ANGGOTA : " + item._2._1().size());
                    System.out.println("SSE : " + item._2._3());
                    temp1.put(item._1,item._2()._2());

                });
                if(i == 0){
                    oldCentroid = new HashMap<>(temp1);
                }else{
                    if(converge(oldCentroid,temp1)){
                        long endTime = System.currentTimeMillis();
                        System.out.println("TIME : " + (endTime-startTime)/ 1000.0);
                        System.out.println("CONVERGE ");
                        break;
                    }
                    oldCentroid.forEach((key,value)->{
                        if(!temp1.containsKey(key)){
                            temp1.put(key,value);
                        }
                    });
                    oldCentroid = new HashMap<>(temp1);
                }
                long endTime = System.currentTimeMillis();
                System.out.println("TIME : " + (endTime-startTime)/ 1000.0);
                System.out.println("\n");
                mapBroadCast = sc.broadcast(oldCentroid);
            }


        }

    }
    private static boolean converge(Map<Integer,Map<String,Double>> oldCentroid, Map<Integer,Map<String,Double>> centroid ){
        boolean isEqual =
                oldCentroid.entrySet().stream()
                        .allMatch(entry -> entry.getValue().equals(centroid.get(entry.getKey())));
        return isEqual;
    }
}
