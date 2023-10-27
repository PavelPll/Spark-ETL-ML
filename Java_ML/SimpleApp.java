/* ffSimpleApp.java */
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.RowFactory;

import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.DataFrameNaFunctions;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.ml.feature.StringIndexer;

import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;


import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;



import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;


public class SimpleApp {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("app").setMaster("local");
    //JavaSparkContext sc = new JavaSparkContext(conf);
    //Or
    SparkContext sc_raw = new SparkContext(conf);
    JavaSparkContext sc = JavaSparkContext.fromSparkContext(sc_raw);

    //Read data
    JavaRDD<String> rawRDD = sc.textFile("./src/housing.data");
    JavaRDD<String[]> RDD = rawRDD.map(line->line.substring(1).split("\\s+"));
    Row r1 = RowFactory.create("name1", "value1", 1);

    //convert JavaRDD<String[]> to JavaRDD<Row>
    JavaRDD<Row> RDD_rows = RDD.map(fields -> RowFactory.create(fields));

    StructType schema = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("CRIM", DataTypes.StringType, true),
      DataTypes.createStructField("ZN", DataTypes.StringType, true),
      DataTypes.createStructField("INDUS", DataTypes.StringType, true),
      DataTypes.createStructField("CHAS", DataTypes.StringType, true),
      DataTypes.createStructField("NOX", DataTypes.StringType, true),
      DataTypes.createStructField("RM", DataTypes.StringType, true),
      DataTypes.createStructField("AGE", DataTypes.StringType, true),
      DataTypes.createStructField("DIS", DataTypes.StringType, true),
      DataTypes.createStructField("PAD", DataTypes.StringType, true),
      DataTypes.createStructField("TAX", DataTypes.StringType, true),
      DataTypes.createStructField("PTRATIO", DataTypes.StringType, true),
      DataTypes.createStructField("B", DataTypes.StringType, true),
      DataTypes.createStructField("LSTAT", DataTypes.StringType, true),
      DataTypes.createStructField("MEDV", DataTypes.StringType, true)
      //DataTypes.createStructField("val2", DataTypes.DoubleType, true),
      //DataTypes.createStructField("val3", DataTypes.IntegerType, true)
});
    //RDD to Daraframe
    SparkSession spark = SparkSession
    .builder()
    .appName("DataScientest")
    .getOrCreate();
    Dataset<Row> df = spark.createDataFrame(RDD_rows, schema);
    df.show();
    Dataset<Row> df2 = df.select(col("CRIM").cast("double"),
                             col("ZN").cast("double"),
                             col("INDUS").cast("double"),
                             col("CHAS").cast("double"),
                             col("NOX").cast("double"),
                             col("RM").cast("double"),
                             col("AGE").cast("double"),
                             col("DIS").cast("double"),
                             col("PAD").cast("double"),
                             col("TAX").cast("double"),
                             col("PTRATIO").cast("double"),
                             col("B").cast("double"),
                             col("LSTAT").cast("double"),
                             col("MEDV").cast("double") );
    df2.printSchema();
    df2.describe().show();
    //some statistics
    df2.groupBy("CHAS","MEDV").count().sort("CHAS","MEDV").show();
    df2.filter(col("CHAS").equalTo(1.0)).groupBy("CHAS","MEDV").count().sort("CHAS","MEDV").show();

    //to svmlib format
    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[] {"CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM","AGE","DIS","PAD","TAX","PTRATIO","B","LSTAT","MEDV"})
      .setOutputCol("features_pre");
    Dataset<Row> data = assembler.transform(df2).select("MEDV", "features_pre");
    data = data.withColumnRenamed("MEDV", "label");
    data.show();

    StandardScaler scaler = new StandardScaler()
      .setInputCol("features_pre")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false);

    //split dataset
    Dataset<Row>[] data_split = data.randomSplit(new double[] {0.8, 0.2}, 12345);
    Dataset<Row> train = data_split[0];
    Dataset<Row> test = data_split[1];
///////////////////////////////////////////////////////////
    //Single fit
    LinearRegression lr = new LinearRegression();
    StandardScaler scaler2 = new StandardScaler()
      .setInputCol("features_pre")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(false);
    StandardScalerModel scalerModel2 = scaler2.fit(data);
    Dataset<Row> scaled = scalerModel2.transform(data);
    scaled.show();
    LinearRegression lr0 = new LinearRegression();
    LinearRegressionModel lr0model = lr0.fit(scaled);
    //show some parameters
    System.out.println("RegParam for single model: "+ lr0model.getRegParam());
    System.out.println("Solver for single model: "+ lr0model.getSolver());
    System.out.println("Standardization for single model: "+ lr0model.getStandardization());

/////////////////////////////////////////////////////////////
    //Try the same, but using pipeline:
    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {scaler, lr});
    PipelineModel model = pipeline.fit(train);
    Dataset<Row> predictions = model.transform(test);
    predictions.show();
    JavaRDD<Row> predictions_rdd = predictions.toJavaRDD();
    JavaPairRDD< Object, Object> predictionAndLabels = predictions_rdd.mapToPair(p ->
      new Tuple2<>(p.getAs(3), p.getAs(0)));
    System.out.println(predictions.getClass());
    System.out.println(predictions.rdd().getClass());
    //MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
    //System.out.format("Weighted precision = %f\n", metrics.weightedPrecision());

    Dataset<Row> forEvaluationDF = predictions.select(col("label"),col("prediction"));
    RegressionEvaluator evaluateR2 = new RegressionEvaluator().setMetricName("r2");
    double r2 = evaluateR2.evaluate(forEvaluationDF);
    System.out.format("r2 = %f\n", r2);
//Je n'arrive pas trouver COMMENT avoir des proprietes de scaler et de la regression
//à partir de pipeline/pipeline model
//System.out.println(model.stages()); donne que l'adresse
//pour pyspark pipeline.getStages[0] ou model.stages[0] (pyspask)
/////////////////////////////////////////////////
    //Try the same, but using CV
    //go to Class LinearRegression then SetSolver
    //to get all possible solvers: "l-bfgs", "normal"
    ParamMap[] paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam(), new double[] {1, 0.1, 0.01})
      .addGrid(lr.maxIter(), new int[] {1, 10, 20})
      //.addGrid(lr.solver(), new String[] {"l-bfgs", "normal"})
      .build();

    CrossValidator cv = new CrossValidator()
      .setEstimator(pipeline)
      //.setEvaluator(new MulticlassClassificationEvaluator())
      .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4);

    CrossValidatorModel cvModel = cv.fit(train);
    Dataset<Row> predictions_cv = cvModel.transform(test);
    predictions_cv.show();

    Dataset<Row> forEvaluationDF_cv = predictions_cv.select(col("label"),col("prediction"));
    RegressionEvaluator evaluateR_cv = new RegressionEvaluator().setMetricName("r2");
    double rCv2 = evaluateR2.evaluate(forEvaluationDF);
    System.out.format("r2 = %f\n", rCv2);

//https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/mllib/evaluation/package-summary.html
//ici on trouve class RegressionMetrics
//et r2()
    JavaRDD<Row> predictions_cv_rdd2 = predictions_cv.select("prediction", "label").toJavaRDD();
    JavaPairRDD< Object, Object> predictionAndLabels2 = predictions_cv_rdd2.mapToPair(p -> new Tuple2<>(p.getAs(0), p.getAs(1)));
    //MulticlassMetrics metrics_cv_2 = new MulticlassMetrics(predictionAndLabels2.rdd());
    RegressionMetrics metrics_cv_2 = new RegressionMetrics(predictionAndLabels2.rdd());
    System.out.format("Weighted precision cv = %f\n", metrics_cv_2.r2());
    System.out.println(Arrays.toString(cvModel.avgMetrics()));
  }
}