package com.daniel.java.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by daniel on 2020/4/27.
 **/
public class SparkHiveKerberosJavaDemo {
    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(SparkHiveKerberosJavaDemo.class);
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("SparkHiveKerberosScalaDemo");
        String keyTab = null;
        String principal = null;
        if (args.length == 2) {
            keyTab = args[0];
            principal = args[1];
        } else {
            System.out.println("Usage: This class need to arguments , first is the keytab file.");
            System.out.println("       The Second is the principal.");
            System.exit(-1);
        }
        SparkSession sparkSession = SparkSession
                .builder()
                // if there has no hive-site.xml, we need this config
                //.config("spark.sql.warehouse.dir", warehouseLocation)
                .config(sparkConf)
                .config("hadoop.security.authentication", "kerberos")
                .config("spark.yarn.keytab", keyTab)
                .config("spark.yarn.principal", principal)
                .enableHiveSupport()
                .getOrCreate();

        logger.info(sparkSession.conf().get("spark.sql.warehouse.dir"));
        sparkSession.sql("show databases").show();
        sparkSession.close();
    }
}


/*


提交命令
1. 先kinit principal

On Yarn Cluster
spark2-submit --class com.daniel.java.spark.sql.SparkHiveKerberosJavaDemo   \
--master yarn --deploy-mode cluster --executor-memory 2g --executor-cores 2 \
--driver-memory 2g --num-executors 2 /root/democode-1.0-SNAPSHOT.jar  \
/root/daniel.keytab daniel@DANIEL.COM

on Yarn Client

spark2-submit --class com.daniel.java.spark.sql.SparkHiveKerberosJavaDemo   \
--master yarn --deploy-mode client --executor-memory 2g --executor-cores 2 \
--driver-memory 2g --num-executors 2 /root/democode-1.0-SNAPSHOT.jar  \
/root/daniel.keytab daniel@DANIEL.COM


 */