package com.adintellig.hbase.ads.redis;


import com.adintellig.util.ConfigFactory;
import com.adintellig.util.ConfigProperties;
import com.adintellig.util.FileUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisBulkLoad
{
  static final Log LOG = LogFactory.getLog(RedisBulkLoad.class);
  public static final String NAME = "Redis-Bulk-Load";
  public static final String LOCAL_DIR = "/tmp/attribute_ads";
  public static final String JEDIS_PASSWORD = "_houyi630";
  public static String host;
  public static int port;
	static ConfigProperties config = ConfigFactory.getInstance().getConfigProperties("/app.properties");

  static List<String> hashFields = new ArrayList<String>();

  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    CommandLine cmd = parseArgs(otherArgs);
    String input = cmd.getOptionValue("i");
    String field = cmd.getOptionValue("f");

    if (!hashFields.contains(field)) {
      LOG.info("The input field does not exist! + " + field);
      return;
    }

    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(input);
    if (!fs.exists(inputPath)) {
      LOG.info("Input path is not exist!");
      return;
    }

    FileUtil.deleteFile(new File("/tmp/attribute_ads"));

    fs.copyToLocalFile(inputPath, new Path("/tmp/attribute_ads"));

    long st = System.currentTimeMillis();
    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxActive(1000);
    config.setMaxIdle(20);
    JedisPool pool = new JedisPool(config, host, port, 20000);
    Jedis jedis = (Jedis)pool.getResource();
    jedis.auth("_houyi630");
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/tmp/attribute_ads" + File.separator + "part-r-00000")));

    String line = null;
    int count = 0;
    while (null != (line = br.readLine())) {
      count++;
      line = line.trim();
      String[] arr = line.split("\t", -1);
      if (arr.length == 2) {
        String uid = arr[0];
        String adidOrAttr = arr[1];
        try {
          jedis.hset(uid, field, adidOrAttr);
        } catch (Exception e) {
          e.printStackTrace();

          pool.returnResource(jedis); continue; } finally { pool.returnResource(jedis);
        }
      }
      
      if (count % 1000 == 0) {
        System.out.println("count: " + count);
      }
    }

    long en = System.currentTimeMillis();
    System.out.println("count: " + count);
    System.out.println("time: " + (en - st));
    System.out.println("AVG time: " + (en - st) / count + "ms");

    pool.destroy();
    br.close();
  }

  private static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    Option o = new Option("i", "input", true, "the directory or file to read from (must exist)");

    o.setArgName("input");
    o.setRequired(true);
    options.addOption(o);

    o = new Option("f", "field", true, "field to write into (must exist)");
    o.setArgName("field");
    o.setRequired(true);
    options.addOption(o);

    options.addOption("d", "debug", false, "switch on DEBUG log level");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.err.println("ERROR: " + e.getMessage() + "\n");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Redis-Bulk-Load ", options, true);
      System.exit(-1);
    }
    return cmd;
  }

  static
  {
    hashFields.add("adidlist");
    hashFields.add("attr_gender");
    hashFields.add("attr_age");

    host = config.getProperty("redis.host");
    port = config.getInt("redis.port", 6379);
  }
}