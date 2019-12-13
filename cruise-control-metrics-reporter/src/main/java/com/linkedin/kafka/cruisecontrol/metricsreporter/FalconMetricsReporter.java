package com.linkedin.kafka.cruisecontrol.metricsreporter;

import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.MetricsUtils;

/**
 * Report KAFKA Connector related metrics to Falcon
 */
public class FalconMetricsReporter implements MetricsReporter, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FalconMetricsReporter.class);
  private Map<org.apache.kafka.common.MetricName, KafkaMetric> interestedMetrics = new ConcurrentHashMap<>();
  private KafkaThread metricsReporterRunner;
  private volatile boolean shutdown = false;
  private long reportingIntervalMs;
  private String falconUrl;
  private String endpoint;
  private long lastReportingTime = System.currentTimeMillis();

  @Override
  public void configure(Map<String, ?> configs) {
    Object interval = configs.get("falcon.metrics.reporting.interval.ms");
    reportingIntervalMs = interval == null ? 60000 : Long.parseLong(interval.toString());
    Object url = configs.get("falcon.url");
    falconUrl = url == null ? "http://transfer.falcon.vip.sankuai.com:6060/api/push" : url.toString();
    try {
      String hostname = InetAddress.getLocalHost().getHostName();
      hostname = hostname == null ? "UnknownClient" : hostname.trim();
      if (falconUrl.indexOf("_HOST") != -1){
        falconUrl = falconUrl.replaceAll("_HOST", hostname);
      }
      int index = hostname.indexOf(".");
      this.endpoint = index != -1 ? hostname.substring(0, index) : hostname;
    } catch (UnknownHostException e) {
      LOG.error("FalconSink init failed!", e);
      endpoint = "UnknownClient";
    }
    LOG.info("Falcon metrics repoter interval is {}, url is {}", reportingIntervalMs, falconUrl);
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    for (KafkaMetric kafkaMetric : metrics) {
      addMetricIfInterested(kafkaMetric);
    }
    LOG.info("Added {} Kafka metrics for Falcon metrics during initialization.", interestedMetrics.size());
    metricsReporterRunner = new KafkaThread("FalconMetricsReporterRunner", this, true);
    metricsReporterRunner.start();
  }

  @Override
  public void metricChange(KafkaMetric metric) {
    addMetricIfInterested(metric);
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    interestedMetrics.remove(metric.metricName());
  }

  private void addMetricIfInterested(KafkaMetric metric) {
    LOG.trace("Checking Kafka metric {}", metric.metricName());
    if (MetricsUtils.isConnectInterested(metric.metricName())) {
      LOG.debug("Added new metric {} to falcon metrics reporter.", metric.metricName());
      interestedMetrics.put(metric.metricName(), metric);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing Falcon metrics reporter.");
    shutdown = true;
    if (metricsReporterRunner != null) {
      metricsReporterRunner.interrupt();
    }
  }

  @Override
  public void run() {
    LOG.info("Starting Falcon metrics reporter with reporting interval of {} ms.", reportingIntervalMs);
    try {
      while (!shutdown) {
        long now = System.currentTimeMillis();
        LOG.debug("Reporting metrics for time {}.", now);
        try {
          if (now > lastReportingTime + reportingIntervalMs) {
            lastReportingTime = now;
            reportKafkaMetrics(now);
          }
        } catch (Exception e) {
          LOG.error("Got exception in Falcon metrics reporter", e);
        }
        long nextReportTime = now + reportingIntervalMs;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reporting finished for time {} in {} ms. Next reporting time {}",
                    now, System.currentTimeMillis() - now, nextReportTime);
        }
        while (!shutdown && now < nextReportTime) {
          try {
            Thread.sleep(nextReportTime - now);
          } catch (InterruptedException ie) {
            // let it go
          }
          now = System.currentTimeMillis();
        }
      }
    } finally {
      LOG.info("Falcon metrics reporter exited.");
    }
  }

  private void reportKafkaMetrics(long now) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reporting KafkaMetrics. {}", interestedMetrics.values());
    }
    try {
      URL url = new URL (falconUrl);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setConnectTimeout(3000);
      con.setReadTimeout(3000);
      con.setDoOutput(true);
      con.setDoInput(true);
      con.setUseCaches(false);
      con.setRequestMethod("POST");
      con.connect();
      String pairsMap = generateJSON(now);
      try (PrintWriter wr = new PrintWriter(con.getOutputStream())) {
        wr.write(pairsMap);
        wr.flush();
      }
      if (con.getResponseCode() > 300) {
        LOG.warn("send metrics to falcon error, return code : " + con.getResponseCode());
      }
      con.disconnect();
    } catch (Exception e) {
      LOG.error("send metrics to falcon failed!", e);
    }
  }

  private String generateJSON(long now) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode arrayNode = mapper.createArrayNode(); 
    for (KafkaMetric metric : interestedMetrics.values()) {
      Map<String, String> tags = metric.metricName().tags();
      String metricName = new StringBuilder(tags.get("connector")).append(".")
          .append(tags.get("task")).append(".").append(metric.metricName().name()).toString();
      ObjectNode node = mapper.createObjectNode();
      node.put("endpoint", endpoint);
      node.put("metric", metricName);
      node.put("timestamp", now);
      node.put("step", reportingIntervalMs / 1000);
      node.put("value", Double.parseDouble(metric.metricValue().toString()));
      node.put("counterType", "GAUGE");
      node.put("tags", "");
      arrayNode.add(node); 
    }
    return mapper.writeValueAsString(arrayNode); 
  }
}
