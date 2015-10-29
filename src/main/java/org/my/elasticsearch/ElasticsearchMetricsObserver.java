package org.my.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.netflix.servo.Metric;
import com.netflix.servo.publish.BaseMetricObserver;

/**
 * A servo metrics observer publishing metrics into an elasticsearch
 * 
 * @author Bruno DOOLAEGHE
 *
 */
public class ElasticsearchMetricsObserver extends BaseMetricObserver {

	/* target elsatic search */
	private String host;
	private int port;
	private String cluster;
	private String index;
	private String type;

	public ElasticsearchMetricsObserver(String metricsPrefix, String host, String port, String cluster, String index, String type) {
		super(metricsPrefix);
		this.host = host;
		this.port = Integer.parseInt(port);
		this.cluster = cluster;
		this.index = index;
		this.type = type;
	}
	
	@Override
	public void updateImpl(List<Metric> metrics) {
		try (TransportClient client = client()) {
			XContentBuilder contentBuilder = jsonBuilder().startObject().prettyPrint();
			
			// es fields
			SimpleDateFormat dfIndex = new SimpleDateFormat("yyyy.MM.dd");
			String today = dfIndex.format(System.currentTimeMillis());
			contentBuilder.field("DAY", today);
			SimpleDateFormat dfTs = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
			contentBuilder.field("@timestamp", dfTs.format(System.currentTimeMillis()));
			
			// metrics value
			for (Metric metric : metrics) {
				contentBuilder.field(metric.getConfig().getName(), metric.getValue());
			}
			
			contentBuilder.endObject();
			
			// send to ES index
			IndexRequest request = new IndexRequest(index + "-" + today, type, Long.toString(System.nanoTime()));
			request.source(contentBuilder);
			client.index(request).actionGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private TransportClient client() {
		Builder settingsBuilder = ImmutableSettings.settingsBuilder();
		settingsBuilder.put("cluster.name", cluster);
		return new TransportClient(settingsBuilder).addTransportAddress(new InetSocketTransportAddress(host, port));
	}

}
