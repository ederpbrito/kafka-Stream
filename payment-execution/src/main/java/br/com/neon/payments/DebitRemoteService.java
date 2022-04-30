package br.com.neon.payments;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;

import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.neon.payments.domain.Debit;
import io.javalin.Javalin;
import io.javalin.http.Context;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class DebitRemoteService {
	private final HostInfo hostInfo;
	private final KafkaStreams streams;
	 

	private static final Logger log = LoggerFactory.getLogger(DebitRemoteService.class);

	DebitRemoteService(HostInfo hostInfo, KafkaStreams streams) {
		this.hostInfo = hostInfo;
		this.streams = streams;
	}

	ReadOnlyKeyValueStore<String, Debit> getStore() {
		return streams.store(StoreQueryParameters.fromNameAndType(
				// state store name
				"debit-counts",
				// state store type
				QueryableStoreTypes.keyValueStore()));
	}

	void start() {
		Javalin app = Javalin.create().start(hostInfo.port());

		/** Local key-value store query: all entries */
		app.get("/debit", this::getAll);

		/** Local key-value store query: approximate number of entries */
		app.get("/debit/count", this::getCount);

		/** Local key-value store query: approximate number of entries */
		app.get("/debit/count/local", this::getCountLocal);

		/** Local key-value store query: range scan (inclusive) */
		app.get("/debit/:from/:to", this::getRange);

		/** Local key-value store query: point-lookup / single-key lookup */
		app.get("/debit/:key", this::getKey);
	}

	void getAll(Context ctx) {
		Map<String, Debit> debits = new HashMap<>();

		KeyValueIterator<String, Debit> range = getStore().all();
		while (range.hasNext()) {
			KeyValue<String, Debit> next = range.next();
			String key = next.key;
			Debit debit = next.value;
			debits.put(key, debit);
		}
		// close the iterator to avoid memory leaks!
		range.close();
		// return a JSON response
		ctx.json(debits);
	}

	void getRange(Context ctx) {
		String from = ctx.pathParam("from");
		String to = ctx.pathParam("to");

		Map<String, Debit> debits = new HashMap<>();

		KeyValueIterator<String, Debit> range = getStore().range(from, to);
		while (range.hasNext()) {
			KeyValue<String, Debit> next = range.next();
			String key = next.key;
			Debit debit = next.value;
			debits.put(key, debit);
		}
		// close the iterator to avoid memory leaks!
		range.close();
		// return a JSON response
		ctx.json(debits);
	}

	void getCount(Context ctx) {
		long count = getStore().approximateNumEntries();

		for (StreamsMetadata metadata : streams.allMetadataForStore("debit-counts")) {
			if (!hostInfo.equals(metadata.hostInfo())) {
				continue;
			}
			count += fetchCountFromRemoteInstance(metadata.hostInfo().host(), metadata.hostInfo().port());
		}

		ctx.json(count);
	}

	long fetchCountFromRemoteInstance(String host, int port) {
		OkHttpClient client = new OkHttpClient();

		String url = String.format("http://%s:%d/debit/count/local", host, port);
		Request request = new Request.Builder().url(url).build();

		try (Response response = client.newCall(request).execute()) {
			return Long.parseLong(response.body().string());
		} catch (Exception e) {
			// log error
			log.error("Could not get debit count", e);
			return 0L;
		}
	}

	void getCountLocal(Context ctx) {
		long count = 0L;
		try {
			count = getStore().approximateNumEntries();
		} catch (Exception e) {
			log.error("Could not get local debit count", e);
		} finally {
			ctx.result(String.valueOf(count));
		}
	}

	void getKey(Context ctx) {
		String debitId = ctx.pathParam("key");

		// find out which host has the key
		KeyQueryMetadata metadata = streams.queryMetadataForKey("debit-counts", debitId, Serdes.String().serializer());

		// the local instance has this key
		if (hostInfo.equals(metadata.activeHost())) {
			log.info("Querying local store for key");
			Debit debit = getStore().get(debitId);

			if (debit == null) {
				// game was not found
				ctx.status(404);
				return;
			}

			// game was found, so return the high scores
			ctx.json(debit);
			return;
		}

		// a remote instance has the key
		String remoteHost = metadata.activeHost().host();
		int remotePort = metadata.activeHost().port();
		String url = String.format("http://%s:%d/debit/%s", remoteHost, remotePort, debitId);

		// issue the request
		OkHttpClient client = new OkHttpClient();
		Request request = new Request.Builder().url(url).build();

		try (Response response = client.newCall(request).execute()) {
			log.info("Querying remote store for key");
			ctx.result(response.body().string());
		} catch (Exception e) {
			ctx.status(500);
		}
	}
}
