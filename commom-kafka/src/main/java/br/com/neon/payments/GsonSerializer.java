package br.com.neon.payments;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T>{

	private final Gson gson = new GsonBuilder().create();

	@Override
	public byte[] serialize(String s, T object) {
		return gson.toJson(object).getBytes();		
	} 
}
