package io.ventura.generators.nexmark;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomSerializer implements Serializer<ByteBuffer> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, ByteBuffer data) {
		if (data == null) {
			return null;
		}
		return data.array();
	}

	@Override
	public void close() {

	}
}