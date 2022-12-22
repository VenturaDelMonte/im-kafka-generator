package io.ventura.generators.nexmark;

import com.google.common.util.concurrent.RateLimiter;
import io.ventura.generators.nexmark.original.RandomStrings;
import io.ventura.generators.nexmark.utils.ParameterTool;
import io.ventura.generators.nexmark.utils.ThreadLocalFixedSeedRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class YSBGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(YSBGenerator.class);


	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Generator Thread Group");

	public static void main(String[] args) {

		LOG.debug("{}", Arrays.toString(args));

		ParameterTool params = ParameterTool.fromArgs(args);

		final int numPartitions = params.getInt("numPartitions", 1);
		final long inputSize = params.getLong("inputSize", 1);
		final String hostname = params.get("hostname", "localhost");
		final String kafkaServers = params.get("kafkaServers", "localhost:9092");
		final int kafkaBatchSize = params.getInt("kafkaBatchSize", 8192);
		final int kafkaBufferMemory = params.getInt("kafkaBufferMemory", 256 * 1024 * 1024);
		final int kafkaBatchSizeMultiplier = params.getInt("kafkaBatchSizeMultiplier", 4);
		final int kafkaLinger = params.getInt("kafkaLinger", 100);
		final int desiredMinThroughputMBSec = params.getInt("desiredMinThroughputMBSec", 1);
		final int desiredMaxThroughputMBSec = params.getInt("desiredMaxThroughputMBSec", 1);
		final int cycleLengthSec = params.getInt("cycleLengthSec", 10);
		final String csvLoggingPath = params.get("csv", System.getProperty("java.io.tmpdir"));


		ExecutorService workers = Executors.newFixedThreadPool(numPartitions);

		LOG.info("Ready to start Nexmark generator with {} ({} GB) -- generator {} kafkaServers {}",
				numPartitions,
				inputSize,
				hostname,
				kafkaServers);

		Properties cfg = new Properties();

		int batchSize = kafkaBatchSize;

		cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.ventura.generators.nexmark.CustomSerializer");
		cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		cfg.put(ProducerConfig.RETRIES_CONFIG, 0);
		cfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaBufferMemory);
		cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize * kafkaBatchSizeMultiplier);
		cfg.put(ProducerConfig.ACKS_CONFIG, "0");
		cfg.put(ProducerConfig.LINGER_MS_CONFIG, kafkaLinger);
		cfg.put("send.buffer.bytes", -1);
		cfg.put("max.in.flight.requests.per.connection", "10");

		try {
			int totalWorkers = numPartitions;
			CountDownLatch starter = new CountDownLatch(totalWorkers);
			CountDownLatch controller = new CountDownLatch(totalWorkers);
			CountDownLatch fairStarter = new CountDownLatch(1);

			for (int j = 0; j < totalWorkers; j++) {
				Properties workerCfg = (Properties) cfg.clone();
				workerCfg.put(ProducerConfig.CLIENT_ID_CONFIG, "ysb-" + j);
				KafkaProducer<byte[], ByteBuffer> kafkaProducer = new KafkaProducer<>(workerCfg);
				YSBGeneratorRunner runner = new YSBGeneratorRunner(j, "ysb-" + j, 0, kafkaProducer, inputSize, starter, controller, fairStarter, desiredMinThroughputMBSec, desiredMaxThroughputMBSec, cycleLengthSec);
				workers.submit(runner);
			}
			starter.await();
			fairStarter.countDown();
			controller.await();
			workers.shutdown();
			workers.awaitTermination(1, TimeUnit.SECONDS);

		} catch (Throwable error) {
			LOG.error("Error while producing to kafka: {}", error.toString(), error);
		}
	}

	private static class YSBGeneratorRunner implements Runnable {

		private static final long ONE_GIGABYTE = 1024 * 1024 * 1024;

		private static final long ONE_KILOBYTE = 1024;

		private static final long ONE_MEGABYTE = 1024 * 1024;
		protected final int workerId;
		private final String name;
		private final byte[] genId;
		private final int targetPartition;
		private final long targetPartitionSize;

		private final KafkaProducer<byte[], ByteBuffer> kafkaYSB;
		private final CountDownLatch controller, starter;
		private final long desiredMinThroughputBytesPerSecond, desiredMaxThroughputBytesPerSecond, cycleLengthSec;
		private final CountDownLatch fairStarter;


		YSBGeneratorRunner(
				int workerId,
				String name,
				int targetPartition,
				KafkaProducer<byte[], ByteBuffer> kafkaYSB,
				long targetPartitionSize,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredMinThroughputMBSec,
				int desiredMaxThroughputMBSec,
				long cycleLengthSec) {
			this.targetPartitionSize = targetPartitionSize * ONE_GIGABYTE;

			this.workerId = workerId;
			this.kafkaYSB = kafkaYSB;
			this.controller = controller;
			this.starter = starter;
			this.name = name;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = targetPartition;
			this.fairStarter = fairStarter;
			this.desiredMinThroughputBytesPerSecond = ONE_MEGABYTE * desiredMinThroughputMBSec;
			this.desiredMaxThroughputBytesPerSecond = ONE_MEGABYTE * desiredMaxThroughputMBSec;
			this.cycleLengthSec = cycleLengthSec;

		}

		private static final int BUFFER_SIZE = 8192;

		private static final int CACHED_BUFFERS = 8192 * 4;

		private static final int METADATA_SIZE = 4 + 4 + 8;


		@Override
		public void run() {
			try {
				ArrayBlockingQueue<ByteBuffer> cachedBuffers = null;
				ScheduledFuture<?> futureP = null;
				ScheduledFuture<?> futureA = null;
				ScheduledFuture<?> futureB = null;
				try {
					ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
						@Override
						public Thread newThread(@Nonnull Runnable r) {
							Thread t = new Thread(THREAD_GROUP, r);
							t.setDaemon(true);
							return t;
						}
					});

					ByteBuffer b = ByteBuffer.wrap(genId);
					b.putInt(workerId);
					b.putLong(Long.reverse(System.nanoTime()) ^ System.currentTimeMillis());
					b.putInt(ThreadLocalFixedSeedRandom.current().nextInt());
					b.flip();
					b.get(genId);

					// tasks should be removed if the future is canceled
					executor.setRemoveOnCancelPolicy(true);

					// make sure shutdown removes all pending tasks
					executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
					executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

					cachedBuffers = new ArrayBlockingQueue<>(CACHED_BUFFERS);
					for (int i = 0; i < CACHED_BUFFERS; i++) {
						ByteBuffer buff = ByteBuffer.allocate(BUFFER_SIZE);
						cachedBuffers.add(buff);
					}

					int recordSize = 78;

					long recordsToGenerate = targetPartitionSize / 78;
					long pendingRecords = recordsToGenerate;

					AtomicLong sharedCounterPerson = new AtomicLong();

					starter.countDown();
					fairStarter.await();

					double startMs = System.currentTimeMillis();
					long sentBytes = 0;
					ThreadLocalFixedSeedRandom randomness = ThreadLocalFixedSeedRandom.current();

					long currentThroughput = desiredMinThroughputBytesPerSecond;
					long throughputDelta = (desiredMaxThroughputBytesPerSecond - desiredMinThroughputBytesPerSecond) / cycleLengthSec;
					long throughputChangeTimestamp = 0;
					long cycleLengthMSec = cycleLengthSec * 1_000;

					RateLimiter throughputThrottler = RateLimiter.create(currentThroughput, 5, TimeUnit.SECONDS);
					LOG.debug("Create throughputThrottler for {} -> << {} MB/sec : {} MB/sec >>",
							workerId, desiredMinThroughputBytesPerSecond / ONE_MEGABYTE, desiredMaxThroughputBytesPerSecond / ONE_MEGABYTE);
					boolean varyingWorkload = desiredMaxThroughputBytesPerSecond != desiredMinThroughputBytesPerSecond;

					long sentBytesDelta = 0;
					int recordsPerBuffer = (BUFFER_SIZE - METADATA_SIZE) / recordSize;
					ByteBuffer buf = cachedBuffers.take();
					buf.putInt(0x30011991);
					int itemsPerBuffer = (int) Math.min(recordsPerBuffer, pendingRecords);
					long backlogRecords = pendingRecords - itemsPerBuffer;
					buf.putInt(itemsPerBuffer);
					buf.putLong(backlogRecords);


					long sentRecords = 0;
					long eventId = 0;

					for (; eventId < recordsToGenerate; eventId++) {
						final long timestamp = System.currentTimeMillis();
						buf.putLong(0);
						buf.putLong(1); // 16
						buf.putLong(randomness.nextInt(10000));
						buf.putLong(0); // 32
						buf.putLong(eventId % 3); // 40
						buf.putLong(timestamp); // 48
						buf.putLong(0x01020304); // 56
						buf.putLong(1); // 64
						buf.putLong(1); // 72
						buf.putInt(1); //76
						buf.putShort((short) 1); // 78
						pendingRecords--;
						if (buf.remaining() < recordSize) {
							buf.position(buf.position() + buf.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>("ysb-flink", targetPartition, genId, buf);
							kafkaYSB.send(kafkaRecord, new KafkaNexmarkGenerator.InternalCallback(cachedBuffers, buf, sharedCounterPerson, itemsPerBuffer));
							sentRecords += itemsPerBuffer;
							buf = cachedBuffers.take();
							buf.putInt(0x30011991);
							itemsPerBuffer = (int) Math.min(itemsPerBuffer, pendingRecords);
							backlogRecords = pendingRecords - itemsPerBuffer;
							buf.putInt(itemsPerBuffer);
							buf.putLong(backlogRecords);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
							throughputThrottler.acquire(BUFFER_SIZE);
//							LOG.debug("Sent buffer " + sentRecords);
						}
						if (varyingWorkload && (timestamp - throughputChangeTimestamp) > cycleLengthMSec) {
							currentThroughput += throughputDelta;
							if (currentThroughput > desiredMaxThroughputBytesPerSecond) {
								throughputDelta = -throughputDelta;
							} else if (currentThroughput < desiredMinThroughputBytesPerSecond) {
								currentThroughput = desiredMinThroughputBytesPerSecond;
								throughputDelta = -throughputDelta;
							}
							throughputThrottler.setRate(currentThroughput);
							throughputChangeTimestamp = timestamp;
							LOG.debug("Throttler changed to {} MB/s", ((double) currentThroughput) / ONE_MEGABYTE);
						}

					}

				} catch (Throwable error) {

				}

			} catch (Throwable error) {

			}
		}


	}

}