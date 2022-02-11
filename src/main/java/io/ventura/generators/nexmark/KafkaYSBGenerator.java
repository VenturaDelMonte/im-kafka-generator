package io.ventura.generators.nexmark;

import com.google.common.util.concurrent.RateLimiter;
import io.ventura.generators.nexmark.utils.ParameterTool;
import io.ventura.generators.nexmark.utils.ThreadLocalFixedSeedRandom;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaYSBGenerator {

	private static final long ONE_KILOBYTE = 1024L;
	private static final long ONE_MEGABYTE = 1024L * 1024L;
	private static final long ONE_GIGABYTE = 1024L * 1024L * 1024L;

	private static final long LOGGING_THRESHOLD = 256 * ONE_MEGABYTE;

	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Generator Thread Group");

	private static final Logger LOG = LoggerFactory.getLogger(KafkaYSBGenerator.class);

	private static HashMap<String, int[]> PARTITIONS_RANGES = new HashMap<>();


	static {
		// PAY ATTENTION HERE

		// partition configs copied from KafkaNexmarkGenerator
		PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});
		PARTITIONS_RANGES.put("localhost-4", new int[] { 0, 1, 2, 3 });
		PARTITIONS_RANGES.put("localhost-8", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });

		PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PARTITIONS_RANGES.put("cloud-37-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31, 32 });

		PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PARTITIONS_RANGES.put("cloud-37-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });

		PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		PARTITIONS_RANGES.put("cloud-37-16", new int[] {  4,  5,  6,  7 });
		PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		PARTITIONS_RANGES.put("cloud-37-20", new int[] {  4,  5,  6,  7 });
		PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });

		// GCP
		int PARTITIONS = 8;
		int NODES = 4;
		for (int i = 0; i < NODES; i++) {
			int tmp[] = new int[PARTITIONS];
			for (int j = 0; j < PARTITIONS; j++) {
				tmp[j] = i * PARTITIONS + j;
			}
			PARTITIONS_RANGES.put(String.format("im-generator-%02d-64", i + 1), tmp);
			PARTITIONS_RANGES.put(String.format("im-generator-%02d-32", i + 1), tmp);
			PARTITIONS_RANGES.put(String.format("im-generator-%02d-16", i + 1), tmp);
			PARTITIONS_RANGES.put(String.format("im-generator-%02d-8", i + 1), tmp);
		}
		PARTITIONS_RANGES.put("im-generator-01-4", new int[] {0, 1, 2, 3});
	}

	private static final long MAX_PERSON_ID = 1_000_000_000L;
	private static final long MAX_AUCTION_ID = 180_000_000_000L;
	private static final long MAX_BID_ID = MAX_AUCTION_ID;

	public static void main(String[] args) {

		LOG.debug("{}", Arrays.toString(args));

//		GeneratorParameters params = new GeneratorParameters();
//
//		JCommander.newBuilder()
//				.addObject(params)
//				.acceptUnknownOptions(false)
//				.build()
//				.parse(args);

		ParameterTool params = ParameterTool.fromArgs(args);

		final int n_workers = params.getInt("YSBworkers", 1);
		final int n_partitions = params.getInt("YSBpartitions", 1);
		final long inputSizeItems = params.getInt("inputSizeItems", 1);
		final String hostname = params.get("hostname", "localhost");
		final String kafkaServers = params.get("kafkaServers", "localhost:29092");
		final int kafkaBatchSize = params.getInt("kafkaBatchSize", 8192);
		final int kafkaBufferMemory = params.getInt("kafkaBufferMemory", 256 * 1024 * 1024);
		final int kafkaBatchSizeMultiplier = params.getInt("kafkaBatchSizeMultiplier", 4);
		final int kafkaLinger = params.getInt("kafkaLinger", 100);
		final int desiredThroughputKBSec = params.getInt("desiredThroughputKBSec", 1024);
		final String csvLoggingPath = params.get("csv", System.getProperty("java.io.tmpdir"));
		final boolean rustMode = params.getBoolean("rustMode", false);
		final boolean varyingWorkload = params.getBoolean("varyingWorkload", false);

		ExecutorService workers = Executors.newFixedThreadPool(n_workers);

		LOG.info("Ready to start YSB generator with {} partitions and {} workers ({} GB). generator {} kafkaServers {}. ",
				n_partitions,
				workers,
				inputSizeItems,
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

		int[] partitions = PARTITIONS_RANGES.get(hostname + "-" + n_partitions);

		LOG.debug("Selected: {}", Arrays.toString(partitions));

		HashMap<String, Long> helper = new HashMap<>();

		helper.put("cloud-14", 0l);
		helper.put("cloud-37", 1l);
		helper.put("cloud-37", 1l);
		helper.put("cloud-24", 2l);
		helper.put("cloud-25", 3l);
		helper.put("cloud-33", 4l);

		helper.put("im-generator-01", 0l);
		helper.put("im-generator-02", 1l);
		helper.put("im-generator-03", 2l);
		helper.put("im-generator-04", 3l);
		helper.put("im-generator-05", 4l);

		helper.put("localhost", 0L);

		try {
			int totalWorkers = n_workers;
			CountDownLatch starter = new CountDownLatch(totalWorkers);
			CountDownLatch controller = new CountDownLatch(totalWorkers);
			CountDownLatch fairStarter = new CountDownLatch(1);

			for (int j = 0; j < totalWorkers; j++) {
				// every worker works for all three topics
				Properties workerConfig = (Properties) cfg.clone();
				workerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "YSBGen-" + j);

				KafkaProducer<byte[], ByteBuffer> kafkaProducerYSB = new KafkaProducer<>(workerConfig);

				int targetPartition = partitions[j];

				GeneratorRunner runner = new GeneratorRunner(
						j,
						YSB_TOPIC,
						hostname,
						targetPartition,
						kafkaProducerYSB,
						inputSizeItems,
						starter,
						controller,
						fairStarter,
						desiredThroughputKBSec,
						csvLoggingPath,
						varyingWorkload
				);

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

	private static final String YSB_TOPIC = "ysb_batch";

	private static final int BUFFER_SIZE = 8192;

	private static final int CACHED_BUFFERS = 8192 * 4;

	private static final int METADATA_SIZE = 4 + 4 + 8;

	public static class GeneratorRunner implements Runnable {

		protected final int workerId;
		private final String name, topicNameYSB;
		private final byte[] genId;
		private final int targetPartition;
		private final long targetPartitionSize;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerYSB;
		private final CountDownLatch controller, starter;
		private final long desiredThroughputBytesPerSecond;
		private final CountDownLatch fairStarter;

		private final String csvDirectory;

		private final boolean varyingWorkload;

		GeneratorRunner(
				int workerId,
				String topicNameYSB,
				String name,
				int targetPartition,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerYSB,
				long targetPartitionSize,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputKBSec,
				String csvDirectory,
				boolean varyingWorkload) {

			this.targetPartitionSize = targetPartitionSize * ONE_GIGABYTE;
			this.workerId = workerId;
			this.kafkaProducerYSB = kafkaProducerYSB;
			this.controller = controller;
			this.starter = starter;
			this.topicNameYSB = topicNameYSB;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = targetPartition;
			this.name = name;
			this.fairStarter = fairStarter;
			this.desiredThroughputBytesPerSecond = ONE_KILOBYTE * desiredThroughputKBSec;
			this.csvDirectory = csvDirectory;
			this.varyingWorkload = varyingWorkload;
		}

		@Override
		public void run() {
			ArrayBlockingQueue<ByteBuffer> cachedBuffers = null;
			ScheduledFuture<?> future = null;
			try {
				ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
					@Override
					public Thread newThread(@Nonnull Runnable r) {
						Thread t = new Thread(THREAD_GROUP, r);
						t.setDaemon(true);
						return t;
					}
				});

				ByteBuffer b = ByteBuffer.wrap(genId); // for what is this buffer?
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

//				int ysbRecordSize = 84; // = 9*8 + 1*4 + 1*2
				int ysbRecordSize = 88; // = 11*8

				long recordsToGenerate = targetPartitionSize / ysbRecordSize;

				int itemsPerBuffer = (BUFFER_SIZE - METADATA_SIZE) / ysbRecordSize;

				AtomicLong sharedCounterSentRecords = new AtomicLong();

				starter.countDown();
				fairStarter.await();

				ThroughtputLogger personLogger = new ThroughtputLogger(sharedCounterSentRecords, csvDirectory, name, topicNameYSB + "-" + workerId, 5, ysbRecordSize);
				future = executor.scheduleAtFixedRate(personLogger, 5, 5, TimeUnit.SECONDS);

				double startMs = System.currentTimeMillis();
				long sentBytes = 0;
				ThreadLocalFixedSeedRandom randomness = ThreadLocalFixedSeedRandom.current();

				long desiredThroughputBytesPerSecondMax = desiredThroughputBytesPerSecond;
				long desiredThroughputBytesPerSecondMin = varyingWorkload ? 1024 * 1024 : desiredThroughputBytesPerSecond; // 1 MB/s
				long throughputDelta = varyingWorkload ? 512 * 1024 : 0;
				long currentThroughput = desiredThroughputBytesPerSecondMin;
				long throughputChangeTimestamp = 0;

				RateLimiter throughputThrottler = RateLimiter.create(currentThroughput, 5, TimeUnit.SECONDS);
				LOG.debug("Create throughputThrottler for {} -> << {} MB/sec : {} MB/sec >>",
						workerId, desiredThroughputBytesPerSecondMin / ONE_MEGABYTE, desiredThroughputBytesPerSecondMax / ONE_MEGABYTE);
				int checksum = 0xdeedf000;
				long pendingRecords = recordsToGenerate;
				long sentBytesDelta = 0;

				cachedBuffers = new ArrayBlockingQueue<>(CACHED_BUFFERS);
				for (int i = 0; i < CACHED_BUFFERS; i++) {
					ByteBuffer buff = ByteBuffer.allocate(BUFFER_SIZE);
					cachedBuffers.add(buff);
				}

				long sentRecords = 0;
				long eventId = 0;

				long numBuffersToGenerate = (long) Math.ceil((((double) recordsToGenerate) / itemsPerBuffer));
				for (long i = 0; i < numBuffersToGenerate; i++) {

					ByteBuffer buf = cachedBuffers.take();
					buf.putInt(checksum);
					int itemsInThisBuffer = (int) Math.min(itemsPerBuffer, pendingRecords);
					long backlog = pendingRecords - itemsInThisBuffer;
					buf.putInt(itemsInThisBuffer);
					buf.putLong(backlog);

					for (int j = 0; j < itemsInThisBuffer; j++) {
						final long ts = System.currentTimeMillis();
						final long campaign_id = randomness.nextInt(10000);
						final long event_type = eventId % 3;

						buf.putLong(1);				// user id
						buf.putLong(0);				// page id
						buf.putLong(campaign_id);	// campaign id
						buf.putLong(0);				// ad type
						buf.putLong(event_type);	// event type
						buf.putLong(ts);			// current ms
						buf.putLong(192168001);		// ip
						buf.putLong(1);				// d1
						buf.putLong(1);				// d2
//						buf.putInt(1);				// d3
//						buf.putShort((short) 1);	// d4
						buf.putLong(1);				// d3 Long
						buf.putLong(1);				// d4 Long

					}

					ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameYSB, targetPartition, genId, buf);
					kafkaProducerYSB.send(kafkaRecord, new InternalCallback(cachedBuffers, buf, sharedCounterSentRecords, itemsInThisBuffer));
					pendingRecords -= itemsInThisBuffer;

					/*
					todo do we need this stuff
					bufP.position(bufP.position() + bufP.remaining());
					sentPersons += itemsInThisBufferP;
					 */

					sentBytes += BUFFER_SIZE;
					sentBytesDelta += BUFFER_SIZE;
					throughputThrottler.acquire(BUFFER_SIZE);

					final long timestamp = System.currentTimeMillis();

					if (sentBytesDelta > LOGGING_THRESHOLD) {
						LOG.info("{} has just sent {} MB to kafka in {} sec - rate limiter {} bytes/sec",
								name,
								sentBytes / ONE_MEGABYTE,
								(timestamp - startMs) / 1_000,
								throughputThrottler.getRate());
						sentBytesDelta = 0;
					}

//					if ((timestamp - throughputChangeTimestamp) > 10_000) {
//						currentThroughput += throughputDelta;
//						if (currentThroughput > desiredThroughputBytesPerSecondMax) {
//							throughputDelta = -throughputDelta;
//						} else if (currentThroughput < desiredThroughputBytesPerSecondMin) {
//							currentThroughput = desiredThroughputBytesPerSecondMin;
//							throughputDelta = -throughputDelta;
//						}
//						throughputThrottler.setRate(currentThroughput);
//						throughputChangeTimestamp = timestamp;
//						LOG.debug("Throttler changed to {}", currentThroughput);
//					}

				}

				while (!sharedCounterSentRecords.compareAndSet(sentRecords, 0)) { // todo not sure about the workings
					Thread.sleep(100);
				}
				double end = System.currentTimeMillis();
				double diff = end - startMs;
				LOG.info("{} is finished after {} msec and {} GBs and {} items with an overall throughput of {}",
						name,
						diff,
						sentBytes / ONE_GIGABYTE,
						recordsToGenerate,
						(sentBytes * 1_000.0) / (diff * ONE_GIGABYTE));
			} catch (Throwable error) {
				LOG.error("Error: {}", error);
			} finally {
				if (cachedBuffers != null) {
					cachedBuffers.clear();
				}
				kafkaProducerYSB.close();
				controller.countDown();
				if (future != null) {
					future.cancel(false);
				}
			}
		}
	}

	public static Thread addShutdownHook(
		final AutoCloseable service,
		final String serviceName,
		final Logger logger) {

		final Thread shutdownHook = new Thread(() -> {
			try {
				service.close();
			} catch (Throwable t) {
				logger.error("Error during shutdown of {} via JVM shutdown hook.", serviceName, t);
			}
		}, serviceName + " shutdown hook");

		return addShutdownHookThread(shutdownHook, serviceName, logger) ? shutdownHook : null;
	}

	public static boolean addShutdownHookThread(
		final Thread shutdownHook,
		final String serviceName,
		final Logger logger) {

		try {
			// Add JVM shutdown hook to call shutdown of service
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			return true;
		} catch (IllegalStateException e) {
			// JVM is already shutting down. no need to do our work
		} catch (Throwable t) {
			logger.error("Cannot register shutdown hook that cleanly terminates {}.", serviceName, t);
		}
		return false;
	}

	private static class ThroughtputLogger implements Runnable {

		private static final int UPDATE_INTERVAL_SECONDS = 5;

		private final int eventSize;
		/** The time-span over which the average is calculated. */
		private final int timeSpanInSeconds;
		/** Circular array containing the history of values. */
		private final long[] values;
		/** The index in the array for the current time. */
		private int time = 0;
		/** The last rate we computed. */
		private double currentRate = 0;

		private final AtomicLong counter;

		private final String name, topic;

		private final BufferedWriter writer;

		private final StringBuffer stringBuffer;

		private int writtenSoFar = 0;

		private final Thread cleaningHelper;

		private boolean logInit = false;

		public ThroughtputLogger(AtomicLong counter, String dir, String name, String topic, int timeSpanInSeconds, int eventSize) throws Exception {
			this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
			this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
			this.eventSize = eventSize;
			this.counter = counter;
			this.name = name;
			this.topic = topic;

			File logDir = new File(dir);
			if (!logDir.exists()) {
				logDir.mkdirs();
			}
			File logFile = new File(logDir, name + "_" + topic + ".csv");
			this.stringBuffer = new StringBuffer(8192);
			if (logFile.exists()) {
				this.writer = new BufferedWriter(new FileWriter(logFile, true));
				this.writer.write("\n");
			} else {
				this.writer = new BufferedWriter(new FileWriter(logFile, false));
				stringBuffer.append("ts,name,topic,metric,value");
				stringBuffer.append("\n");
				writer.write(stringBuffer.toString());
				writtenSoFar += stringBuffer.length() * 2;
			}

			cleaningHelper = addShutdownHook(writer, topic, LOG);

			stringBuffer.setLength(0);
			logInit = true;
		}

		@Override
		public void run() {
			long ts = System.currentTimeMillis();
			time = (time + 1) % values.length;
			values[time] = counter.get();
			currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
			double throughputGBs = currentRate * eventSize / ONE_GIGABYTE;
			stringBuffer.append(ts).append(",").append(name).append(",").append(topic).append(",recordsPerSec,").append(currentRate).append("\n");
			stringBuffer.append(ts).append(",").append(name).append(",").append(topic).append(",gbps,").append(throughputGBs).append("\n");
//			LOG.info("METRICS - {}: generator.nexmark.{}.{}.recordsPerSec: {}", ts, name, topic, currentRate);
//			LOG.info("METRICS - {}: generator.nexmark.{}.{}.gbps: {}", ts, name, topic, throughputGBs);
			try {
				writer.write(stringBuffer.toString());
				writtenSoFar += stringBuffer.length() * 2;
				if (writtenSoFar >= (8 * 1024 * 1024)) {
					try {
						writer.flush();
					} catch (IOException e) {
					}
					writtenSoFar = 0;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				stringBuffer.setLength(0);
			}
		}
	}

	public static class InternalCallback implements Callback {

		private final Queue<ByteBuffer> destination;
		private final ByteBuffer buffer;
		private final AtomicLong sharedCounter;
		private final int delta;

		public InternalCallback(Queue<ByteBuffer> destination, ByteBuffer buffer, AtomicLong sharedCounter, int delta) {
			this.destination = destination;
			this.buffer = buffer;
			this.sharedCounter = sharedCounter;
			this.delta = delta;
		}

		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			buffer.clear();
			destination.offer(buffer);
			sharedCounter.addAndGet(delta);
		}
	}

	public static String readProperty(final String key, String def) {
		if (key == null) {
			throw new NullPointerException("key");
		} else if (key.isEmpty()) {
			throw new IllegalArgumentException("key must not be empty.");
		} else {
			String value = null;

			try {
				if (System.getSecurityManager() == null) {
					value = System.getProperty(key);
				} else {
					value = (String) AccessController.doPrivileged(new PrivilegedAction<String>() {
						public String run() {
							return System.getProperty(key);
						}
					});
				}
			} catch (SecurityException var4) {
				LOG.warn("Unable to retrieve a system property '{}'; default values will be used.", key, var4);
			}

			return value == null ? def : value;
		}
	}

}
