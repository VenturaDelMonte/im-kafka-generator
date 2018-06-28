package io.ventura.generators.nexmark;

import com.beust.jcommander.JCommander;
import io.ventura.generators.nexmark.original.Cities;
import io.ventura.generators.nexmark.original.Countries;
import io.ventura.generators.nexmark.original.Emails;
import io.ventura.generators.nexmark.original.Firstnames;
import io.ventura.generators.nexmark.original.Lastnames;
import io.ventura.generators.nexmark.original.RandomStrings;
import io.ventura.generators.nexmark.utils.ThreadLocalFixedSeedRandom;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaNexmarkGenerator {

	private static final long ONE_KILOBYTE = 1024L;
	private static final long ONE_MEGABYTE = 1024L * 1024L;
	private static final long ONE_GIGABYTE = 1024L * 1024L * 1024L;

	private static final long LOGGING_THRESHOLD = 128 * ONE_MEGABYTE;

	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Generator Thread Group");

	private static final Logger LOG = LoggerFactory.getLogger(KafkaNexmarkGenerator.class);

	private static HashMap<String, int[]> PARTITIONS_RANGES = new HashMap<>();

	static {
		// PAY ATTENTION HERE

		PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});

		PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PARTITIONS_RANGES.put("cloud-23-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31, 32 });

		PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		PARTITIONS_RANGES.put("cloud-23-16", new int[] {  4,  5,  6,  7 });
		PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		PARTITIONS_RANGES.put(new String(RandomStrings.RANDOM_STRINGS_NAME[0]), null); // DO NOT REMOVE!
	}

	public static void main(String[] args) {

		GeneratorParameters params = new GeneratorParameters();

		JCommander.newBuilder()
				.addObject(params)
				.acceptUnknownOptions(false)
				.build()
				.parse(args);


		ExecutorService workers = Executors.newFixedThreadPool(params.personsWorkers + params.auctionsWorkers);

		LOG.info("Ready to start Nexmark generator with {} partitions and {} workers for persons and {} partitions and {} workers for auctions generator {} kafkaServers {}",
				params.personsPartition, params.personsWorkers, params.auctionsPartition, params.auctionsWorkers, params.hostname, params.kafkaServers);

		Properties cfg = new Properties();

		cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferSerializer");
		cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaServers);
		cfg.put(ProducerConfig.RETRIES_CONFIG, 0);
		cfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, params.kafkaBufferMemory);
		cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, params.kafkaBatchSize);
		cfg.put(ProducerConfig.ACKS_CONFIG, "all");
		cfg.put(ProducerConfig.LINGER_MS_CONFIG, "100");

		int[] partitions = PARTITIONS_RANGES.get(params.hostname + "-" + params.personsPartition);

		try {
			CountDownLatch controller = new CountDownLatch(params.personsWorkers + params.auctionsWorkers);
			for (int j = 0; j < params.personsWorkers; j++) {
				Properties workerConfig = (Properties) cfg.clone();
				cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkPersonsGen-" + j);
				workers.submit(new PersonsGenerator(j, params.personsPartition, params.hostname, partitions, new KafkaProducer<>(workerConfig), params.inputSizeItemsPersons, controller, params.desiredPersonsThroughputKBSec));
			}

			for (int j = 0; j < params.auctionsWorkers; j++) {
				Properties workerConfig = (Properties) cfg.clone();
				cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkAuctiosGen-" + j);
				workers.submit(new AuctionsGenerator(j, params.auctionsPartition, params.hostname, partitions, new KafkaProducer<>(workerConfig), params.inputSizeItemsAuctions, controller, params.desiredAuctionsThroughputKBSec));
			}

			controller.await();
			workers.shutdown();
			workers.awaitTermination(1, TimeUnit.SECONDS);
		} catch (Throwable error) {
			LOG.error("Error while producing to kafka: {}", error.toString(), error);
		}
	}

	private static final String PERSONS_TOPIC = "nexmark_persons";
	private static final String AUCTIONS_TOPIC = "nexmark_auctions";

	private static final int BUFFER_SIZE = 8192;

	private static final int CACHED_BUFFERS = 8192 * 2;

	private static final int METADATA_SIZE = 4 + 4 + 8;

	private final static int PERSON_RECORD_SIZE = 128;
	private final static int AUCTION_RECORD_SIZE = 269;

	private static final AtomicLong currentPersonId = new AtomicLong();
	private static final AtomicLong currentAuctionId = new AtomicLong();

	public static class AuctionsGenerator extends AbstractGenerator {

		private static final int MAX_AUCTION_LENGTH_MSEC = 24 * 60 * 60 * 1_000; // 24 hours
		private static final int MIN_AUCTION_LENGTH_MSEC = 2 * 60 * 60 * 1_000; // 2 hours


		AuctionsGenerator(int workerId, int numPartition, String hostname, int[] partitions, KafkaProducer<byte[], ByteBuffer> kafkaProducer, long inputSizeItemsPersons, CountDownLatch controller, int desiredThroughputMBSec) {
			super(workerId, numPartition, AUCTIONS_TOPIC, hostname + ".auctions." + workerId, partitions, kafkaProducer, inputSizeItemsPersons, controller, desiredThroughputMBSec);
		}

		@Override
		public int itemSize() {
			return AUCTION_RECORD_SIZE;
		}

		@Override
		public void writeItem(ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			long currPerson;
			do {
				currPerson = currentPersonId.get();
			} while (currPerson <= 0);
//			long now = System.nanoTime() / 1_000_000;
			long nowMillis = System.currentTimeMillis();
			long auctionId = currentAuctionId.getAndIncrement();
			long matchingPerson = r.nextLong(currPerson);
//			OpenAuction curr = new OpenAuction(
//						now,r.nextInt(1000) + 1,
//						now + r.nextInt(MAX_AUCTION_LENGTH_MSEC) + MIN_AUCTION_LENGTH_MSEC);
////			while (true) {
//				curr = openAuctions.computeIfAbsent(auctionId, id -> new OpenAuction(
//						now,r.nextInt(1000) + 1,
//						now + r.nextInt(MAX_AUCTION_LENGTH_MSEC) + MIN_AUCTION_LENGTH_MSEC));
//				if (curr.endTime > now) {
//					break;
//				}
//				openAuctions.remove(auctionId);
//			}
			buf.putLong(auctionId); // 8
			buf.putLong(matchingPerson); // 16
			buf.put((byte) r.nextInt(64)); // 17
			buf.putInt(r.nextInt(10) + 1); // 21
			buf.putLong(nowMillis); // 29
			buf.putLong(nowMillis + r.nextInt(MAX_AUCTION_LENGTH_MSEC) + MIN_AUCTION_LENGTH_MSEC); // 37
			buf.putInt(r.nextInt(1000) + 1); // 41
			int idx = r.nextInt(RandomStrings.NUM_RANDOM_STRINGS);
			buf.put(RandomStrings.RANDOM_STRINGS_NAME[idx]); // 61
			buf.put(RandomStrings.RANDOM_STRINGS_DESCR[idx]); // 261
			buf.putLong(nowMillis); // 269
		}

		@Override
		public int genChecksum() {
			return 0x30061992;
		}
	}

	public static class PersonsGenerator extends AbstractGenerator {

		PersonsGenerator(
				int workerId,
				int numPartition,
				String hostname,
				int[] partitions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducer,
				long inputSizeItemsPersons,
				CountDownLatch controller,
				int desiredThroughputKBSec) {

			super(workerId, numPartition, PERSONS_TOPIC, hostname + ".persons." + workerId, partitions, kafkaProducer, inputSizeItemsPersons, controller, desiredThroughputKBSec);
		}

		@Override
		public int itemSize() {
			return PERSON_RECORD_SIZE;
		}

		@Override
		public void writeItem(ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			int ifn = r.nextInt(Firstnames.NUM_FIRSTNAMES);
			int iln = r.nextInt(Lastnames.NUM_LASTNAMES);
			int iem = r.nextInt(Emails.NUM_EMAILS);
			int ict = r.nextInt(Countries.NUM_COUNTRIES);
			int icy = r.nextInt(Cities.NUM_CITIES);
			buf.putLong(currentPersonId.getAndIncrement()); // 8
			buf.put(Firstnames.FIRSTNAMES_16[ifn]);
			for (int j = 0, skip = 16 - Firstnames.FIRSTNAMES_16[ifn].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 24
			buf.put(Lastnames.LASTNAMES_16[iln]);
			for (int j = 0, skip = 16 - Lastnames.LASTNAMES_16[iln].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 40
			buf.put(Emails.EMAILS_16[iem]);
			for (int j = 0, skip = 16 - Emails.EMAILS_16[iem].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 56
			buf.put(Cities.CITIES_16[icy]);
			for (int j = 0, skip = 16 - Cities.CITIES_16[icy].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 74
			buf.put(Countries.COUNTRIES_16[ict]);
			for (int j = 0, skip = 16 - Countries.COUNTRIES_16[ict].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 90
			buf.putLong(r.nextLong()); // 98
			buf.putLong(r.nextLong(9000) + 1000); // 106
			buf.putInt(r.nextInt(9000) + 1000); // 110
			buf.putInt(r.nextInt(50) + 18); // 114
			buf.putInt(r.nextInt(9000) + 1000); // 118
			buf.putShort((short) (r.nextBoolean() ? 0 : 1)); // 120
			buf.putLong(System.currentTimeMillis()); // 128
		}

		@Override
		public int genChecksum() {
			return 0x30011991;
		}

	}

	public static abstract class AbstractGenerator implements Runnable {

		private final String name, topicName;
		private final int numPartition;
		private final byte[] genId;
		private final int targetPartition;
		private final long inputSizeItemsPersons;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducer;
		private final CountDownLatch controller;
		private final long desiredThroughputBytesPerSecond;


		AbstractGenerator(
				int workerId,
				int numPartition,
				String topicName,
				String name,
				int[] partitions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducer,
				long inputSizeItemsPersons,
				CountDownLatch controller,
				int desiredThroughputKBSec) {
			this.numPartition = numPartition;
			this.inputSizeItemsPersons = inputSizeItemsPersons * ONE_GIGABYTE;
			this.kafkaProducer = kafkaProducer;
			this.controller = controller;
			this.topicName = topicName;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = partitions[workerId];
			this.name = name;
			ByteBuffer b = ByteBuffer.wrap(genId);
			b.putInt(workerId);
			b.putLong(Long.reverse(System.nanoTime()) ^ System.currentTimeMillis());
			b.putInt(ThreadLocalFixedSeedRandom.current().nextInt());
			this.desiredThroughputBytesPerSecond = ONE_KILOBYTE * desiredThroughputKBSec;
		}

		public abstract int itemSize();

		public abstract void writeItem(ThreadLocalFixedSeedRandom r, ByteBuffer buf);

		public abstract int genChecksum();

		@Override
		public void run() {

			ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(THREAD_GROUP, r);
					t.setDaemon(true);
					return t;
				}
			});

			// tasks should be removed if the future is canceled
			executor.setRemoveOnCancelPolicy(true);

			// make sure shutdown removes all pending tasks
			executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
			executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

			ArrayBlockingQueue<ByteBuffer> cachedBuffers = new ArrayBlockingQueue<>(CACHED_BUFFERS);
			for (int i = 0; i < CACHED_BUFFERS; i++) {
				cachedBuffers.offer(ByteBuffer.allocate(BUFFER_SIZE));
			}

			long recordsToGenerate = inputSizeItemsPersons / itemSize();
			int itemsPerBuffer = (BUFFER_SIZE - METADATA_SIZE) / itemSize();

			AtomicLong sharedCounter = new AtomicLong();

			ScheduledFuture<?> future = executor.scheduleAtFixedRate(new ThroughtputLogger(sharedCounter, name, 5, itemSize()), 5, 5, TimeUnit.SECONDS);

			double startNs = System.nanoTime();
			long sentBytes = 0;
			long sentItems = 0;
			ThroughputThrottler throughputThrottler = new ThroughputThrottler(desiredThroughputBytesPerSecond, ((long) startNs) / 1000000);
			ThreadLocalFixedSeedRandom randomness = ThreadLocalFixedSeedRandom.current();
			int chk = genChecksum();
			try {
				long pending = recordsToGenerate;
				long sentBytesDelta = 0;
				for (long i = 0; i < recordsToGenerate; ) {
					ByteBuffer buf = cachedBuffers.take();
					buf.putInt(chk);
					int itemsInThisBuffer = (int) Math.min(itemsPerBuffer, pending);
					long backlog = pending - itemsInThisBuffer;
					buf.putInt(itemsInThisBuffer);
					buf.putLong(backlog);
					for (int k = 0; k < itemsInThisBuffer && i < recordsToGenerate; k++, i++, pending--) {
						writeItem(randomness, buf);
					}
					buf.position(buf.position() + buf.remaining());
					ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicName, targetPartition, genId, buf);
					kafkaProducer.send(kafkaRecord, new InternalCallback(cachedBuffers, buf, sharedCounter, itemsPerBuffer));
					sentBytes += BUFFER_SIZE;
					sentItems += itemsPerBuffer;
					long nowMs = System.nanoTime() / 1000000;
					throughputThrottler.throttleIfNeeded(sentBytes, nowMs);
					sentBytesDelta += BUFFER_SIZE;
					if (sentBytesDelta > LOGGING_THRESHOLD) {
						LOG.info("{} has just sent {} MB to kafka in {}",
								name,
								sentBytes / ONE_MEGABYTE,
								(nowMs - (startNs / 1_000_000) / 1000));
						sentBytesDelta = 0;
					}
				}
				while (!sharedCounter.compareAndSet(sentItems, 0)) {
					Thread.sleep(100);
				}
				double end = System.nanoTime();
				double diff = end - startNs;
				LOG.info("{} is finished in {} msec with an overall throughput of {}", name, diff / 1_000_000, (sentBytes * 1_000_000_000.0) / (diff * ONE_GIGABYTE));
			} catch (Throwable error) {
				LOG.error("Error: {}", error);
			} finally {
				cachedBuffers.clear();
				kafkaProducer.close();
				controller.countDown();
				future.cancel(false);
			}
		}
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

		private final String name;

		public ThroughtputLogger(AtomicLong counter, String name, int timeSpanInSeconds, int eventSize) {
			this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
			this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
			this.eventSize = eventSize;
			this.counter = counter;
			this.name = name;
		}

		@Override
		public void run() {
//			long now = milliSecondFromNano();
			long ts = System.currentTimeMillis();
			time = (time + 1) % values.length;
			values[time] = counter.get();
			currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
			double throughputGBs = currentRate * eventSize / ONE_GIGABYTE;
			LOG.info("METRICS - {}: generator.nexmark.{}.recordsPerSec: {}", ts, name, currentRate);
			LOG.info("METRICS - {}: generator.nexmark.{}.gbps: {}", ts, name, throughputGBs);
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


}
