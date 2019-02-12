package io.ventura.generators.nexmark;

import com.beust.jcommander.JCommander;
import com.google.common.util.concurrent.RateLimiter;
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

import javax.annotation.Nonnull;
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
import java.util.concurrent.atomic.AtomicLongArray;

public class KafkaNexmarkGenerator {

	private static final long ONE_KILOBYTE = 1024L;
	private static final long ONE_MEGABYTE = 1024L * 1024L;
	private static final long ONE_GIGABYTE = 1024L * 1024L * 1024L;

	private static final long LOGGING_THRESHOLD = 128 * ONE_MEGABYTE;

	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Generator Thread Group");

	private static final Logger LOG = LoggerFactory.getLogger(KafkaNexmarkGenerator.class);

	private static HashMap<String, int[]> PERSONS_PARTITIONS_RANGES = new HashMap<>();
	private static HashMap<String, int[]> AUCTIONS_PARTITIONS_RANGES = new HashMap<>();

	static {
		// PAY ATTENTION HERE

		PERSONS_PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});

		PERSONS_PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-23-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31, 32 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-23-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		PERSONS_PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		PERSONS_PARTITIONS_RANGES.put("cloud-23-16", new int[] {  4,  5,  6,  7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		PERSONS_PARTITIONS_RANGES.put("cloud-23-20", new int[] {  4,  5,  6,  7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });

		AUCTIONS_PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-23-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-23-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });


		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-23-16", new int[] {  4,  5,  6,  7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-23-20", new int[] {  4,  5,  6,  7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });

		AUCTIONS_PARTITIONS_RANGES.put(new String(RandomStrings.RANDOM_STRINGS_NAME[0]), null); // DO NOT REMOVE! This is needed to init RandomStrings from the main thread first
	}

	private static final long MAX_PERSON_ID = 260_000_000L;
	private static final long MAX_AUCTION_ID = 20_000_000_000L;

	public static void main(String[] args) {

		GeneratorParameters params = new GeneratorParameters();

		JCommander.newBuilder()
				.addObject(params)
				.acceptUnknownOptions(false)
				.build()
				.parse(args);


		ExecutorService workers = Executors.newFixedThreadPool(params.personsWorkers + params.auctionsWorkers);

		LOG.info("Ready to start Nexmark generator with {} partitions and {} workers for persons topic ({} GB) and {} partitions and {} workers for auctions ({} GB) -- generator {} kafkaServers {}",
				params.personsPartition,
				params.personsWorkers,
				params.inputSizeItemsPersons,
				params.auctionsPartition,
				params.auctionsWorkers,
				params.inputSizeItemsAuctions,
				params.hostname,
				params.kafkaServers);

		Properties cfg = new Properties();

		cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteBufferSerializer");
		cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaServers);
		cfg.put(ProducerConfig.RETRIES_CONFIG, 0);
		cfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, params.kafkaBufferMemory);
		cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, params.kafkaBatchSize);
		cfg.put(ProducerConfig.ACKS_CONFIG, "all");
		cfg.put(ProducerConfig.LINGER_MS_CONFIG, "100");

		int[] partitionsPersons = PERSONS_PARTITIONS_RANGES.get(params.hostname + "-" + params.personsPartition);
		int[] partitionsAuctions = AUCTIONS_PARTITIONS_RANGES.get(params.hostname + "-" + params.auctionsPartition);

		HashMap<String, Long> helper = new HashMap<>();

		helper.put("cloud-14", 0l);
		helper.put("cloud-23", 1l);
		helper.put("cloud-24", 2l);
		helper.put("cloud-25", 3l);
		helper.put("cloud-33", 4l);

		helper.put("localhost", 3L);

		long personStride = MAX_PERSON_ID / 5L;
		long personStart = personStride * helper.get(params.hostname);
		long personEnd = personStart + personStride;

		long auctionStride = MAX_AUCTION_ID / 5L;
		long auctionStart = auctionStride * helper.get(params.hostname);
		long auctionEnd = auctionStart + auctionStride;

		try {
			CountDownLatch starter = new CountDownLatch(params.personsWorkers + params.auctionsWorkers);
			CountDownLatch controller = new CountDownLatch(params.personsWorkers + params.auctionsWorkers);
			CountDownLatch fairStarter = new CountDownLatch(1);
			AtomicLongArray personsUpperBound = new AtomicLongArray(params.auctionsWorkers);
			long threadStridePerson = (personEnd - personStart) / params.personsWorkers;
			long threadStrideAuction = (auctionEnd - auctionStart) / params.personsWorkers;
			for (int j = 0; j < params.personsWorkers; j++) {
				Properties workerConfig = (Properties) cfg.clone();
				workerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkPersonsGen-" + j);
				personsUpperBound.set(j, -1L);
				long start = threadStridePerson * j;
				long end = start + threadStridePerson;
				workers.submit(new PersonsGenerator(j, params.hostname, partitionsPersons, new KafkaProducer<>(workerConfig), params.inputSizeItemsPersons, starter, controller, fairStarter, params.desiredPersonsThroughputKBSec, start, end, personsUpperBound));
			}

			for (int j = 0; j < params.auctionsWorkers; j++) {
				Properties workerConfig = (Properties) cfg.clone();
				workerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkAuctiosGen-" + j);
				long start = threadStrideAuction * j;
				long end = start + threadStridePerson;
				workers.submit(new AuctionsGenerator(j, params.hostname, partitionsAuctions, new KafkaProducer<>(workerConfig), params.inputSizeItemsAuctions, starter, controller, fairStarter, params.desiredAuctionsThroughputKBSec, start, end, personsUpperBound));
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

	private static final String PERSONS_TOPIC = "nexmark_persons";
	private static final String AUCTIONS_TOPIC = "nexmark_auctions";

	private static final int BUFFER_SIZE = 8192;

	private static final int CACHED_BUFFERS = 8192 * 2;

	private static final int METADATA_SIZE = 4 + 4 + 8;

	private final static int PERSON_RECORD_SIZE = 206;
	private final static int AUCTION_RECORD_SIZE = 269;

	public static class AuctionsGenerator extends AbstractGenerator {

		private static final int MAX_AUCTION_LENGTH_MSEC = 24 * 60 * 60 * 1_000; // 24 hours
		private static final int MIN_AUCTION_LENGTH_MSEC = 2 * 60 * 60 * 1_000; // 2 hours

		private final long minAuctionId, maxAuctionId;

		private long currentIndex;

		private final AtomicLongArray personsUpperBound;

		AuctionsGenerator(
				int workerId,
				String hostname,
				int[] partitions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducer,
				long inputSizeItemsPersons,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputMBSec,
				long start,
				long end,
				AtomicLongArray personsUpperBound) {
			super(workerId, AUCTIONS_TOPIC, hostname + ".auctions." + workerId, partitions, kafkaProducer, inputSizeItemsPersons, starter, controller, fairStarter, desiredThroughputMBSec);
			this.minAuctionId = start;
			this.maxAuctionId = end;
			this.currentIndex = start;
			this.personsUpperBound = personsUpperBound;
		}

		@Override
		public int itemSize() {
			return AUCTION_RECORD_SIZE;
		}

		@Override
		public void writeItem(long itemId, ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			long currPerson;
			do {
				currPerson = personsUpperBound.get(this.workerId);
			} while (currPerson <= 0);
//			long now = System.nanoTime() / 1_000_000;
			long nowMillis = System.currentTimeMillis();
			if (currentIndex == maxAuctionId) {
				currentIndex = minAuctionId;
			}
			long auctionId = currentIndex++;//r.nextLong(minAuctionId, maxAuctionId);
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

		private final long minPersonId, maxPersonId;

		private long currentIndex, maxSoFar;

		private long minIndexSoFar;

		private final AtomicLongArray personUpperBound;

		PersonsGenerator(
				int workerId,
				String hostname,
				int[] partitions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducer,
				long inputSizeItemsPersons,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputKBSec,
				long start,
				long end,
				AtomicLongArray personUpperBound) {

			super(workerId, PERSONS_TOPIC, hostname + ".persons." + workerId, partitions, kafkaProducer, inputSizeItemsPersons, starter, controller, fairStarter, desiredThroughputKBSec);

			this.personUpperBound = personUpperBound;
			this.minPersonId = start;
			this.maxPersonId = end;
			this.currentIndex = this.maxSoFar = this.minIndexSoFar = start;
		}

		@Override
		public int itemSize() {
			return PERSON_RECORD_SIZE;
		}

		@Override
		public void writeItem(long itemId, ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			int ifn = r.nextInt(Firstnames.NUM_FIRSTNAMES);
			int iln = r.nextInt(Lastnames.NUM_LASTNAMES);
			int iem = r.nextInt(Emails.NUM_EMAILS);
			int ict = r.nextInt(Countries.NUM_COUNTRIES);
			int icy = r.nextInt(Cities.NUM_CITIES);
//			buf.putLong(currentPersonId.getAndIncrement()); // 8
//			currentPersonId.compareAndSet(end, start);
			long personId = r.nextLong(minIndexSoFar, ++currentIndex);
			maxSoFar = Math.max(currentIndex, maxSoFar);
			if (currentIndex % 1_000 == 0) {
				minIndexSoFar++;
			}
			if (currentIndex == maxPersonId) {
				minIndexSoFar = currentIndex = minPersonId;
			}
			personUpperBound.lazySet(workerId, maxSoFar);
			buf.putLong(personId);
			buf.put(Firstnames.FIRSTNAMES_32[ifn]);
			for (int j = 0, skip = 32 - Firstnames.FIRSTNAMES_32[ifn].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 40
			buf.put(Lastnames.LASTNAMES_32[iln]);
			for (int j = 0, skip = 32 - Lastnames.LASTNAMES_32[iln].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 72
			buf.put(Emails.EMAILS_32[iem]);
			for (int j = 0, skip = 32 - Emails.EMAILS_32[iem].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 104
			buf.put(Cities.CITIES_32[icy]);
			for (int j = 0, skip = 32 - Cities.CITIES_32[icy].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 136
			buf.put(Countries.COUNTRIES_32[ict]);
			for (int j = 0, skip = 32 - Countries.COUNTRIES_32[ict].length; j < skip; j++) {
				buf.put((byte) 0x00);
			} // 168
			buf.putLong(r.nextLong()); // 176
			buf.putLong(r.nextLong(9000) + 1000); // 184
			buf.putInt(r.nextInt(9000) + 1000); // 188
			buf.putInt(r.nextInt(50) + 18); // 192
			buf.putInt(r.nextInt(9000) + 1000); // 196
			buf.putShort((short) (r.nextBoolean() ? 0 : 1)); // 198
			buf.putLong(System.currentTimeMillis()); // 206
		}

		@Override
		public int genChecksum() {
			return 0x30011991;
		}

	}

	public static abstract class AbstractGenerator implements Runnable {

		protected final int workerId;
		private final String name, topicName;
		private final byte[] genId;
		private final int targetPartition;
		private final long inputSizeItemsPersons;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducer;
		private final CountDownLatch controller, starter;
		private final long desiredThroughputBytesPerSecond;
		private final CountDownLatch fairStarter;


		AbstractGenerator(
				int workerId,
				String topicName,
				String name,
				int[] partitions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducer,
				long inputSizeItemsPersons,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputKBSec) {
			this.inputSizeItemsPersons = inputSizeItemsPersons * ONE_GIGABYTE;
			this.workerId = workerId;
			this.kafkaProducer = kafkaProducer;
			this.controller = controller;
			this.starter = starter;
			this.topicName = topicName;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = partitions[workerId];
			this.name = name;
			this.fairStarter = fairStarter;
			ByteBuffer b = ByteBuffer.wrap(genId);
			b.putInt(workerId);
			b.putLong(Long.reverse(System.nanoTime()) ^ System.currentTimeMillis());
			b.putInt(ThreadLocalFixedSeedRandom.current().nextInt());
			this.desiredThroughputBytesPerSecond = ONE_KILOBYTE * desiredThroughputKBSec;
		}

		public abstract int itemSize();

		public abstract void writeItem(long itemId, ThreadLocalFixedSeedRandom r, ByteBuffer buf);

		public abstract int genChecksum();

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

				// tasks should be removed if the future is canceled
				executor.setRemoveOnCancelPolicy(true);

				// make sure shutdown removes all pending tasks
				executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
				executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

				cachedBuffers = new ArrayBlockingQueue<>(CACHED_BUFFERS);
				for (int i = 0; i < CACHED_BUFFERS; i++) {
					cachedBuffers.offer(ByteBuffer.allocate(BUFFER_SIZE));
				}

				long recordsToGenerate = inputSizeItemsPersons / itemSize();
				int itemsPerBuffer = (BUFFER_SIZE - METADATA_SIZE) / itemSize();

				AtomicLong sharedCounter = new AtomicLong();

				starter.countDown();
				fairStarter.await();

				future = executor.scheduleAtFixedRate(new ThroughtputLogger(sharedCounter, name, 5, itemSize()), 5, 5, TimeUnit.SECONDS);

				double startNs = System.nanoTime();
				long sentBytes = 0;
				long sentItems = 0;
//				ThroughputThrottler throughputThrottler = new ThroughputThrottler(desiredThroughputBytesPerSecond, ((long) startNs) / 1_000_000);
				ThreadLocalFixedSeedRandom randomness = ThreadLocalFixedSeedRandom.current();

				RateLimiter throughputThrottler = RateLimiter.create(desiredThroughputBytesPerSecond);
				int chk = genChecksum();

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
						writeItem(i, randomness, buf);
					}
					buf.position(buf.position() + buf.remaining());
					ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicName, targetPartition, genId, buf);
					throughputThrottler.acquire(BUFFER_SIZE);
					kafkaProducer.send(kafkaRecord, new InternalCallback(cachedBuffers, buf, sharedCounter, itemsPerBuffer));
					sentBytes += BUFFER_SIZE;
					sentItems += itemsPerBuffer;
//					long nowMs = System.nanoTime() / 1_000_000;
//					throughputThrottler.throttleIfNeeded(sentBytes, nowMs);
					sentBytesDelta += BUFFER_SIZE;
					if (sentBytesDelta > LOGGING_THRESHOLD) {
						long nowMs = System.nanoTime() / 1_000_000;
						LOG.info("{} has just sent {} MB to kafka in {}",
								name,
								sentBytes / ONE_MEGABYTE,
								(nowMs - (startNs / 1_000_000) / 1_000));
						sentBytesDelta = 0;
					}
				}
				while (!sharedCounter.compareAndSet(sentItems, 0)) {
					Thread.sleep(100);
				}
				double end = System.nanoTime();
				double diff = end - startNs;
				LOG.info("{} is finished after {} msec and {} GBs and {} items with an overall throughput of {}",
						name,
						diff / 1_000_000,
						sentBytes / ONE_GIGABYTE,
						recordsToGenerate,
						(sentBytes * 1_000_000_000.0) / (diff * ONE_GIGABYTE));
			} catch (Throwable error) {
				LOG.error("Error: {}", error);
			} finally {
				if (cachedBuffers != null) {
					cachedBuffers.clear();
				}
				kafkaProducer.close();
				controller.countDown();
				if (future != null) {
					future.cancel(false);
				}
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
