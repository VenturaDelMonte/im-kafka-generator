package io.ventura.generators.nexmark;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
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
		PERSONS_PARTITIONS_RANGES.put("cloud-37-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31, 32 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-37-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		PERSONS_PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		PERSONS_PARTITIONS_RANGES.put("cloud-37-16", new int[] {  4,  5,  6,  7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		PERSONS_PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		PERSONS_PARTITIONS_RANGES.put("cloud-37-20", new int[] {  4,  5,  6,  7 });
		PERSONS_PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		PERSONS_PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		PERSONS_PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });

		AUCTIONS_PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-37-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-37-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-37-16", new int[] {  4,  5,  6,  7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		AUCTIONS_PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-37-20", new int[] {  4,  5,  6,  7 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		AUCTIONS_PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });


		// GCP
		PERSONS_PARTITIONS_RANGES.put("im-generator-01-16", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		PERSONS_PARTITIONS_RANGES.put("im-generator-02-16", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });

		AUCTIONS_PARTITIONS_RANGES.put("im-generator-01-16", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		AUCTIONS_PARTITIONS_RANGES.put("im-generator-02-16", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });


		AUCTIONS_PARTITIONS_RANGES.put(new String(RandomStrings.RANDOM_STRINGS_NAME[0]), null); // DO NOT REMOVE! This is needed to init RandomStrings from the main thread first
	}

	private static final long MAX_PERSON_ID = 1_000_000_000L;
	private static final long MAX_AUCTION_ID = 80_000_000_000L;

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
		helper.put("cloud-37", 1l);
		helper.put("cloud-37", 1l);
		helper.put("cloud-24", 2l);
		helper.put("cloud-25", 3l);
		helper.put("cloud-33", 4l);

		helper.put("im-generator-01", 0l);
		helper.put("im-generator-02", 1l);
		helper.put("im-generator-03", 2l);

		helper.put("localhost", 0L);

		long personStride = MAX_PERSON_ID / 5L;
		long personStart = personStride * helper.get(params.hostname);
		long personEnd = personStart + personStride;

		long auctionStride = MAX_AUCTION_ID / 5L;
		long auctionStart = auctionStride * helper.get(params.hostname);
		long auctionEnd = auctionStart + auctionStride;

		try {
			int totalWorkers = params.auctionsWorkers + params.personsWorkers;
			CountDownLatch starter = new CountDownLatch(totalWorkers);
			CountDownLatch controller = new CountDownLatch(totalWorkers);
			CountDownLatch fairStarter = new CountDownLatch(1);
			long threadStridePerson = (personEnd - personStart) / totalWorkers;
			long threadStrideAuction = (auctionEnd - auctionStart) / totalWorkers;
			for (int j = 0; j < totalWorkers; j++) {
				Properties workerConfigPerson = (Properties) cfg.clone();
				workerConfigPerson.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkPersonsGen-" + j);

				Properties workerConfigAuction = (Properties) cfg.clone();
				workerConfigAuction.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkAuctiosGen-" + j);

				long startP = threadStridePerson * j;
				long endP = startP + threadStridePerson;

				long startA = threadStrideAuction * j;
				long endA = startA + threadStrideAuction;

				PersonsGenerator p = new PersonsGenerator(startP, endP);
				AuctionsGenerator a = new AuctionsGenerator(startA, endA, startP);

				KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons = new KafkaProducer<>(workerConfigPerson);
				KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions = new KafkaProducer<>(workerConfigAuction);

				int targetPartition = partitionsPersons[j];

				GeneratorRunner runner = new GeneratorRunner(
						j,
						PERSONS_TOPIC,
						AUCTIONS_TOPIC,
						params.hostname,
						targetPartition,
						a,
						p,
						kafkaProducerPersons,
						kafkaProducerAuctions,
						params.inputSizeItemsPersons + params.inputSizeItemsAuctions,
						starter,
						controller,
						fairStarter,
						params.desiredAuctionsThroughputKBSec
				);

				workers.submit(runner);
			}

//			for (int j = 0; j < params.auctionsWorkers; j++) {
//				Properties workerConfig = (Properties) cfg.clone();
//				workerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkAuctiosGen-" + j);
//				long start = threadStrideAuction * j;
//				long end = start + threadStrideAuction;
//				workers.submit(new AuctionsGenerator(j, params.hostname, partitionsAuctions, new KafkaProducer<>(workerConfig), params.inputSizeItemsAuctions, starter, controller, fairStarter, params.desiredAuctionsThroughputKBSec, start, end, personsLowerBound, personsUpperBound));
//			}
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

	private interface AbstractGenerator {
		int itemSize();

		void writeItem(long itemId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf);

		int genChecksum();
	}

	public static class AuctionsGenerator implements AbstractGenerator {

		private static final int MAX_AUCTION_LENGTH_MSEC = 24 * 60 * 60 * 1_000; // 24 hours
		private static final int MIN_AUCTION_LENGTH_MSEC = 2 * 60 * 60 * 1_000; // 2 hours

		private static final int HOT_SELLER_RATIO = 100;
		private final long minAuctionId, maxAuctionId;
		private final long minPersonId;

		AuctionsGenerator(
				long startAuction,
				long endAuction,
				long minPersonId) {
			this.minAuctionId = startAuction;
			this.maxAuctionId = endAuction;
			this.minPersonId = minPersonId;
		}

		@Override
		public int itemSize() {
			return AUCTION_RECORD_SIZE;
		}

		@Override
		public void writeItem(long eventId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			long epoch = eventId / TOTAL_EVENT_RATIO;
			long offset = eventId % TOTAL_EVENT_RATIO;
//			long now = System.nanoTime() / 1_000_000;
			long nowMillis = System.currentTimeMillis();

			if (offset < PERSON_EVENT_RATIO) {
				epoch--;
				offset = AUCTION_EVENT_RATIO - 1;
			} else {
				offset = AUCTION_EVENT_RATIO - 1;
			}
			long auctionId = minAuctionId + epoch * AUCTION_EVENT_RATIO + offset;//r.nextLong(minAuctionId, maxAuctionId);

			epoch = eventId / TOTAL_EVENT_RATIO;
			offset = eventId % TOTAL_EVENT_RATIO;

			if (offset >= PERSON_EVENT_RATIO) {
				offset = PERSON_EVENT_RATIO - 1;
			}
			long matchingPerson;
			if (r.nextInt(100) > 20) {
				long personId = epoch * PERSON_EVENT_RATIO + offset;
				matchingPerson = minPersonId + (personId / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
			} else {
				long personId = epoch * PERSON_EVENT_RATIO + offset + 1;
				long activePersons = Math.min(personId, 20_000);
				long n = r.nextLong(activePersons + 100);
				matchingPerson = minPersonId + personId + activePersons - n;
			}
//
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

	public static class PersonsGenerator implements AbstractGenerator {

		private final long minPersonId, maxPersonId;

		PersonsGenerator(
				long start,
				long end) {

			this.minPersonId = start;
			this.maxPersonId = end;
		}

		@Override
		public int itemSize() {
			return PERSON_RECORD_SIZE;
		}

		@Override
		public void writeItem(long eventId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			int ifn = r.nextInt(Firstnames.NUM_FIRSTNAMES);
			int iln = r.nextInt(Lastnames.NUM_LASTNAMES);
			int iem = r.nextInt(Emails.NUM_EMAILS);
			int ict = r.nextInt(Countries.NUM_COUNTRIES);
			int icy = r.nextInt(Cities.NUM_CITIES);

			long epoch = eventId / TOTAL_EVENT_RATIO;
			long offset = eventId % TOTAL_EVENT_RATIO;

			if (offset >= PERSON_EVENT_RATIO) {
				offset = PERSON_EVENT_RATIO - 1;
			}

			long personId = minPersonId + epoch * PERSON_EVENT_RATIO + offset;

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

	private static final long PERSON_EVENT_RATIO = 1;
	private static final long AUCTION_EVENT_RATIO = 4;
	private static final long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO;

	public static class GeneratorRunner implements Runnable {

		protected final int workerId;
		private final String name, topicNamePerson, topicNameAuction;
		private final byte[] genId;
		private final int targetPartition;
		private final long targetPartitionSize;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions;
		private final CountDownLatch controller, starter;
		private final long desiredThroughputBytesPerSecond;
		private final CountDownLatch fairStarter;

		private final AuctionsGenerator auctionsGenerator;
		private final PersonsGenerator personsGenerator;

		GeneratorRunner(
				int workerId,
				String topicNamePerson,
				String topicNameAuction,
				String name,
				int targetPartition,
				AuctionsGenerator auctionsGenerator,
				PersonsGenerator personsGenerator,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions,
				long targetPartitionSize,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputKBSec) {
			this.targetPartitionSize = targetPartitionSize * ONE_GIGABYTE;
			this.auctionsGenerator = auctionsGenerator;
			this.topicNameAuction = topicNameAuction;
			this.personsGenerator = personsGenerator;
			this.workerId = workerId;
			this.kafkaProducerPersons = kafkaProducerPersons;
			this.kafkaProducerAuctions = kafkaProducerAuctions;
			this.controller = controller;
			this.starter = starter;
			this.topicNamePerson = topicNamePerson;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = targetPartition;
			this.name = name;
			this.fairStarter = fairStarter;
			this.desiredThroughputBytesPerSecond = ONE_KILOBYTE * desiredThroughputKBSec;
		}

//		public abstract int itemSize();
//
//		public abstract void writeItem(long itemId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf);
//
//		public abstract int genChecksum();

		@Override
		public void run() {
			ArrayBlockingQueue<ByteBuffer> cachedBuffers = null;
			ScheduledFuture<?> futureP = null;
			ScheduledFuture<?> futureA = null;
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
					cachedBuffers.add(ByteBuffer.allocate(BUFFER_SIZE));
				}

				int personSize = personsGenerator.itemSize();
				int auctionSize = auctionsGenerator.itemSize();

				long ratio = targetPartitionSize / TOTAL_EVENT_RATIO;
				long personsToGenerate = (PERSON_EVENT_RATIO * ratio) / personSize;
				long auctionsToGenerate = (AUCTION_EVENT_RATIO * ratio) / auctionSize;
				long recordsToGenerate = personsToGenerate + auctionsToGenerate;

				int itemsPerBufferPerson = (BUFFER_SIZE - METADATA_SIZE) / personSize;
				int itemsPerBufferAuction = (BUFFER_SIZE - METADATA_SIZE) / auctionSize;

				AtomicLong sharedCounterPerson = new AtomicLong();
				AtomicLong sharedCounterAuction = new AtomicLong();

				starter.countDown();
				fairStarter.await();

				futureP = executor.scheduleAtFixedRate(new ThroughtputLogger(sharedCounterPerson, name, topicNamePerson + "-" + workerId, 5, personSize), 5, 5, TimeUnit.SECONDS);
				futureA = executor.scheduleAtFixedRate(new ThroughtputLogger(sharedCounterAuction, name, topicNameAuction + "-" + workerId,5, auctionSize), 6, 5, TimeUnit.SECONDS);

				double startNs = System.nanoTime();
				long sentBytes = 0;
//				ThroughputThrottler throughputThrottler = new ThroughputThrottler(desiredThroughputBytesPerSecond, ((long) startNs) / 1_000_000);
				ThreadLocalFixedSeedRandom randomness = ThreadLocalFixedSeedRandom.current();

				RateLimiter throughputThrottler = RateLimiter.create(desiredThroughputBytesPerSecond);
				int chkP = personsGenerator.genChecksum();
				int chkA = auctionsGenerator.genChecksum();
				long pendingPerson = (recordsToGenerate / TOTAL_EVENT_RATIO) * PERSON_EVENT_RATIO;
				long pendingAuctions = (recordsToGenerate / TOTAL_EVENT_RATIO) * AUCTION_EVENT_RATIO;
				long sentBytesDelta = 0;
				ByteBuffer bufP = cachedBuffers.take();
				ByteBuffer bufA = cachedBuffers.take();
				bufA.putInt(chkA);
				bufP.putInt(chkP);
				int itemsInThisBufferA = (int) Math.min(itemsPerBufferAuction, pendingAuctions);
				int itemsInThisBufferP = (int) Math.min(itemsPerBufferPerson, pendingPerson);
				long backlogPerson = pendingPerson - itemsInThisBufferP;
				long backlogAuction = pendingAuctions - itemsInThisBufferA;
				bufP.putInt(itemsInThisBufferP);
				bufP.putLong(backlogPerson);
				bufA.putInt(itemsInThisBufferA);
				bufA.putLong(backlogAuction);

				long sentPersons = 0;
				long sentAuctions = 0;
				long eventId = 0;

				long prevBP = backlogPerson;
				long prevBA = backlogAuction;
				for (; eventId < recordsToGenerate; eventId++) {

					final long timestamp = System.nanoTime();

					long rem = eventId % TOTAL_EVENT_RATIO;
					if (rem < PERSON_EVENT_RATIO) {
						personsGenerator.writeItem(eventId, timestamp, randomness, bufP);
						pendingPerson--;
						if (bufP.remaining() < personSize) {
							bufP.position(bufP.position() + bufP.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNamePerson, targetPartition, genId, bufP);
							kafkaProducerPersons.send(kafkaRecord, new InternalCallback(cachedBuffers, bufP, sharedCounterPerson, itemsInThisBufferP));
							sentPersons += itemsInThisBufferP;
							bufP = cachedBuffers.take();
							bufP.putInt(chkP);
							itemsInThisBufferP = (int) Math.min(itemsPerBufferPerson, pendingPerson);
							backlogPerson = pendingPerson - itemsInThisBufferP;
							Preconditions.checkArgument(backlogPerson < prevBP);
							bufP.putInt(itemsInThisBufferP);
							bufP.putLong(backlogPerson);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
						}
					} else {
						auctionsGenerator.writeItem(eventId, timestamp, randomness, bufA);
						pendingAuctions--;
						if (bufA.remaining() < auctionSize) {
							bufA.position(bufA.position() + bufA.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameAuction, targetPartition, genId, bufA);
							kafkaProducerAuctions.send(kafkaRecord, new InternalCallback(cachedBuffers, bufA, sharedCounterAuction, itemsInThisBufferA));
							sentAuctions += itemsInThisBufferA;
							bufA = cachedBuffers.take();
							bufA.putInt(chkA);
							itemsInThisBufferA = (int) Math.min(itemsPerBufferAuction, pendingAuctions);
							backlogAuction = pendingAuctions - itemsInThisBufferA;
							Preconditions.checkArgument(backlogAuction < prevBA);
							bufA.putInt(itemsInThisBufferA);
							bufA.putLong(backlogAuction);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
						}
					}
//					long nowMs = System.nanoTime() / 1_000_000;
//					throughputThrottler.throttleIfNeeded(sentBytes, nowMs);

					if (sentBytesDelta > LOGGING_THRESHOLD) {
						long nowMs = timestamp / 1_000_000;
						LOG.info("{} has just sent {} MB to kafka in {}",
								name,
								sentBytes / ONE_MEGABYTE,
								(nowMs - (startNs / 1_000_000) / 1_000));
						sentBytesDelta = 0;
					}
					throughputThrottler.acquire(BUFFER_SIZE);
				}
				while (!sharedCounterPerson.compareAndSet(sentPersons, 0)) {
					Thread.sleep(100);
				}
				while (!sharedCounterAuction.compareAndSet(sentAuctions, 0)) {
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
				kafkaProducerAuctions.close();
				kafkaProducerPersons.close();
				controller.countDown();
				if (futureA != null) {
					futureA.cancel(false);
				}
				if (futureP != null) {
					futureP.cancel(false);
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

		private final String name, topic;

		public ThroughtputLogger(AtomicLong counter, String name, String topic, int timeSpanInSeconds, int eventSize) {
			this.timeSpanInSeconds = timeSpanInSeconds - (timeSpanInSeconds % UPDATE_INTERVAL_SECONDS);
			this.values = new long[this.timeSpanInSeconds / UPDATE_INTERVAL_SECONDS + 1];
			this.eventSize = eventSize;
			this.counter = counter;
			this.name = name;
			this.topic = topic;
		}

		@Override
		public void run() {
//			long now = milliSecondFromNano();
			long ts = System.currentTimeMillis();
			time = (time + 1) % values.length;
			values[time] = counter.get();
			currentRate =  ((double) (values[time] - values[(time + 1) % values.length]) / timeSpanInSeconds);
			double throughputGBs = currentRate * eventSize / ONE_GIGABYTE;
			LOG.info("METRICS - {}: generator.nexmark.{}.{}.recordsPerSec: {}", ts, name, topic, currentRate);
			LOG.info("METRICS - {}: generator.nexmark.{}.{}.gbps: {}", ts, name, topic, throughputGBs);
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
