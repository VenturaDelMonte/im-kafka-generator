package io.ventura.generators.nexmark;

import com.beust.jcommander.JCommander;
import com.google.common.util.concurrent.RateLimiter;
import io.ventura.generators.nexmark.original.Cities;
import io.ventura.generators.nexmark.original.Countries;
import io.ventura.generators.nexmark.original.Emails;
import io.ventura.generators.nexmark.original.Firstnames;
import io.ventura.generators.nexmark.original.Lastnames;
import io.ventura.generators.nexmark.original.RandomStrings;
import io.ventura.generators.nexmark.utils.ParameterTool;
import io.ventura.generators.nexmark.utils.ThreadLocalFixedSeedRandom;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

	private static final long LOGGING_THRESHOLD = 256 * ONE_MEGABYTE;

	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Generator Thread Group");

	private static final Logger LOG = LoggerFactory.getLogger(KafkaNexmarkGenerator.class);

	private static HashMap<String, int[]> PERSONS_PARTITIONS_RANGES = new HashMap<>();
	private static HashMap<String, int[]> AUCTIONS_PARTITIONS_RANGES = new HashMap<>();
	private static HashMap<String, int[]> BIDS_PARTITIONS_RANGES = new HashMap<>();


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

		BIDS_PARTITIONS_RANGES.put("localhost-2", new int[] { 0, 1});

		BIDS_PARTITIONS_RANGES.put("cloud-14-32", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		BIDS_PARTITIONS_RANGES.put("cloud-37-32", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		BIDS_PARTITIONS_RANGES.put("cloud-24-32", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		BIDS_PARTITIONS_RANGES.put("cloud-25-32", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });

		BIDS_PARTITIONS_RANGES.put("cloud-14-40", new int[] { 0, 1, 2, 3, 4, 5, 6, 7 });
		BIDS_PARTITIONS_RANGES.put("cloud-37-40", new int[] { 8, 9, 10, 11, 12, 13, 14, 15 });
		BIDS_PARTITIONS_RANGES.put("cloud-24-40", new int[] { 16, 17, 18, 19, 20, 21, 22, 23 });
		BIDS_PARTITIONS_RANGES.put("cloud-25-40", new int[] { 24, 25, 26, 27, 28, 29, 30, 31 });
		BIDS_PARTITIONS_RANGES.put("cloud-33-40", new int[] { 32, 33, 34, 35, 36, 37, 38, 39 });

		BIDS_PARTITIONS_RANGES.put("cloud-14-16", new int[] {  0,  1,  2,  3 });
		BIDS_PARTITIONS_RANGES.put("cloud-37-16", new int[] {  4,  5,  6,  7 });
		BIDS_PARTITIONS_RANGES.put("cloud-24-16", new int[] {  8,  9, 10, 11 });
		BIDS_PARTITIONS_RANGES.put("cloud-25-16", new int[] { 12, 13, 14, 15 });

		BIDS_PARTITIONS_RANGES.put("cloud-14-20", new int[] {  0,  1,  2,  3 });
		BIDS_PARTITIONS_RANGES.put("cloud-37-20", new int[] {  4,  5,  6,  7 });
		BIDS_PARTITIONS_RANGES.put("cloud-24-20", new int[] {  8,  9, 10, 11 });
		BIDS_PARTITIONS_RANGES.put("cloud-25-20", new int[] { 12, 13, 14, 15 });
		BIDS_PARTITIONS_RANGES.put("cloud-33-20", new int[] { 16, 17, 18, 19 });


		// GCP

		int PARTITIONS = 8;
		int NODES = 4;
		for (int i = 0; i < NODES; i++) {
			int tmp[] = new int[PARTITIONS];
			for (int j = 0; j < PARTITIONS; j++) {
				tmp[j] = i * PARTITIONS + j;
			}
			PERSONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-64", i + 1), tmp);
			AUCTIONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-64", i + 1), tmp);
			BIDS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-64", i + 1), tmp);
			PERSONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-32", i + 1), tmp);
			AUCTIONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-32", i + 1), tmp);
			BIDS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-32", i + 1), tmp);
			PERSONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-16", i + 1), tmp);
			AUCTIONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-16", i + 1), tmp);
			BIDS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-16", i + 1), tmp);
			PERSONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-8", i + 1), tmp);
			AUCTIONS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-8", i + 1), tmp);
			BIDS_PARTITIONS_RANGES.put(String.format("im-generator-%02d-8", i + 1), tmp);
		}
		PERSONS_PARTITIONS_RANGES.put("im-generator-01-4", new int[] {0, 1, 2, 3});
		AUCTIONS_PARTITIONS_RANGES.put("im-generator-01-4", new int[] {0, 1, 2, 3});
		BIDS_PARTITIONS_RANGES.put("im-generator-01-4", new int[] {0, 1, 2, 3});


		AUCTIONS_PARTITIONS_RANGES.put(new String(RandomStrings.RANDOM_STRINGS_NAME[0]), null); // DO NOT REMOVE! This is needed to init RandomStrings from the main thread first
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

		final int personsWorkers = params.getInt("personsWorkers", 1);
		final int auctionsWorkers = params.getInt("auctionsWorkers", 1);
		final int personsPartition = params.getInt("personsPartition", 1);
		final long inputSizeItemsPersons = params.getInt("inputSizeItemsPersons", 1);
		final int auctionsPartition = params.getInt("auctionsPartition", 1);
		final long inputSizeItemsAuctions = params.getLong("inputSizeItemsAuctions", 1);
		final String hostname = params.get("hostname", "localhost");
		final String kafkaServers = params.get("kafkaServers", "localhost:9092");
		final int kafkaBatchSize = params.getInt("kafkaBatchSize", 8192);
		final int kafkaBufferMemory = params.getInt("kafkaBufferMemory", 256 * 1024 * 1024);
		final int kafkaBatchSizeMultiplier = params.getInt("kafkaBatchSizeMultiplier", 4);
		final int kafkaLinger = params.getInt("kafkaLinger", 100);
		final int desiredAuctionsThroughputKBSec = params.getInt("desiredAuctionsThroughputKBSec", 1024);
		final String csvLoggingPath = params.get("csv", System.getProperty("java.io.tmpdir"));
		final boolean rustMode = params.getBoolean("rustMode", false);
		final boolean varyingWorkload = params.getBoolean("varyingWorkload", false);

		final boolean oneRecordPerEvent = params.getBoolean("oneRecordPerEvent", false);

		ExecutorService workers = Executors.newFixedThreadPool(personsWorkers + auctionsWorkers);

		LOG.info("Ready to start Nexmark generator with {} partitions and {} workers for persons topic ({} GB) and {} partitions and {} workers for auctions ({} GB) -- generator {} kafkaServers {}. " +
						"Emit only one record per Kafka Event: {}",
				personsPartition,
				personsWorkers,
				inputSizeItemsPersons,
				auctionsPartition,
				auctionsWorkers,
				inputSizeItemsAuctions,
				hostname,
				kafkaServers,
				oneRecordPerEvent);

		// todo change name of kafka topics, when oneRecordPerEvent==true. because we then use a different schema!

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

		int[] partitionsPersons = PERSONS_PARTITIONS_RANGES.get(hostname + "-" + personsPartition);

		LOG.debug("Selected: {}", Arrays.toString(partitionsPersons));

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

		long personStride = MAX_PERSON_ID / 5L;
		long personStart = 1_000 + personStride * helper.get(hostname);
		long personEnd = personStart + personStride;

		long auctionStride = MAX_AUCTION_ID / 5L;
		long auctionStart = 1_000 + auctionStride * helper.get(hostname);
		long auctionEnd = auctionStart + auctionStride;

		long bidsStride = MAX_BID_ID / 5L;
		long bidsStart = 1_000 + bidsStride * helper.get(hostname);
		long bidsEnd = bidsStart + bidsStride;

		try {
			int totalWorkers = auctionsWorkers + personsWorkers;
			CountDownLatch starter = new CountDownLatch(totalWorkers);
			CountDownLatch controller = new CountDownLatch(totalWorkers);
			CountDownLatch fairStarter = new CountDownLatch(1);
			long threadStridePerson = (personEnd - personStart) / totalWorkers;
			long threadStrideAuction = (auctionEnd - auctionStart) / totalWorkers;
			long threadStrideABids = (bidsEnd - bidsStart) / totalWorkers;
			for (int j = 0; j < totalWorkers; j++) {
				// every worker works for all three topics
				Properties workerConfigPerson = (Properties) cfg.clone();
				workerConfigPerson.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkPersonsGen-" + j);

				Properties workerConfigAuction = (Properties) cfg.clone();
				workerConfigAuction.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkAuctiosGen-" + j);

				Properties workerConfigBid = (Properties) cfg.clone();
				workerConfigBid.put(ProducerConfig.CLIENT_ID_CONFIG, "nexmarkBidsGen-" + j);

				// every worker gets assigned its section of the datasets
				long startP = threadStridePerson * j;
				long endP = startP + threadStridePerson;

				long startA = threadStrideAuction * j;
				long endA = startA + threadStrideAuction;

				long startB = threadStrideABids * j;
				long endB = startB + threadStrideABids;

				PersonsGenerator p = new PersonsGenerator(startP, endP);
				AuctionsGenerator a = new AuctionsGenerator(startA, endA, startP);
				BidGenerator b = new BidGenerator(startB, endB, startA, startP);

				KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons = new KafkaProducer<>(workerConfigPerson);
				KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions = new KafkaProducer<>(workerConfigAuction);
				KafkaProducer<byte[], ByteBuffer> kafkaProducerBids = new KafkaProducer<>(workerConfigBid);

				int targetPartition = partitionsPersons[j];

				GeneratorRunner runner = new GeneratorRunner(
						j,
						PERSONS_TOPIC,
						AUCTIONS_TOPIC,
						BIDS_TOPIC,
						hostname,
						targetPartition,
						a,
						p,
						b,
						kafkaProducerPersons,
						kafkaProducerAuctions,
						kafkaProducerBids,
						inputSizeItemsPersons + inputSizeItemsAuctions,
						starter,
						controller,
						fairStarter,
						desiredAuctionsThroughputKBSec,
						csvLoggingPath,
						rustMode,
						varyingWorkload,
						oneRecordPerEvent
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

	private static final String PERSONS_TOPIC = "nexmark_persons";
	private static final String AUCTIONS_TOPIC = "nexmark_auctions";
	private static final String BIDS_TOPIC = "nexmark_bids";

	private static final int BUFFER_SIZE = 8192;

	private static final int CACHED_BUFFERS = 8192 * 4;

	private static final int METADATA_SIZE = 4 + 4 + 8;

	private final static int PERSON_RECORD_SIZE = 206;
	private final static int AUCTION_RECORD_SIZE = 269;
	private final static int BID_RECORD_SIZE = 8 + 8 + 8 + 8;

	private static final int HOT_SELLER_RATIO = 100;
	private static final int HOT_AUCTION_RATIO = 100;

	private interface AbstractGenerator {
		int itemSize();

		void writeItem(long itemId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf);

		int genChecksum();
	}

	public static class BidGenerator implements AbstractGenerator {

		private final long startBid, endBid;
		private final long minPersonId, minAuctionId;

//		private static final byte[] DUMMY = new byte[64];

		BidGenerator(
				long startBid,
				long endBid,
				long minAuctionId,
				long minPersonId) {
			this.startBid = startBid;
			this.endBid = endBid;
			this.minPersonId = minPersonId;
			this.minAuctionId = minAuctionId;
		}

		@Override
		public int itemSize() {
			return BID_RECORD_SIZE;
		}

		@Override
		public void writeItem(long eventId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf) {
			long auction, bidder;

			long epoch = eventId / TOTAL_EVENT_RATIO;
			long offset = eventId % TOTAL_EVENT_RATIO;

			if (offset < PERSON_EVENT_RATIO) {
				epoch--;
				offset = AUCTION_EVENT_RATIO - 1;
			} else if (offset >= (PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO)) {
				offset = AUCTION_EVENT_RATIO - 1;
			} else {
				offset -= PERSON_EVENT_RATIO;
			}

			if (r.nextInt(100) > HOT_AUCTIONS_PROB) {
				auction = minAuctionId + (((epoch * AUCTION_EVENT_RATIO + offset + 1) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO);
			} else {
				long a = Math.max(0, epoch * AUCTION_EVENT_RATIO + offset + 1 - 20_000);
				long b = epoch * AUCTION_EVENT_RATIO + offset;
				auction = minAuctionId + a + r.nextLong(b - a + 1 + 100);
			}

			if (r.nextInt(100) > 85) {
				long personId = epoch * PERSON_EVENT_RATIO + offset;
				bidder = minPersonId + (personId / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
			} else {
				long personId = epoch * PERSON_EVENT_RATIO + offset + 1;
				long activePersons = Math.min(personId, 60_000);
				long n = r.nextLong(activePersons + 100);
				bidder = minPersonId + personId + activePersons - n;
			}

			buf.putLong(Math.abs(bidder));
			buf.putLong(Math.abs(auction));
			buf.putDouble(r.nextDouble());
			buf.putLong(timestamp);
//			buf.put(DUMMY);
		}

		@Override
		public int genChecksum() {
			return 0xdeedbeaf;
		}
	}

	public static class AuctionsGenerator implements AbstractGenerator {

		private static final int MAX_AUCTION_LENGTH_MSEC = 24 * 60 * 60 * 1_000; // 24 hours
		private static final int MIN_AUCTION_LENGTH_MSEC = 2 * 60 * 60 * 1_000; // 2 hours


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
//			long nowMillis = System.currentTimeMillis();

			if (offset < PERSON_EVENT_RATIO) {
				epoch--;
				offset = AUCTION_EVENT_RATIO - 1;
			} else if (offset >= (PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO)) {
				offset = AUCTION_EVENT_RATIO - 1;
			} else {
				offset -= PERSON_EVENT_RATIO;
			}

			long auctionId = minAuctionId + epoch * AUCTION_EVENT_RATIO + offset;//r.nextLong(minAuctionId, maxAuctionId);

			epoch = eventId / TOTAL_EVENT_RATIO;
			offset = eventId % TOTAL_EVENT_RATIO;

			if (offset >= PERSON_EVENT_RATIO) {
				offset = PERSON_EVENT_RATIO - 1;
			}
			long matchingPerson;
			if (r.nextInt(100) > HOT_SELLERS_PROB) {
				long personId = epoch * PERSON_EVENT_RATIO + offset;
				matchingPerson = minPersonId + (personId / HOT_SELLER_RATIO) * HOT_SELLER_RATIO;
			} else {
				long personId = epoch * PERSON_EVENT_RATIO + offset + 1;
				long activePersons = Math.min(personId, 60_000);
				long n = r.nextLong(activePersons + 100);
				matchingPerson = minPersonId + personId + activePersons - n;
			}
//
			buf.putLong(Math.abs(auctionId)); // 8
			buf.putLong(Math.abs(matchingPerson)); // 16
			buf.put((byte) r.nextInt(64)); // 17
			buf.putInt(r.nextInt(10) + 1); // 21
			buf.putLong(timestamp); // 25
			buf.putLong(timestamp + r.nextInt(MAX_AUCTION_LENGTH_MSEC) + MIN_AUCTION_LENGTH_MSEC); // 33
			buf.putInt(r.nextInt(1000) + 1); // 41
			int idx = r.nextInt(RandomStrings.NUM_RANDOM_STRINGS);
			buf.put(RandomStrings.RANDOM_STRINGS_NAME[idx]); // 61
			buf.put(RandomStrings.RANDOM_STRINGS_DESCR[idx]); // 261
			buf.putLong(timestamp); // 269
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
			buf.putLong(timestamp); // 206
		}

		@Override
		public int genChecksum() {
			return 0x30011991;
		}

	}


	private static final boolean USE_UNIFORM_DISTRIBUTION = readProperty("generator.use.uniform", "0").equalsIgnoreCase("1");

	private static final long PERSON_EVENT_RATIO = Integer.parseInt(readProperty("generator.person.ratio", "1"));
	private static final long AUCTION_EVENT_RATIO = Integer.parseInt(readProperty("generator.auction.ratio", "4"));
	private static final long BID_EVENT_RATIO = Integer.parseInt(readProperty("generator.bid.ratio", "0"));
	private static final long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO + BID_EVENT_RATIO;

	private static final int HOT_AUCTIONS_PROB = Integer.parseInt(readProperty("generator.auction.hot", "85"));
	private static final int HOT_SELLERS_PROB = Integer.parseInt(readProperty("generator.person.hot", "85"));

	public static class GeneratorRunner implements Runnable {

		protected final int workerId;
		private final String name, topicNamePerson, topicNameAuction, topicNameBid;
		private final byte[] genId;
		private final int targetPartition;
		private final long targetPartitionSize;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions;
		private final KafkaProducer<byte[], ByteBuffer> kafkaProducerBids;
		private final CountDownLatch controller, starter;
		private final long desiredThroughputBytesPerSecond;
		private final CountDownLatch fairStarter;

		private final AuctionsGenerator auctionsGenerator;
		private final PersonsGenerator personsGenerator;
		private final BidGenerator bidGenerator;

		private final String csvDirectory;

		private final boolean rustMode;

		private final boolean varyingWorkload;

		private final boolean oneRecordPerEvent;

		GeneratorRunner(
				int workerId,
				String topicNamePerson,
				String topicNameAuction,
				String topicNameBid,
				String name,
				int targetPartition,
				AuctionsGenerator auctionsGenerator,
				PersonsGenerator personsGenerator,
				BidGenerator bidGenerator,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerPersons,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerAuctions,
				KafkaProducer<byte[], ByteBuffer> kafkaProducerBids,
				long targetPartitionSize,
				CountDownLatch starter,
				CountDownLatch controller,
				CountDownLatch fairStarter,
				int desiredThroughputKBSec,
				String csvDirectory,
				boolean rustMode,
				boolean varyingWorkload,
				boolean oneRecordPerEvent) {
			this.targetPartitionSize = targetPartitionSize * ONE_GIGABYTE;
			this.auctionsGenerator = auctionsGenerator;
			this.topicNameAuction = topicNameAuction;
			this.personsGenerator = personsGenerator;
			this.workerId = workerId;
			this.kafkaProducerPersons = kafkaProducerPersons;
			this.kafkaProducerAuctions = kafkaProducerAuctions;
			this.kafkaProducerBids = kafkaProducerBids;
			this.bidGenerator = bidGenerator;
			this.controller = controller;
			this.starter = starter;
			this.topicNamePerson = topicNamePerson;
			this.genId = new byte[4 + 4 + 8];
			this.targetPartition = targetPartition;
			this.name = name;
			this.topicNameBid = topicNameBid;
			this.fairStarter = fairStarter;
			this.desiredThroughputBytesPerSecond = ONE_KILOBYTE * desiredThroughputKBSec;
			this.csvDirectory = csvDirectory;
			this.rustMode = rustMode;
			this.varyingWorkload = varyingWorkload;
			this.oneRecordPerEvent = oneRecordPerEvent;
		}

//		public abstract int itemSize();
//
//		public abstract void writeItem(long itemId, long timestamp, ThreadLocalFixedSeedRandom r, ByteBuffer buf);
//
//		public abstract int genChecksum();

		@Override
		public void run() {
			// if !oneRecordPerEvent we only use one set of Buffers:
			ArrayBlockingQueue<ByteBuffer> cachedBuffers = null;
			// if oneRecordPerEvent, we use three sets. The buffers have the size of anPerson/Auction/Buffer record
			ArrayBlockingQueue<ByteBuffer> cachedBuffersP = null;
			ArrayBlockingQueue<ByteBuffer> cachedBuffersA = null;
			ArrayBlockingQueue<ByteBuffer> cachedBuffersB = null;
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

				int personSize = personsGenerator.itemSize();
				int auctionSize = auctionsGenerator.itemSize();
				int bidSize = bidGenerator.itemSize();

				long ratio = targetPartitionSize / TOTAL_EVENT_RATIO;
				long personsToGenerate = (PERSON_EVENT_RATIO * ratio) / personSize;
				long auctionsToGenerate = (AUCTION_EVENT_RATIO * ratio) / auctionSize;
				long bidsToGenerate = (BID_EVENT_RATIO * ratio) / auctionSize;
				long recordsToGenerate = personsToGenerate + auctionsToGenerate + bidsToGenerate;

				int itemsPerBufferPerson = (BUFFER_SIZE - METADATA_SIZE) / personSize;
				int itemsPerBufferAuction = (BUFFER_SIZE - METADATA_SIZE) / auctionSize;
				int itemsPerBufferBid = (BUFFER_SIZE - METADATA_SIZE) / bidSize;

				AtomicLong sharedCounterPerson = new AtomicLong();
				AtomicLong sharedCounterAuction = new AtomicLong();
				AtomicLong sharedCounterBid = new AtomicLong();

				starter.countDown();
				fairStarter.await();

				ThroughtputLogger personLogger = new ThroughtputLogger(sharedCounterPerson, csvDirectory, name, topicNamePerson + "-" + workerId, 5, personSize);
				futureP = executor.scheduleAtFixedRate(personLogger, 5, 5, TimeUnit.SECONDS);

				ThroughtputLogger auctionLogger = new ThroughtputLogger(sharedCounterAuction, csvDirectory, name, topicNameAuction + "-" + workerId,5, auctionSize);
				futureA = executor.scheduleAtFixedRate(auctionLogger, 6, 5, TimeUnit.SECONDS);

				ThroughtputLogger bidsLogger = new ThroughtputLogger(sharedCounterBid, csvDirectory, name, topicNameBid + "-" + workerId,5, bidSize);
				futureB = executor.scheduleAtFixedRate(bidsLogger, 6, 5, TimeUnit.SECONDS);

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
				int chkP = personsGenerator.genChecksum();
				int chkA = auctionsGenerator.genChecksum();
				int chkB = bidGenerator.genChecksum();
				long pendingPerson = (recordsToGenerate / TOTAL_EVENT_RATIO) * PERSON_EVENT_RATIO;
				long pendingAuctions = (recordsToGenerate / TOTAL_EVENT_RATIO) * AUCTION_EVENT_RATIO;
				long pendingBids = (recordsToGenerate / TOTAL_EVENT_RATIO) * BID_EVENT_RATIO;
				long sentBytesDelta = 0;

				ByteBuffer bufP, bufA, bufB;
				// for !oneRecordPerEvent only:
				int itemsInThisBufferA=-1, itemsInThisBufferP=-1, itemsInThisBufferB=-1;
				long backlogPerson, backlogAuction, backlogBid;

				if (oneRecordPerEvent) {
					cachedBuffersP = new ArrayBlockingQueue<>(CACHED_BUFFERS);
					cachedBuffersA = new ArrayBlockingQueue<>(CACHED_BUFFERS);
					cachedBuffersB = new ArrayBlockingQueue<>(CACHED_BUFFERS);
					for (int i = 0; i < CACHED_BUFFERS; i++) {
						ByteBuffer buffP = ByteBuffer.allocate(personSize);
						ByteBuffer buffA = ByteBuffer.allocate(auctionSize);
						ByteBuffer buffB = ByteBuffer.allocate(bidSize);
						if (rustMode) {
							LOG.error("one record at a time with rust mode is not supported!"); // todo
						}
						cachedBuffersP.add(buffP);
						cachedBuffersA.add(buffA);
						cachedBuffersB.add(buffB);
					}

					bufP = cachedBuffersP.take();
					bufA = cachedBuffersA.take();
					bufB = cachedBuffersB.take();
				} else {
					cachedBuffers = new ArrayBlockingQueue<>(CACHED_BUFFERS);
					for (int i = 0; i < CACHED_BUFFERS; i++) {
						ByteBuffer buff = ByteBuffer.allocate(BUFFER_SIZE);
						if (rustMode) {
							buff.order(ByteOrder.LITTLE_ENDIAN);
						}
						cachedBuffers.add(buff);
					}

					bufP = cachedBuffers.take();
					bufA = cachedBuffers.take();
					bufB = cachedBuffers.take();
					bufA.putInt(chkA);
					bufP.putInt(chkP);
					bufB.putInt(chkB);
					itemsInThisBufferA = (int) Math.min(itemsPerBufferAuction, pendingAuctions);
					itemsInThisBufferP = (int) Math.min(itemsPerBufferPerson, pendingPerson);
					itemsInThisBufferB = (int) Math.min(itemsPerBufferBid, pendingBids);
					backlogPerson = pendingPerson - itemsInThisBufferP;
					backlogAuction = pendingAuctions - itemsInThisBufferA;
					backlogBid = pendingBids - itemsInThisBufferA;
					bufP.putInt(itemsInThisBufferP);
					bufP.putLong(backlogPerson);
					bufA.putInt(itemsInThisBufferA);
					bufA.putLong(backlogAuction);
					bufB.putInt(itemsInThisBufferB);
					bufB.putLong(backlogBid);
				}

				long sentPersons = 0;
				long sentAuctions = 0;
				long sentBids = 0;
				long eventId = 0;

				for (; eventId < recordsToGenerate; eventId++) {

					final long timestamp = System.currentTimeMillis();

					long rem = eventId % TOTAL_EVENT_RATIO;
					if (rem < PERSON_EVENT_RATIO) { // rem indicates, which type of buffer to generate next to keep the three streams balanced
						personsGenerator.writeItem(eventId, timestamp, randomness, bufP);
						pendingPerson--;
						if (oneRecordPerEvent) {
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNamePerson, targetPartition, genId, bufP);
							kafkaProducerPersons.send(kafkaRecord, new InternalCallback(cachedBuffersP, bufP, sharedCounterPerson, 1));
							bufP = cachedBuffersP.take();

							sentBytes += personSize;
							sentBytesDelta += personSize;
							throughputThrottler.acquire(personSize);
						}
						// case: !oneRecordPerEvent
						else if (bufP.remaining() < personSize) {
							bufP.position(bufP.position() + bufP.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNamePerson, targetPartition, genId, bufP);
							kafkaProducerPersons.send(kafkaRecord, new InternalCallback(cachedBuffers, bufP, sharedCounterPerson, itemsInThisBufferP));
							sentPersons += itemsInThisBufferP;
							bufP = cachedBuffers.take();
							bufP.putInt(chkP);
							itemsInThisBufferP = (int) Math.min(itemsPerBufferPerson, pendingPerson);
							backlogPerson = pendingPerson - itemsInThisBufferP;
							bufP.putInt(itemsInThisBufferP);
							bufP.putLong(backlogPerson);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
							throughputThrottler.acquire(BUFFER_SIZE);
						}
					} else if (rem < (PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO)) {
						auctionsGenerator.writeItem(eventId, timestamp, randomness, bufA);
						pendingAuctions--;
						if (oneRecordPerEvent) {
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameAuction, targetPartition, genId, bufA);
							kafkaProducerPersons.send(kafkaRecord, new InternalCallback(cachedBuffersA, bufA, sharedCounterPerson, 1));
							bufA = cachedBuffersA.take();

							sentBytes += auctionSize;
							sentBytesDelta += auctionSize;
							throughputThrottler.acquire(auctionSize);
						}
						// case: !oneRecordPerEvent
						else if (bufA.remaining() < auctionSize) {
							bufA.position(bufA.position() + bufA.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameAuction, targetPartition, genId, bufA);
							kafkaProducerAuctions.send(kafkaRecord, new InternalCallback(cachedBuffers, bufA, sharedCounterAuction, itemsInThisBufferA));
							sentAuctions += itemsInThisBufferA;
							bufA = cachedBuffers.take();
							bufA.putInt(chkA);
							itemsInThisBufferA = (int) Math.min(itemsPerBufferAuction, pendingAuctions);
							backlogAuction = pendingAuctions - itemsInThisBufferA;
							bufA.putInt(itemsInThisBufferA);
							bufA.putLong(backlogAuction);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
							throughputThrottler.acquire(BUFFER_SIZE);
						}
					} else {
						bidGenerator.writeItem(eventId, timestamp, randomness, bufB);
						pendingBids--;
						if (oneRecordPerEvent) {
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameBid, targetPartition, genId, bufB);
							kafkaProducerPersons.send(kafkaRecord, new InternalCallback(cachedBuffersB, bufB, sharedCounterPerson, 1));
							bufP = cachedBuffersB.take();

							sentBytes += bidSize;
							sentBytesDelta += bidSize;
							throughputThrottler.acquire(bidSize);
						}
						// case: !oneRecordPerEvent
						else if (bufB.remaining() < bidSize) {
							bufB.position(bufB.position() + bufB.remaining());
							ProducerRecord<byte[], ByteBuffer> kafkaRecord = new ProducerRecord<>(topicNameBid, targetPartition, genId, bufB);
							kafkaProducerBids.send(kafkaRecord, new InternalCallback(cachedBuffers, bufB, sharedCounterBid, itemsInThisBufferB));
							sentBids += itemsInThisBufferB;
							bufB = cachedBuffers.take();
							bufB.putInt(chkB);
							itemsInThisBufferB = (int) Math.min(itemsPerBufferBid, pendingBids);
							backlogBid = pendingBids - itemsInThisBufferB;
							bufB.putInt(itemsInThisBufferB);
							bufB.putLong(backlogBid);
							sentBytes += BUFFER_SIZE;
							sentBytesDelta += BUFFER_SIZE;
							throughputThrottler.acquire(BUFFER_SIZE);
						}
					}
//

					if (sentBytesDelta > LOGGING_THRESHOLD) {
						LOG.info("{} has just sent {} MB to kafka in {} sec - rate limiter {} bytes/sec",
								name,
								sentBytes / ONE_MEGABYTE,
								(timestamp - startMs) / 1_000,
								throughputThrottler.getRate());
						sentBytesDelta = 0;
					}

					if ((timestamp - throughputChangeTimestamp) > 10_000) {
						currentThroughput += throughputDelta;
						if (currentThroughput > desiredThroughputBytesPerSecondMax) {
							throughputDelta = -throughputDelta;
						} else if (currentThroughput < desiredThroughputBytesPerSecondMin) {
							currentThroughput = desiredThroughputBytesPerSecondMin;
							throughputDelta = -throughputDelta;
						}
						throughputThrottler.setRate(currentThroughput);
						throughputChangeTimestamp = timestamp;
						LOG.debug("Throttler changed to {}", currentThroughput);
					}

				}
				while (!sharedCounterPerson.compareAndSet(sentPersons, 0)) {
					Thread.sleep(100);
				}
				while (!sharedCounterAuction.compareAndSet(sentAuctions, 0)) {
					Thread.sleep(100);
				}
				while (!sharedCounterAuction.compareAndSet(sentBids, 0)) {
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
				kafkaProducerAuctions.close();
				kafkaProducerPersons.close();
				kafkaProducerBids.close();
				controller.countDown();
				if (futureA != null) {
					futureA.cancel(false);
				}
				if (futureP != null) {
					futureP.cancel(false);
				}
				if (futureB != null) {
					futureB.cancel(false);
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
