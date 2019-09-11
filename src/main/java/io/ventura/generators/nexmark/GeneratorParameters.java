package io.ventura.generators.nexmark;

import com.beust.jcommander.Parameter;

public class GeneratorParameters {

	@Parameter(names = {"-k", "--kafka"}, description = "Specifies the hostname of kafka")
	public String kafkaServers = "localhost:9092";

	@Parameter(names = {"-h", "--hostname"}, description = "Specifies the hostname of kafka")
	public String hostname = "localhost";

	@Parameter(names = {"-personsPartition"}, description = "Specifies the partition of the persons topic of kafka")
	public int personsPartition = 1;

	@Parameter(names = {"-auctionsPartition"}, description = "Specifies the partition of the persons topic of kafka")
	public int auctionsPartition = 1;

	@Parameter(names = {"-bidsPartition"}, description = "Specifies the partition of the persons topic of kafka")
	public int bidsPartition = 1;

	@Parameter(names = {"-personsWorkers"}, description = "Specifies the partition of the persons topic of kafka")
	public int personsWorkers = 1;

	@Parameter(names = {"-bidsWorkers"}, description = "Specifies the partition of the persons topic of kafka")
	public int bidsWorkers = 1;

	@Parameter(names = {"-auctionsWorkers"}, description = "Specifies the partition of the persons topic of kafka")
	public int auctionsWorkers = 1;

	@Parameter(names = {"-inputSizeItemsBids"}, description = "Specifies the partition of the persons topic of kafka")
	public long inputSizeItemsBids = 1;

	@Parameter(names = {"-kafkaBufferMemory"}, description = "Specifies the partition of the persons topic of kafka")
	public int kafkaBufferMemory = 256 * 1024 * 1024;

	@Parameter(names = {"-dataBufferMemory"}, description = "Specifies the partition of the persons topic of kafka")
	public int dataBufferMemory = 256 * 1024 * 1024;

	@Parameter(names = {"-kafkaBatchSizeMultiplier"}, description = "Specifies the partition of the persons topic of kafka")
	public int kafkaBatchSizeMultiplier = 4;

	@Parameter(names = {"-kafkaLinger"}, description = "Specifies the partition of the persons topic of kafka")
	public int kafkaLinger = 100;

	@Parameter(names = {"-kafkaBatchSize"}, description = "Specifies the partition of the persons topic of kafka")
	public int kafkaBatchSize = 8196;

	@Parameter(names = {"-inputSizeItemsAuctions"}, description = "Specifies the partition of the persons topic of kafka")
	public long inputSizeItemsAuctions = 1;

	@Parameter(names = {"-inputSizeItemsPersons"}, description = "Specifies the partition of the persons topic of kafka")
	public long inputSizeItemsPersons = 1;

	@Parameter(names = {"-desiredPersonsThroughputKBSec"}, description = "Specifies the partition of the persons topic of kafka")
	public int desiredPersonsThroughputKBSec = 512;

	@Parameter(names = {"-desiredAuctionsThroughputKBSec"}, description = "Specifies the partition of the persons topic of kafka")
	public int desiredAuctionsThroughputKBSec = 512;

	@Parameter(names = {"-desiredBidsThroughputKBSec"}, description = "Specifies the partition of the persons topic of kafka")
	public int desiredBidsThroughputKBSec = 512;

}
