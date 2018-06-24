package io.ventura.generators.nexmark;

import javax.annotation.Nonnegative;

public class ThroughputThrottler {

	private static final long NS_PER_MS = 1000000L;
	private static final long NS_PER_SEC = 1000 * NS_PER_MS;
	private static final long MIN_SLEEP_NS = 10 * NS_PER_MS;

	private final long startMs;
	private final long sleepTimeNs;
	private final long targetThroughput;

	private long sleepDeficitNs = 0;
	private boolean wakeup = false;

	private long lastTimeCheck;
	private long lastAmountCheck = 0;

	/**
	 * @param targetThroughput Can be messages/sec or bytes/sec
	 * @param startMs          When the very first message is sent
	 */
	public ThroughputThrottler(@Nonnegative long targetThroughput, @Nonnegative long startMs) {
		this.lastTimeCheck = milliSecondFromNano();
		this.startMs = startMs;
		this.targetThroughput = targetThroughput;
		this.sleepTimeNs = targetThroughput > 0 ?
						   NS_PER_SEC / targetThroughput :
						   Long.MAX_VALUE;
	}

	public static long milliSecondFromNano() {
		return System.nanoTime() / 1000000;
    }


	public void throttleIfNeeded(long amountSoFar, long sendStartMs) {

		float elapsedSec = (sendStartMs - startMs) / 1000.f;

		if (elapsedSec <= 0) {
			return;
		}

		float currentThroughput = amountSoFar / elapsedSec;

		if (!(currentThroughput > this.targetThroughput)) {
			return;
		}

		float now = milliSecondFromNano();
		float interval = now - lastTimeCheck;
//		float delta = (amountSoFar - lastAmountCheck) / interval;

		sleepDeficitNs += (long) ((currentThroughput * NS_PER_SEC / targetThroughput) - interval);

		// If enough sleep deficit has accumulated, sleep a little
		if (sleepDeficitNs >= MIN_SLEEP_NS) {
			long sleepStartNs = System.nanoTime();
			try {
				synchronized (this) {
					long remaining = sleepDeficitNs;
					while (!wakeup && remaining > 0) {
						long sleepMs = remaining / 1000000;
						long sleepNs = remaining - sleepMs * 1000000;
						this.wait(sleepMs, (int) sleepNs);
						long elapsed = System.nanoTime() - sleepStartNs;
						remaining = sleepDeficitNs - elapsed;
					}
					wakeup = false;
				}
				sleepDeficitNs = 0;
			} catch (InterruptedException e) {
				// If sleep is cut short, reduce deficit by the amount of
				// time we actually spent sleeping
				long sleepElapsedNs = System.nanoTime() - sleepStartNs;
				if (sleepElapsedNs <= sleepDeficitNs) {
					sleepDeficitNs -= sleepElapsedNs;
				}
			}
			lastTimeCheck = milliSecondFromNano();
			lastAmountCheck = amountSoFar;
		}
	}


}