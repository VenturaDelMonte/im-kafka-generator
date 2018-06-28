package io.ventura.generators.nexmark.original;

import java.util.concurrent.ThreadLocalRandom;

public class RandomStrings {

	public static final int NUM_RANDOM_STRINGS = 2000;
	public static final int STRING_LENGTH_NAME = 20;
	public static final int STRING_LENGTH_DESCR = 200;

	public static byte[][] RANDOM_STRINGS_NAME = new byte[NUM_RANDOM_STRINGS][];
	public static byte[][] RANDOM_STRINGS_DESCR = new byte[NUM_RANDOM_STRINGS][];

	private static ThreadLocalRandom random = ThreadLocalRandom.current();

	static {
		for (int i = 0; i < NUM_RANDOM_STRINGS; i++) {

			RANDOM_STRINGS_NAME[i] = new byte[STRING_LENGTH_NAME];
			RANDOM_STRINGS_DESCR[i] = new byte[STRING_LENGTH_DESCR];

			for (int j = 0; j < RANDOM_STRINGS_NAME[i].length - 1; j++) {
				if (random.nextInt(13) == 0) {
					RANDOM_STRINGS_NAME[i][j] = (byte) ' ';
				} else {
					RANDOM_STRINGS_NAME[i][j] = (byte) ('a' + random.nextInt(26));
				}
			}
			RANDOM_STRINGS_NAME[i][RANDOM_STRINGS_NAME[i].length - 1] = 0;

			for (int j = 0; j < RANDOM_STRINGS_DESCR[i].length - 1; j++) {
				if (random.nextInt(13) == 0) {
					RANDOM_STRINGS_DESCR[i][j] = (byte) ' ';
				} else {
					RANDOM_STRINGS_DESCR[i][j] = (byte) ('a' + random.nextInt(26));
				}
			}
			RANDOM_STRINGS_DESCR[i][RANDOM_STRINGS_DESCR[i].length - 1] = 0;

		}
	}

}
