package com.yahoo.ycsb.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * BetterScrambledZipfianGenerator behaves similarly to
 * ScrambledZipfianGenerator but avoids an important issue.
 * ScrambledZipfianGenerator has so many buckets that the weights
 * assigned to individual buckets becomes small relative to the
 * number of buckets, and, as a result, requests are more spread
 * out than we would expect from a zipfian workload.
 *
 * BetterScrambledZipfianGenerator avoids this by establishing a
 * 1-1, random mapping of values as opposed to using a large number
 * of buckets, hashing, and reduction modulo the number of buckets.
 */
public class BetterScrambledZipfianGenerator extends NumberGenerator {
  private final int _nitems;
  private final ZipfianGenerator _gen;
  private final List<Integer> _map;

  private BetterScrambledZipfianGenerator(int nitems, double theta) {
    _nitems = nitems;
    _map = new ArrayList<>(nitems);
    for (int i = 0; i < nitems; i++) {
      _map.add(i);
    }
    Collections.shuffle(_map, new Random(1));
    _gen = new ZipfianGenerator(nitems, theta);
  }

  public static BetterScrambledZipfianGenerator withTheta(int nitems, double theta) {
    return new BetterScrambledZipfianGenerator(nitems, theta);
  }

  @Override
  public Long nextValue() {
    long ret = _gen.nextValue();
    ret = _map.get((int)ret);
    setLastValue(ret);
    return ret;
  }

  @Override
  public double mean() {
    return (double)_nitems / 2;
  }
}
