package com.gemstone.gemfire.cache.lucene.internal.distributed;

import com.gemstone.gemfire.cache.lucene.internal.repository.IndexResultCollector;

/**
 * An implementation of {@link IndexResultCollector} to collect {@link EntryScore}. It is expected that the results will
 * be ordered by score of the entry.
 */
public class TopEntriesCollector implements IndexResultCollector {
  private final String name;
  private final TopEntries entries;

  public TopEntriesCollector(String name) {
    this.name = name;
    this.entries = new TopEntries();
  }

  @Override
  public void collect(Object key, float score) {
    entries.addHit(new EntryScore(key, score));
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int size() {
    TopEntries entries = getEntries();
    return entries == null ? 0 : entries.size();
  }

  /**
   * @return The entries collected by this collector
   */
  public TopEntries getEntries() {
    return entries;
  }
}