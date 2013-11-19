package com.jivesoftware.os.amza.shared;

import java.util.NavigableMap;

public interface TableIndex<K, V> extends NavigableMap<K, TimestampedValue<V>> {

   /**
    * Force persistence of all changes
    */
   void flush();
}
