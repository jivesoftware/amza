/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

public class TableName<K, V> implements Comparable<TableName<K, V>>, Serializable {

    private final String ringName;
    private final String tableName;
    private final Class<K> keyClass;
    private final K minKeyInclusive;
    private final K maxKeyExclusive;
    private final Class<V> valueClass;

    @JsonCreator
    public TableName(@JsonProperty("ringName") String ringName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("keyClass") Class<K> keyClass,
            @JsonProperty("minKeyInclusive") K minKeyInclusive,
            @JsonProperty("maxKeyExclusive") K maxKeyExclusive,
            @JsonProperty("valueClass") Class<V> valueClass) {
        this.ringName = ringName.toUpperCase();
        this.tableName = tableName;
        this.keyClass = keyClass;
        this.minKeyInclusive = minKeyInclusive;
        this.maxKeyExclusive = maxKeyExclusive;
        this.valueClass = valueClass;
    }

    public String getRingName() {
        return ringName;
    }

    public String getTableName() {
        return tableName;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public K getMinKeyInclusive() {
        return minKeyInclusive;
    }

    public K getMaxKeyExclusive() {
        return maxKeyExclusive;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    @Override
    public String toString() {
        return "TableName{"
                + "ringName=" + ringName
                + ", tableName=" + tableName
                + ", keyClass=" + keyClass
                + ", minKeyInclusive=" + minKeyInclusive
                + ", maxKeyExclusive=" + maxKeyExclusive
                + ", valueClass=" + valueClass
                + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.ringName);
        hash = 89 * hash + Objects.hashCode(this.tableName);
        hash = 89 * hash + Objects.hashCode(this.keyClass);
        hash = 89 * hash + Objects.hashCode(this.minKeyInclusive);
        hash = 89 * hash + Objects.hashCode(this.maxKeyExclusive);
        hash = 89 * hash + Objects.hashCode(this.valueClass);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TableName<K, V> other = (TableName<K, V>) obj;
        if (!Objects.equals(this.ringName, other.ringName)) {
            return false;
        }
        if (!Objects.equals(this.tableName, other.tableName)) {
            return false;
        }
        if (!Objects.equals(this.keyClass, other.keyClass)) {
            return false;
        }
        if (!Objects.equals(this.minKeyInclusive, other.minKeyInclusive)) {
            return false;
        }
        if (!Objects.equals(this.maxKeyExclusive, other.maxKeyExclusive)) {
            return false;
        }
        if (!Objects.equals(this.valueClass, other.valueClass)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(TableName<K, V> o) {
        int i = ringName.compareTo(o.ringName);
        if (i != 0) {
            return i;
        }
        i = tableName.compareTo(o.tableName);
        if (i != 0) {
            return i;
        }
        i = keyClass.getName().compareTo(o.keyClass.getName());
        if (i != 0) {
            return i;
        }
        i = valueClass.getName().compareTo(o.valueClass.getName());
        if (i != 0) {
            return i;
        }
//        i = minKeyInclusive.compareTo(o.minKeyInclusive);
//        if (i != 0) {
//            return i;
//        }
//        i = maxKeyExclusive.compareTo(o.maxKeyExclusive);
//        if (i != 0) {
//            return i;
//        }
        return i;
    }
}