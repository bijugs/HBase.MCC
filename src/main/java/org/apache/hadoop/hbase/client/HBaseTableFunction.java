package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.client.Table;

public interface HBaseTableFunction<T> {
  public T call(Table table) throws Exception;
}
