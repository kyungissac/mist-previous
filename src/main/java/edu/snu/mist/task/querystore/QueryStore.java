package edu.snu.mist.task.querystore;

import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;


@DefaultImplementation(MemoryQueryStore.class)
public interface QueryStore {

  public void storeState(String queryId, Tuple<String, byte[]> tuple,
                         EventHandler<Tuple<String, byte[]>> callback);

  public void storeInfo(String queryId, Tuple<String, byte[]> tuple,
                        EventHandler<Tuple<String, byte[]>> callback);

  public void getState(String queryId, EventHandler<byte[]> callback);

  public void getInfo(String queryId, EventHandler<byte[]> callback);

  public void deleteQuery(String queryId, EventHandler<String> callback);
}
