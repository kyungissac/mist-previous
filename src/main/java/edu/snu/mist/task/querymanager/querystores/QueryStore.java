package edu.snu.mist.task.querymanager.querystores;

import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;


@DefaultImplementation(MemoryQueryStore.class)
public interface QueryStore {

  public void storeState(String queryId, String state,
                         EventHandler<Tuple<String, String>> callback);

  public void storeLogicalPlan(String queryId, String logicalPlan,
                               EventHandler<Tuple<String, String>> callback);

  public void getState(String queryId, EventHandler<Tuple<String, String>> callback);

  public void getInfo(String queryId, EventHandler<Tuple<String, String>> callback);

  public void deleteQuery(String queryId, EventHandler<String> callback);
}
