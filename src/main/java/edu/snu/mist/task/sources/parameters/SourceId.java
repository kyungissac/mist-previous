package edu.snu.mist.task.sources.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "source id")
public final class SourceId implements Name<String> {
  // empty
}
