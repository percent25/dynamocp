package main.plugins;

import com.google.gson.Gson;

public class DynamoOptions {
  //
  public boolean debug;
  // reading
  public int rcuLimit;
  // writing
  public int wcuLimit;
  //
  public int totalSegments;
  //
  public String resume; // base64 encoded gzipped json state
  //
  public String transform_expression;

  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
  public void infer(int provisionedRcu, int provisionedWcu) {
    int concurrency = Runtime.getRuntime().availableProcessors();
    if (rcuLimit == 0)
      rcuLimit = provisionedRcu;
    if (wcuLimit == 0)
      wcuLimit = provisionedWcu;
    if (totalSegments == 0) {
      totalSegments = rcuLimit == 0 ? concurrency : Math.max(rcuLimit / 128, 1);
    }
  }

  public String toString() {
    return new Gson().toJson(this);
  }
}
