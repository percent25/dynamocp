package main.plugins;

import com.google.gson.Gson;

public class DynamoOptions {
  //
  public boolean debug;
  // reading
  public int rcuLimit = -1;
  // writing
  public int wcuLimit = -1;
  //
  public int totalSegments;
  //
  public String resume; // base64 encoded gzipped json state
  //
  public String transform_expression;

  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
  public void infer() {
    if (rcuLimit == -1)
      rcuLimit = wcuLimit == -1 ? 128 : wcuLimit / 2;
    if (wcuLimit == -1)
      wcuLimit = rcuLimit * 8; // .5 rcu per 4KB eventual read / 1 wcu per 1KB write

    if (totalSegments == 0)
      totalSegments = Math.max(rcuLimit / 128, 1);
  }

  public String toString() {
    return new Gson().toJson(this);
  }
}
