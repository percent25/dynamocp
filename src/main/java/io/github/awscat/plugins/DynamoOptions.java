package io.github.awscat.plugins;

import com.google.gson.Gson;

public class DynamoOptions {
  // //
  // public boolean debug;
  // //
  // public String resume; // base64 encoded gzipped json state

  //###TODO rename to parallelism or totalSegments ?
  public int c; // concurrency, aka totalSegments
  public int rcu; // input
  public int wcu; // output
  public int limit; // input
  public boolean delete; // output

  public int totalSegments() {
    return c;
  }
  
  // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
  public void infer(int concurrency, int provisionedRcu, int provisionedWcu) {
    if (rcu == 0)
      rcu = provisionedRcu;
    if (wcu == 0)
      wcu = provisionedWcu;
    if (c == 0) {
      c = rcu == 0 ? concurrency : Math.max(rcu / 128, 1);
    }
  }

  public String toString() {
    return new Gson().toJson(this);
  }
}
