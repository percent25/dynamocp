package main;

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

  //
  public String toString() {
    return new Gson().toJson(this);
  }
}
