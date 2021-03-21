package io.github.awscat;

import com.google.gson.Gson;

public class CatOptions {
  public boolean loop;
  public int limit;
  public String transform;
  public String toString() {
    return new Gson().toJson(this);
  }
}