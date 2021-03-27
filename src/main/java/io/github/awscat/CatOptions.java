package io.github.awscat;

import com.google.gson.Gson;

public class CatOptions {
  public String filter = "true";
  public String action = "e";
  public String toString() {
    return new Gson().toJson(this);
  }
}