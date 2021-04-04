package io.github.awscat;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class CatOptions {
  public boolean help;
  public final List<String> js = new ArrayList<>();
  public String toString() {
    return new Gson().toJson(this);
  }
}