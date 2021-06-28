package awscat;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class CommandOptions {
  public boolean help;
  public boolean version;
  public boolean debug;
  public boolean trace;
  
  // javascript filters and transforms
  public final List<String> js = new ArrayList<>();
  
  public String toString() {
    return new Gson().toJson(this);
  }
}