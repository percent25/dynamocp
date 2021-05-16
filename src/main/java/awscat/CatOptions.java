package awscat;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class CatOptions {
  public boolean help;
  public boolean version;
  
  // the javascript actions to evaluate
  public final List<String> js = new ArrayList<>();
  
  public String toString() {
    return new Gson().toJson(this);
  }
}