package main;

import java.util.HashMap;

import com.google.common.base.CaseFormat;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.springframework.boot.ApplicationArguments;

public class Options {
  // arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5
  public static <T> T parse(String arg, Class<T> classOfT) {
    var options = new HashMap<>();
    int index = arg.indexOf(",");
    if (index != -1) {
      options.putAll(Splitter.on(",").trimResults().withKeyValueSeparator("=").split(arg.substring(index+1)));
    }
    return new Gson().fromJson(new Gson().toJson(options), classOfT);
  }
  @Deprecated
  public static <T> T parse(ApplicationArguments args, Class<T> classOfT) {
    JsonObject options = new JsonObject();
    for (String name : args.getOptionNames()) {
      String lowerCamel = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
      options.addProperty(lowerCamel, true);
      for (String value : args.getOptionValues(name))
        options.addProperty(lowerCamel, value);
    }
    return new Gson().fromJson(options, classOfT);
  }  
}
