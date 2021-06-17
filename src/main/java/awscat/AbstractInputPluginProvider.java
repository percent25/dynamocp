package awscat;

import java.lang.reflect.Field;

public abstract class AbstractInputPluginProvider implements InputPluginProvider {

  private final String base;
  private final Class<?> classOfOptions;

  public AbstractInputPluginProvider(String base, Class<?> classOfOptions) {
    this.base = base;
    this.classOfOptions = classOfOptions;
  }

  public String help() {
    String options = "";
    for (Field field : classOfOptions.getFields())
      options += String.format(",%s", field.getName());
    return String.format("%s[%s]", base, options);
  }

}
