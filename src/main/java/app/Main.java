package app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;

///###TODO --hash=Metric/
///###TODO --hash=Metric/
///###TODO --hash=Metric/

///###TODO --warmup-period=300
///###TODO --warmup-period=300
///###TODO --warmup-period=300

//###TODO --filter{javascript snippet??} (similar to --hash)
//###TODO --transform={javascript snippet??} (transform is a generalization of --filter)

class AppState {
  public final AtomicLong count = new AtomicLong();
  // public final List<Map<String, AttributeValue>> exclusiveStartKeys = Lists.newArrayList();

  public String toString() {
    return new Gson().toJson(this);
  }
}

class AppOptions {
  //
  public boolean debug;
  // reading
  public int rcuLimit = -1;
  // writing
  public int wcuLimit = -1;
  //
  public String resume; // base64 encoded gzipped app state
  //
  public String transform_expression;

  //
  public String toString() {
    return new Gson().toJson(this);
  }
}

// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle
@SpringBootApplication
public class Main implements ApplicationRunner {

  public static void main(String[] args) throws Exception {
    // args= new String[]{"Dlcm-qa_MetaStore","Dlcm-dev_MetaStore","--rcu-limit=1024"};
    SpringApplication.run(Main.class, args);
  }

  private final ApplicationContext context;
  private final Optional<BuildProperties> buildProperties;

  private AppState appState = new AppState();
  
  /**
   * ctor
   */
  public Main(List<InputPluginProvider> inputPluginProviders, List<OutputPluginProvider> outputPluginProviders, ApplicationContext context, Optional<BuildProperties> buildProperties) {
    this.context = context;
    this.buildProperties = buildProperties;
    this.inputPluginProviders.addAll(inputPluginProviders);
    this.outputPluginProviders.addAll(outputPluginProviders);
  }

  private final List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
  private final List<OutputPluginProvider> outputPluginProviders = new ArrayList<>();

  private AppOptions parseOptions(ApplicationArguments args) {
    JsonObject options = new JsonObject();
    for (String name : args.getOptionNames()) {
      String lowerCamel = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
      options.addProperty(lowerCamel, true);
      for (String value : args.getOptionValues(name))
        options.addProperty(lowerCamel, value);
    }
    return new Gson().fromJson(options, AppOptions.class);
  }

  // private final MetricRegistry metrics = new MetricRegistry();
  
      // private Meter rcuMeter() { return metrics.meter("rcu"); }
      // private Meter wcuMeter() { return metrics.meter("wcu"); }

      // private final AtomicLong readCount = new AtomicLong();
      // private final AtomicLong writeCount = new AtomicLong();
  
  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {

    AppOptions options = parseOptions(args);

    log("desired", options);

    // // source dynamo table name
    // sourceTable = args.getNonOptionArgs().get(0);
    // // targate dynamo table name
    // if (args.getNonOptionArgs().size()>1)
    //   targetTable = args.getNonOptionArgs().get(1);

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    if (options.rcuLimit == -1)
      options.rcuLimit = options.wcuLimit == -1 ? 128 : options.wcuLimit / 2;
    if (options.wcuLimit == -1)
      options.wcuLimit = options.rcuLimit * 8;

    // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
    // appState.exclusiveStartKeys.addAll(Collections.nCopies(Math.max(options.rcuLimit/128, 1), null));

    // if (options.resume!=null)
    //   appState = parseState(options.resume);

    log("reported", options);

        // // https://aws.amazon.com/blogs/developer/rate-limited-scans-in-amazon-dynamodb/
        // readLimiter = RateLimiter.create(options.rcuLimit);
        // writeLimiter = RateLimiter.create(options.wcuLimit);

        // // log thread count
        // log(Range.closedOpen(0, appState.totalSegments()));

    // input plugin

    List<InputPlugin> inputPlugins = new ArrayList<>();
    for (InputPluginProvider provider : inputPluginProviders) {
      log(provider);
      try {
        InputPlugin inputPlugin = provider.get(args.getNonOptionArgs().get(0), args);
        if (inputPlugin!=null)
          inputPlugins.add(inputPlugin);
      } catch (Exception e) {
        log(e);
      }
    }

    if (inputPlugins.size() == 0)
      throw new Exception("no source!");
    if (inputPlugins.size() != 1)
      throw new Exception("ambiguous sources!");

    InputPlugin inputPlugin = inputPlugins.get(0);

    // output plugin

    List<OutputPlugin> outputPlugins = new ArrayList<>();
    for (OutputPluginProvider provider : outputPluginProviders) {
      log(provider);
      try {
        OutputPlugin outputPlugin = provider.get(args.getNonOptionArgs().get(1), args);
        if (outputPlugin!=null)
          outputPlugins.add(outputPlugin);
      } catch (Exception e) {
        log(e);
      }
    }
    if (outputPlugins.size() == 0)
      throw new Exception("no target!");
    if (outputPlugins.size() != 1)
      throw new Exception("ambiguous targets!");

    OutputPlugin outputPlugin = outputPlugins.get(0);

    // ----------------------------------------------------------------------
    // main loop
    // ----------------------------------------------------------------------

    inputPlugin.setListener(jsonElements->{
      log("read", Iterables.size(jsonElements));
      var lf = outputPlugin.write(jsonElements);
      lf.addListener(()->{
        log("write", Iterables.size(jsonElements));
      }, MoreExecutors.directExecutor());
      return lf;
          // for (JsonElement jsonElement : jsonElements) {
          //   // log(jsonElement);
          //   ListenableFuture<?> lf = outputPlugin.write(jsonElement);
          //   lf.addListener(()->{
          //     try {
          //       lf.get();
          //     } catch (Exception e) {
          //       log(e);
          //     }
          //   }, MoreExecutors.directExecutor());
          // }
          // return outputPlugin.flush();
    });

    // % dynamocat MyTable MyQueue
    log("input.read().get();111");
    inputPlugin.read().get();
    log("input.read().get();222");

    // log("output.flush().get();111");
    // outputPlugin.flush().get();
    // log("output.flush().get();222");

  }

  // private static AppState parseState(String base64) throws Exception {
  //   byte[] bytes = BaseEncoding.base64().decode(base64);
  //   ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
  //   InputStream in = new GZIPInputStream(bais);
  //   String json = CharStreams.toString(new InputStreamReader(in));

  //   AppState state = new Gson().fromJson(json, AppState.class);

  //   // sigh.
  //   for (Map<String, AttributeValue> exclusiveStartKey : state.exclusiveStartKeys) {
  //     exclusiveStartKey.putAll(Maps.transformValues(exclusiveStartKey, (value) -> {
  //       //###TODO handle all types
  //       //###TODO handle all types
  //       //###TODO handle all types
  //       return AttributeValue.builder().s(value.s()).build();
  //     }));
  //   }

  //   return state;
  // }

  // private static String renderState(AppState state) throws Exception {
  //   ByteArrayOutputStream baos = new ByteArrayOutputStream();
  //   try (OutputStream out = new GZIPOutputStream(baos, true)) {
  //     out.write(new Gson().toJson(state).getBytes());
  //   }
  //   return BaseEncoding.base64().encode(baos.toByteArray());
  // }

  private void log(Object... args) {
    new LogHelper(this).log(args);
  }

}
