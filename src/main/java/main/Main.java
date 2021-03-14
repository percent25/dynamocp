package main;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import helpers.LogHelper;

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

// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle
@SpringBootApplication
public class Main implements ApplicationRunner {

  public static void main(String[] args) throws Exception {
    System.err.println("main"+Arrays.asList(args));
    // args= new String[]{"dynamo:MyTabl  eOnDemand,rcu=128","dynamo:MyTableOnDemand,delete=true,wcu=5"};
    SpringApplication.run(Main.class, args);
  }

  private final ApplicationContext context;
  private final Optional<BuildProperties> buildProperties;

  private AppState appState = new AppState();
  
  /**
   * ctor
   */
  public Main(List<InputPluginProvider> inputPluginProviders, List<OutputPluginProvider> outputPluginProviders, ApplicationContext context, Optional<BuildProperties> buildProperties) {
    log("ctor");
    this.context = context;
    this.buildProperties = buildProperties;
    this.inputPluginProviders.addAll(inputPluginProviders);
    this.outputPluginProviders.addAll(outputPluginProviders);
  }

  private final List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
  private final List<OutputPluginProvider> outputPluginProviders = new ArrayList<>();

  AtomicLong in = new AtomicLong();
  AtomicLong out = new AtomicLong();

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    log("run");

    try {

    // input plugin
    String source = args.getNonOptionArgs().get(0);
    List<InputPlugin> inputPlugins = new ArrayList<>();
    for (InputPluginProvider provider : inputPluginProviders) {
      try {
        InputPlugin inputPlugin = provider.get(source, args);
        if (inputPlugin!=null)
          inputPlugins.add(inputPlugin);
      } catch (Exception e) {
        log(e);
      }
    }
    if (inputPlugins.size() == 0)
      inputPlugins.add(new SystemInInputPlugin(new FileInputStream(source)));
    if (inputPlugins.size() != 1)
      throw new Exception("ambiguous sources!");
    InputPlugin inputPlugin = inputPlugins.get(0);

    // output plugin
    List<Supplier<OutputPlugin>> outputPlugins = new ArrayList<>();
    if (args.getNonOptionArgs().size()>1) {
      String target = args.getNonOptionArgs().get(1);
      for (OutputPluginProvider provider : outputPluginProviders) {
        try {
          Supplier<OutputPlugin> outputPlugin = provider.get(target, args);
          if (outputPlugin!=null)
            outputPlugins.add(outputPlugin);
        } catch (Exception e) {
          log(e);
        }
      }  
    }
    if (outputPlugins.size() == 0)
      outputPlugins.add(new SystemOutOutputPluginProvider().get("-", args));
    if (outputPlugins.size() != 1)
      throw new Exception("ambiguous targets!");
    Supplier<OutputPlugin> outputPlugin = outputPlugins.get(0);

    // ----------------------------------------------------------------------
    // main loop
    // ----------------------------------------------------------------------

    inputPlugin.setListener(jsonElements->{

      in.addAndGet(Iterables.size(jsonElements));
      log("in", in, "out", out, "[read]");

      var lf = outputPlugin.get().write(jsonElements);
      lf.addListener(()->{

        out.addAndGet(Iterables.size(jsonElements));
        log("in", in, "out", out, "[write]");

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

  } catch (Exception e) {
    log(e.getMessage());
  }
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
