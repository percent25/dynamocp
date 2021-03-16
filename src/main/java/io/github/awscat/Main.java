package io.github.awscat;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import helpers.FutureRunner;
import helpers.GsonTransform;
import helpers.LocalMeter;
import helpers.LogHelper;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ApplicationContext;

// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle
@SpringBootApplication
public class Main implements ApplicationRunner {

  public static void main(String[] args) throws Exception {
    System.err.println("main"+Arrays.asList(args));
    // args = new String[]{"zzz.txt"};
    // args= new String[]{"dynamo:MyTabl  eOnDemand,rcu=128","dynamo:MyTableOnDemand,delete=true,wcu=5"};
    SpringApplication.run(Main.class, args);
  }

  static class TransformUtils {
    public static String uuid() {
      return UUID.randomUUID().toString();
    }
  };

  private final ApplicationContext context;
  private final Optional<BuildProperties> buildProperties;
  
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

  private JsonObject transformExpressions = new JsonObject();

  AtomicLong in = new AtomicLong();
  // AtomicLong inErr = new AtomicLong();
  AtomicLong out = new AtomicLong();
  // AtomicLong outErr = new AtomicLong();

  private final LocalMeter rate = new LocalMeter();

  class Progress {
    final Number in;
    final Number out;
    String rate;
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
    Progress(Number in, Number out) {
      this.in = in;
      this.out = out;
    }
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    log("run");

    try {

    List<InputPlugin> inputPlugins = new ArrayList<>();
    List<Supplier<OutputPlugin>> outputPlugins = new ArrayList<>();

    // STEP 1 input plugin
    if (args.getNonOptionArgs().size() > 0) {
      String source = args.getNonOptionArgs().get(0);
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
        inputPlugins.add(new SystemInInputPluginProvider().get(source, args));
      if (inputPlugins.size() != 1)
        throw new Exception("ambiguous sources!");

      // STEP 2 output plugin
      String target = "-";
      if (args.getNonOptionArgs().size() > 1)
        target = args.getNonOptionArgs().get(1);
      for (OutputPluginProvider provider : outputPluginProviders) {
        try {
          Supplier<OutputPlugin> outputPlugin = provider.get(target, args);
          if (outputPlugin != null)
            outputPlugins.add(outputPlugin);
        } catch (Exception e) {
          log(e);
        }
      }
      if (outputPlugins.size() == 0)
        outputPlugins.add(new SystemOutOutputPluginProvider().get(target, args));
      if (outputPlugins.size() != 1)
        throw new Exception("ambiguous targets!");
    }

    InputPlugin inputPlugin = inputPlugins.get(0);
    Supplier<OutputPlugin> outputPluginSupplier = outputPlugins.get(0);

    // lazy
    var dlq = Suppliers.memoize(()->{
      try {
        return new PrintStream(Files.createTempFile(Paths.get("."), "dlq", ".json").toFile());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    var values = args.getOptionValues("transform");
    if (values != null) {
      if (values.iterator().hasNext()) {
        var value = values.iterator().next();

         transformExpressions = new Gson().fromJson(value, JsonObject.class);
        // JsonObject transformExpressions = new Gson().fromJson("{'id.s':'#{ #uuid() }'}", JsonObject.class);

      }
    }


    // ----------------------------------------------------------------------
    // main loop
    // ----------------------------------------------------------------------

    inputPlugin.setListener(jsonElements->{

      return new FutureRunner(){
        Progress work = new Progress(in, out);
        {
          run(()->{
            OutputPlugin outputPlugin = outputPluginSupplier.get();
            for (JsonElement jsonElement : jsonElements) {
              run(()->{
                // ++work.in;
                in.incrementAndGet();

                JsonElement jsonElementOut = jsonElement;
                jsonElementOut = new GsonTransform(transformExpressions, TransformUtils.class).transform(jsonElementOut);

                return outputPlugin.write(jsonElementOut);
              }, result->{
                // ++work.out;
                out.incrementAndGet();
              }, e->{
                log(e);
                //###TODO dlq NEEDS FLUSH
                //###TODO dlq NEEDS FLUSH
                //###TODO dlq NEEDS FLUSH
                dlq.get().println(jsonElement.toString());
                //###TODO dlq NEEDS FLUSH
                //###TODO dlq NEEDS FLUSH
                //###TODO dlq NEEDS FLUSH
                // inErr.incrementAndGet();
              }, ()->{
                rate.add(1);
                work.rate = rate.toString();
              });
            }
            return outputPlugin.flush();
          // }, result->{
          //   work.success=true;
          // }, e->{
          //   work.failureMessage = ""+e;
          }, ()->{
            log(work);
            // log("in", in, "out", out);
          });
        }
      }.get();

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
