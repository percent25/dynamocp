package io.github.awscat;

import java.io.File;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.CharMatcher;
import com.google.common.base.Suppliers;
import com.google.common.hash.Hashing;
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
    // args = new String[]{"dynamo:MyTable"};
    // args= new String[]{"dynamo:MyTableOnDemand,rcu=128","dynamo:MyTableOnDemand,delete=true,wcu=5"};
    SpringApplication.run(Main.class, args);
  }

  static class TransformUtils {
    public static String uuid() {
      return UUID.randomUUID().toString();
    }
  };

  private final ApplicationContext context;
  private final Optional<BuildProperties> buildProperties;
  
  private final List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
  private final List<OutputPluginProvider> outputPluginProviders = new ArrayList<>();

  private JsonObject transformExpressions = new JsonObject();

  // AtomicLong in = new AtomicLong();
  // AtomicLong inErr = new AtomicLong();
  // AtomicLong out = new AtomicLong();
  // AtomicLong outErr = new AtomicLong();
  AtomicLong request = new AtomicLong();
  AtomicLong success = new AtomicLong();
  AtomicLong failure = new AtomicLong();

  private final LocalMeter rate = new LocalMeter();

  class Progress {
    final Number request;
    final Number success;
    final Number failure;
    String rate;
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
    Progress(Number request, Number success, Number failure) {
      this.request = request;
      this.success = success;
      this.failure = failure;
    }
  }

  // resolved input plugin
  private InputPlugin inputPlugin;
  // resolved output plugin
  private Supplier<OutputPlugin> outputPluginSupplier;

  // lazy
  private final Supplier<PrintStream> failures = Suppliers.memoize(()->{
    try {
      String now = CharMatcher.anyOf("1234567890").retainFrom(Instant.now().toString().substring(0, 20));
      String randomString = Hashing.sha256().hashInt(new SecureRandom().nextInt()).toString().substring(0, 7);
      return failuresPrintStream = new PrintStream(new File(String.format("failures-%s-%s.json", now, randomString)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  });
  private PrintStream failuresPrintStream;

  /**
   * ctor
   */
  public Main(ApplicationArguments args, List<InputPluginProvider> inputPluginProviders, List<OutputPluginProvider> outputPluginProviders, ApplicationContext context, Optional<BuildProperties> buildProperties) {
    log("ctor");
    this.context = context;
    this.buildProperties = buildProperties;
    this.inputPluginProviders.addAll(inputPluginProviders);
    this.inputPluginProviders.add(new SystemInInputPluginProvider(args));
    this.outputPluginProviders.addAll(outputPluginProviders);
    this.outputPluginProviders.add(new SystemOutOutputPluginProvider(args));
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    log("run");

    try {

      // List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
      // List<Supplier<OutputPlugin>> outputPlugins = new ArrayList<>();

      // STEP 1 input plugin
      if (args.getNonOptionArgs().size() > 0) {
        // String source = args.getNonOptionArgs().get(0);
        for (InputPluginProvider provider : inputPluginProviders) {
          //###TODO HANDLE AMBIGUOUS INPUT PLUGINS
          //###TODO HANDLE AMBIGUOUS INPUT PLUGINS
          //###TODO HANDLE AMBIGUOUS INPUT PLUGINS
          boolean canActivate = false;
          try {
            canActivate = provider.canActivate();
          } catch (Exception e) {
            log(e);
          }
          if (canActivate)
            inputPlugin = provider.get(); // activate
          if (inputPlugin != null)
            break;
          // if (inputPluginProviders.size() != 1)
          //   throw new Exception("ambiguous sources!");
        }
        // if (inputPlugin == null)
        //   inputPlugin = new SystemInInputPluginProvider(args).get();

        // STEP 2 output plugin
        // String target = "-";
        // if (args.getNonOptionArgs().size() > 1)
        //   target = args.getNonOptionArgs().get(1);
        for (OutputPluginProvider provider : outputPluginProviders) {
          //###TODO HANDLE AMBIGUOUS OUTPUT PLUGINS
          //###TODO HANDLE AMBIGUOUS OUTPUT PLUGINS
          //###TODO HANDLE AMBIGUOUS OUTPUT PLUGINS
          boolean canActivate = false;
          try {
            canActivate = provider.canActivate();
          } catch (Exception e) {
            log(e);
          }
          if (canActivate)
            outputPluginSupplier = provider.get(); // activate
          if (outputPluginSupplier != null)
            break;
        }
        // if (outputPlugins.size() == 0)
        //   outputPlugins.add(new SystemOutOutputPluginProvider(args).get());
        // if (outputPlugins.size() != 1)
        //   throw new Exception("ambiguous targets!");

        log("inputPlugin", inputPlugin);
        log("outputPlugin", outputPluginSupplier);

        var transformValues = args.getOptionValues("transform");
        if (transformValues != null) {
          if (transformValues.iterator().hasNext()) {
            var transformValue = transformValues.iterator().next();

            transformExpressions = new Gson().fromJson(transformValue, JsonObject.class);
            // JsonObject transformExpressions = new Gson().fromJson("{'id.s':'#{ #uuid() }'}", JsonObject.class);

          }
        }

        // ----------------------------------------------------------------------
        // main loop
        // ----------------------------------------------------------------------

        inputPlugin.setListener(jsonElements->{

          return new FutureRunner(){
            Progress work = new Progress(request, success, failure);
            {
              run(()->{
                OutputPlugin outputPlugin = outputPluginSupplier.get();
                for (JsonElement jsonElement : jsonElements) {
                  run(()->{
                    // ++work.in;
                    request.incrementAndGet();

                    JsonElement jsonElementOut = jsonElement;
                    jsonElementOut = new GsonTransform(transformExpressions, TransformUtils.class).transform(jsonElementOut);

                    return outputPlugin.write(jsonElementOut);
                  }, result->{
                    // ++work.out;
                    success.incrementAndGet();
                  }, e->{
                    log(e);
                    failure.incrementAndGet();
                    //###TODO write before-transform? or post-transform?
                    //###TODO write before-transform? or post-transform?
                    //###TODO write before-transform? or post-transform?
                    failures.get().println(jsonElement.toString()); //###TODO write before-transform? or post-transform?
                    //###TODO write before-transform? or post-transform?
                    //###TODO write before-transform? or post-transform?
                    //###TODO write before-transform? or post-transform?
                  }, ()->{
                    rate.add(1);
                    work.rate = rate.toString();
                  });
                }
                return outputPlugin.flush();
              }, ()->{
                log(work);
              });
            }
          }.get();

        });

        log("start");
        inputPlugin.read().get();
        log("finish");

        // log("output.flush().get();111");
        // outputPlugin.flush().get();
        // log("output.flush().get();222");

      } else
        throw new Exception("missing source!");

    } catch (Exception e) {
      log(e);
    } finally {
      
      if (failuresPrintStream != null)
        failuresPrintStream.close();

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

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
}
