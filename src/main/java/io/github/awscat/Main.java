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

import javax.annotation.PreDestroy;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import helpers.FutureRunner;
import helpers.LocalMeter;
import helpers.LogHelper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
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
  
  private final List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
  private final List<OutputPluginProvider> outputPluginProviders = new ArrayList<>();

  AtomicLong in = new AtomicLong(); // pre-filter
  AtomicLong out = new AtomicLong(); // post-filter
  AtomicLong request = new AtomicLong(); // output plugin
  AtomicLong success = new AtomicLong(); // output plugin
  AtomicLong failure = new AtomicLong(); // output plugin

  private final LocalMeter rate = new LocalMeter();

  class Working {
    private final Number in; // filter
    private final Number out; // filter
    private final Number request;
    private final Number success;
    private final Number failure;
    String rate;
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
    Working(Number in, Number out, Number request, Number success, Number failure) {
      this.in = in;
      this.out = out;
      this.request = request;
      this.success = success;
      this.failure = failure;
    }
  }

  // lazy
  private String failuresName;
  private PrintStream failuresPrintStream;
  private final Supplier<PrintStream> failures = Suppliers.memoize(()->{
    try {
      String now = CharMatcher.anyOf("1234567890").retainFrom(Instant.now().toString().substring(0, 20));
      String randomString = Hashing.sha256().hashInt(new SecureRandom().nextInt()).toString().substring(0, 7);
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      return failuresPrintStream = new PrintStream(new File(failuresName = String.format("failures-%s-%s.json", now, randomString)));
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  });

  @Value("${project-version}")
  private String projectVersion;

  /**
   * ctor
   */
  public Main(ApplicationArguments args, List<InputPluginProvider> inputPluginProviders, List<OutputPluginProvider> outputPluginProviders, ApplicationContext context) {
    log("ctor");
    this.context = context;
    this.inputPluginProviders.addAll(inputPluginProviders);
    this.inputPluginProviders.add(new SystemInPluginProvider(args)); // ensure last
    this.outputPluginProviders.addAll(outputPluginProviders);
    this.outputPluginProviders.add(new SystemOutPluginProvider(args)); // ensure last
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    log("run");

    log("project-version", projectVersion);

    CatOptions options = Args.parseOptions(args, CatOptions.class);
    
    log("options", options);

    try {

      // input plugin
      InputPluginProvider inputPluginProvider = resolveInputPlugin(args);
      
      // output plugin
      String target = "-";
      if (args.getNonOptionArgs().size()>1)
        target = args.getNonOptionArgs().get(1);
      OutputPluginProvider outputPluginProvider = resolveOutputPlugin(args);

      if (args.getNonOptionArgs().size() > 0) {

        var inputPlugin = inputPluginProvider.activate();
        log("inputPlugin", inputPlugin);
        var outputPluginSupplier = outputPluginProvider.activate(target);
        log("outputPlugin", outputPluginProvider);

        // ----------------------------------------------------------------------
        // main loop
        // ----------------------------------------------------------------------

        inputPlugin.setListener(jsonElements->{
          debug("listener", Iterables.size(jsonElements));
          return new FutureRunner() {
            Working work = new Working(in, out, request, success, failure);
            {
              run(()->{
                OutputPlugin outputPlugin = outputPluginSupplier.get();
                for (JsonElement jsonElement : jsonElements) {
                  run(() -> {
                    if (has(options.filter)) {
                      Expressions expressions = new Expressions(jsonElement);
                      in.incrementAndGet();
                      if (expressions.bool(options.filter)) {
                        out.incrementAndGet();
                        run(() -> {
                          request.incrementAndGet();
                          expressions.eval(options.action);
                          return outputPlugin.write(expressions.e());
                        }, result -> {
                          success.incrementAndGet();
                        }, e -> {
                          log(e);
                          // e.printStackTrace();
                          failure.incrementAndGet();
                          failures.get().println(jsonElement); // pre-transform
                        }, () -> {
                          rate.add(1);
                          work.rate = rate.toString();
                        });
                      }
                    }
                    return Futures.immediateVoidFuture();
                  });
                }
                return outputPlugin.flush();
              }, ()->{
                log(work);
                //###TODO flush failuresPrintStream here??
                //###TODO flush failuresPrintStream here??
                //###TODO flush failuresPrintStream here??
              });
            }
          }.get();

        });

        int mtu = outputPluginProvider.mtu();
        log("start", "mtu", mtu);
        inputPlugin.read(mtu).get();
        log("finish", "mtu", mtu);

        // log("output.flush().get();111");
        // outputPlugin.flush().get();
        // log("output.flush().get();222");

      } else
        throw new Exception("missing source!");

    } catch (Exception e) {
      log(e);
      e.printStackTrace();
    } finally {
    }
  }

  @PreDestroy
  public void destroy() {
    if (failuresName != null) {
      try {
        failuresPrintStream.close();
      } finally {
        log(" ########## " + failuresName + " ########## ");
      }
    }
  }

  private InputPluginProvider resolveInputPlugin(ApplicationArguments args) {
    for (InputPluginProvider provider : inputPluginProviders) {
      try {
        if (provider.canActivate())
          return provider;
      } catch (Exception e) {
        log(e);
      }
    }
    return new SystemInPluginProvider(args);
  }

  private OutputPluginProvider resolveOutputPlugin(ApplicationArguments args) {
    if (args.getNonOptionArgs().size() > 1) {
      for (OutputPluginProvider provider : outputPluginProviders) {
        try {
          if (provider.canActivate(args.getNonOptionArgs().get(1)))
            return provider;
        } catch (Exception e) {
          log(e);
        }
      }
    }
    return new SystemOutPluginProvider(args);
  }

  private boolean has(String s) {
    return Strings.nullToEmpty(s).length()>0; 
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
