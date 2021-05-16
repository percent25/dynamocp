package io.github.awscat;

import java.io.*;
import java.security.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import javax.annotation.*;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.*;
import com.google.gson.*;

import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;

import helpers.*;
import io.github.awscat.contrib.*;

// https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle
@SpringBootApplication
public class Main implements ApplicationRunner {

  public static void main(String[] args) throws Exception {
    // System.err.println("main"+Arrays.asList(args));
    // args = new String[]{"dynamo:MyTable"};
    // args= new String[]{"dynamo:MyTableOnDemand,rcu=128","dynamo:MyTableOnDemand,delete=true,wcu=5"};
    System.exit(SpringApplication.exit(SpringApplication.run(Main.class, args)));
    // System.exit(SpringApplication.exit(SpringApplication.run(Main.class, "arn:aws:sqs:us-east-1:000000000000:MyQueue,endpoint=http://localhost:4566,limit=1")));
  }
  
  private final List<InputPluginProvider> inputPluginProviders = new ArrayList<>();
  private final List<OutputPluginProvider> outputPluginProviders = new ArrayList<>();

  AtomicLong in = new AtomicLong(); // pre-filter
  // AtomicLong out = new AtomicLong(); // post-filter
  // AtomicLong request = new AtomicLong(); // output plugin
  AtomicLong success = new AtomicLong(); // output plugin
  AtomicLong failure = new AtomicLong(); // output plugin

  private final LocalMeter rate = new LocalMeter();

  class Working {
    private final Number in; // filter
    // private final Number out; // filter
    // private final Number request;
    private final Number out;
    private final Number err;
    String rate;
    public String toString() {
      return getClass().getSimpleName()+new Gson().toJson(this);
    }
    Working(Number in, Number success, Number failure) {
      this.in = in;
      // this.out = out;
      // this.request = request;
      this.out = success;
      this.err = failure;
    }
  }

  // lazy
  private String failuresFileName;
  private PrintStream failuresPrintStream;
  private final Supplier<PrintStream> failures = Suppliers.memoize(()->{
    try {
      String now = CharMatcher.anyOf("1234567890").retainFrom(Instant.now().toString().substring(0, 20));
      String randomString = Hashing.sha256().hashInt(new SecureRandom().nextInt()).toString().substring(0, 7);
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      //###TODO BUFFEREDOUTPUTSTREAM HERE??
      return failuresPrintStream = new PrintStream(new File(failuresFileName = String.format("failures-%s-%s.json", now, randomString)));
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
  public Main(List<InputPluginProvider> inputPluginProviders, List<OutputPluginProvider> outputPluginProviders) {
    debug("ctor");
    this.inputPluginProviders.addAll(inputPluginProviders);
    this.inputPluginProviders.add(new SystemInPluginProvider()); // ensure last
    this.outputPluginProviders.addAll(outputPluginProviders);
    this.outputPluginProviders.add(new SystemOutPluginProvider()); // ensure last
  }

  /**
   * run
   */
  @Override
  public void run(ApplicationArguments args) throws Exception {
    CatOptions options = Args.parseOptions(args, CatOptions.class);

    stderr("awscat.jar", projectVersion, options);

    boolean help = false;
    if (options.help)
    	help = true;
    if (options.version)
    	help = true;
    if (ImmutableSet.of().equals(ImmutableSet.copyOf(args.getNonOptionArgs())))
    	help = true;
    if (ImmutableSet.of("-h").equals(ImmutableSet.copyOf(args.getNonOptionArgs())))
    	help = true;
    if (ImmutableSet.of("-v").equals(ImmutableSet.copyOf(args.getNonOptionArgs())))
    	help = true;

    if (help) {

      final String indent = "  ";

      stderr("Usage:");
      stderr(indent, "awscat.jar [options] <source> [<target>]");
      
      stderr("options:");
      stderr(indent, "--help");
      stderr(indent, "--version");
      stderr(indent, "--js");
      stderr(indent, "--debug"); // this is spring boot's debug

      stderr("source:");
      for (InputPluginProvider pluginProvider : inputPluginProviders) {
        stderr(indent, pluginProvider.help());
      }
      stderr("target:");
      for (OutputPluginProvider pluginProvider : outputPluginProviders) {
        stderr(indent, pluginProvider.help());
      }

      return;
    }

    //###TODO probably eradicate this
    if (args.getNonOptionArgs().size() == 0) //###TODO probably eradicate this
      throw new Exception("missing source!");
    //###TODO probably eradicate this

    // input plugin
    String source = "-";
    if (args.getNonOptionArgs().size()>0)
      source = args.getNonOptionArgs().get(0);
    InputPluginProvider inputPluginProvider = resolveInputPlugin(source);
    stderr("inputPlugin", inputPluginProvider.name(), inputPluginProvider);
    InputPlugin inputPlugin = inputPluginProvider.activate(source);
    
    // output plugin
    String target = "-";
    if (args.getNonOptionArgs().size()>1)
      target = args.getNonOptionArgs().get(1);
    OutputPluginProvider outputPluginProvider = resolveOutputPlugin(target);
    stderr("outputPlugin", outputPluginProvider.name(), outputPluginProvider);
    Supplier<OutputPlugin> outputPluginSupplier = outputPluginProvider.activate(target);

    // ----------------------------------------------------------------------
    // main loop
    // ----------------------------------------------------------------------

    inputPlugin.setListener(jsonElements->{
      debug("listener", Iterables.size(jsonElements));
      return new FutureRunner() {
        Working work = new Working(in, success, failure);
        {
          run(()->{
            OutputPlugin outputPlugin = outputPluginSupplier.get();
            ExpressionsJs expressions = new ExpressionsJs();
            for (JsonElement jsonElement : jsonElements) {
              run(() -> {
                in.incrementAndGet();

                boolean filter = true;
                expressions.e(jsonElement); //###TODO .deepCopy
                for (String js : options.js)
                  filter = filter && expressions.eval(js);
                if (filter) {
                  run(() -> {
                    return outputPlugin.write(expressions.e());
                  }, result -> {
                    success.incrementAndGet();
                  }, e -> {
                    stderr(e);
                    // e.printStackTrace();
                    failure.incrementAndGet();
                    failures.get().println(jsonElement); // pre-transform
                  }, () -> {
                    rate.add(1);
                  });
                }
                return Futures.immediateVoidFuture();
              });
            }
            return outputPlugin.flush();
          }, ()->{
            work.rate = rate.toString();
            stderr(work);
            //###TODO flush failuresPrintStream here??
            //###TODO flush failuresPrintStream here??
            //###TODO flush failuresPrintStream here??
          });
        }
      }.get();

    });

    int mtu = outputPluginProvider.mtu();
    stderr("start", "mtu", mtu);
    inputPlugin.read(mtu).get();
    stderr("finish", "mtu", mtu);

  }

  @PreDestroy
  public void destroy() {
    if (failuresFileName != null) {
      try {
        failuresPrintStream.close();
      } finally {
        stderr(" ########## " + failuresFileName + " ########## ");
      }
    }
  }

  private InputPluginProvider resolveInputPlugin(String source) {
    for (InputPluginProvider provider : inputPluginProviders) {
      try {
        if (provider.canActivate(source))
          return provider;
      } catch (Exception e) {
        stderr(provider.name(), e);
      }
    }
    return new SystemInPluginProvider();
  }

  private OutputPluginProvider resolveOutputPlugin(String target) {
    for (OutputPluginProvider provider : outputPluginProviders) {
      try {
        if (provider.canActivate(target))
          return provider;
      } catch (Exception e) {
        stderr(provider.name(), e);
      }
    }
    return new SystemOutPluginProvider();
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

  private void stderr(Object... args) {
    System.err.println(new LogHelper(this).str(args));
  }

  private void debug(Object... args) {
    new LogHelper(this).debug(args);
  }
  private void trace(Object... args) {
    new LogHelper(this).trace(args);
  }
}
