package awscat;

import java.lang.reflect.Type;
import java.util.*;

import com.google.common.base.*;
import com.google.gson.*;

import org.springframework.boot.*;

// arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5
public class Args { //###TODO RENAME TO Addresses
    /**
     * parseArg
     * 
     * @param arg e.g., "dynamo:MyTable,c=1,delete=true,wcu=5"
     * @return e.g., "dynamo:MyTable"
     */
    public static String base(String arg) { //###TODO RENAME TO ADDRESSBASE

        // name,foo=1,bar=2
        // ns:name,foo=1,bar=2

        // dynamo:MyTable,c=1,delete=true,wcu=5
        // arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5

        int index = arg.indexOf(",");
        if (index != -1)
            arg = arg.substring(0, index);
        return arg;
    }

    // arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5
    public static <T> T options(String arg, Type typeOfT) { //###TODO RENAME TO ADDRESSOPTIONS
        Map<String, String> options = new HashMap<>();
        Iterator<String> iter = Splitter.on(",").trimResults().split(arg).iterator();
        iter.next();
        while (iter.hasNext()) {
            Iterator<String> keyValue = Splitter.on("=").trimResults().split(iter.next()).iterator();
            String key = keyValue.next();
            String value = "true";
            if (keyValue.hasNext())
                value = keyValue.next();
            options.put(key, value);
        }
        return new Gson().fromJson(new Gson().toJson(options), typeOfT);
    }
    // public static <T> T options(String arg, Class<T> classOfT) { //###TODO RENAME TO ADDRESSOPTIONS
    //     return options(arg, (Type) classOfT);
    // }
  
    // public static String source(ApplicationArguments args) {
    //     return parseArg(args.getNonOptionArgs().get(0));
    // }

    // public static String target(ApplicationArguments args) {
    //     return parseArg(args.getNonOptionArgs().get(1));
    // }

    // public static <T> T parseOptions(ApplicationArguments args, Class<T> classOfT) throws Exception {
    //     JsonObject options = new Gson().toJsonTree(classOfT.getConstructor().newInstance()).getAsJsonObject();
    //     for (String name : args.getOptionNames()) {
    //         String lowerCamel = CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name);
    //         if (args.getOptionValues(name).size()==0) {
    //             options.addProperty(lowerCamel, true);
    //         } else {
    //             for (String value : args.getOptionValues(name)) {
    //                 JsonElement jsonElement = options.get(lowerCamel);
    //                 if (jsonElement == null || !jsonElement.isJsonArray())
    //                     options.addProperty(lowerCamel, value);
    //                 else
    //                     jsonElement.getAsJsonArray().add(value);
    //             }    
    //         }
    //     }
    //     return new Gson().fromJson(options, classOfT);
    // }
    
}
