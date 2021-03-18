package io.github.awscat;

import org.springframework.boot.ApplicationArguments;

// arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5
public class Args {
    /**
     * parseArg
     * 
     * @param arg e.g., "dynamo:MyTable,c=1,delete=true,wcu=5"
     * @return e.g., "dynamo:MyTable"
     */
    public static String base(String arg) {

        // name,foo=1,bar=2
        // ns:name,foo=1,bar=2

        // dynamo:MyTable,c=1,delete=true,wcu=5
        // arn:aws:dynamo:us-east-1:102938475610:table/MyTable,c=1,delete=true,wcu=5

        int index = arg.indexOf(",");
        if (index != -1)
            arg = arg.substring(0, index);
        return arg;
    }

    // public static String source(ApplicationArguments args) {
    //     return parseArg(args.getNonOptionArgs().get(0));
    // }

    // public static String target(ApplicationArguments args) {
    //     return parseArg(args.getNonOptionArgs().get(1));
    // }
}
