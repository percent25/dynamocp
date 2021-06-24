package awscat;

import com.google.gson.JsonElement;

interface OutputTargetArg {
    void setUp();

    String targetArg();

    JsonElement verify();

    void tearDown();
}
