package awscat;

import com.google.gson.JsonElement;

interface OutputTargetArg {
    void setUp();

    String address();

    JsonElement verify();

    void tearDown();
}
