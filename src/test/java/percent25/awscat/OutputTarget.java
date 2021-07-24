package percent25.awscat;

import com.google.gson.JsonElement;

interface OutputTarget {
    void setUp();

    String address();

    JsonElement verify();

    void tearDown();
}
