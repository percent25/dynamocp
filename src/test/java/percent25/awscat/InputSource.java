package percent25.awscat;

import com.google.gson.JsonElement;

interface InputSource {
    void setUp();

    void load(JsonElement jsonElement);

    String address();

    void tearDown();
}
