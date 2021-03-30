package io.github.awscat.contrib;

import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.UUID;

import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.graalvm.polyglot.Value;

public class RootObject {
  // public JsonElement e = new JsonObject();
  private final String now;
  public RootObject(String now) {
    this.now = now;
  }
  // @HostAccess.Export
  public String now() {
    return now;
  }
  // @HostAccess.Export
  public String uuid() {
    return UUID.randomUUID().toString();
  }
  // returns a random string w/fixed length len
  public String fixedString(int len) {
    byte[] bytes = new byte[(3 * len + 3) / 4];
    new SecureRandom().nextBytes(bytes);
    String randomString = BaseEncoding.base64Url().encode(bytes).substring(0);
    return randomString.substring(0, Math.min(len, randomString.length()));
  }
  // returns a random string w/random length [1..len]
  public String randomString(int len) {
    byte[] bytes = new byte[new SecureRandom().nextInt((3 * len + 3) / 4) + 1];
    new SecureRandom().nextBytes(bytes);
    String randomString = BaseEncoding.base64Url().encode(bytes).substring(0);
    return randomString.substring(0, Math.min(len, randomString.length()));
  }
  public String toString() {
    return new Gson().toJson(this);
  }
}
