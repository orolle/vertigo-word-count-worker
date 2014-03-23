package net.orolle.vertigo.module;

import java.util.HashMap;

import net.kuujo.vertigo.java.RichWorkerVerticle;
import net.kuujo.vertigo.message.JsonMessage;

import org.vertx.java.core.json.JsonObject;

public class WordCount extends RichWorkerVerticle {
  private final HashMap <String, Integer> counts = new HashMap<>();

  @Override
  protected void handleMessage(JsonMessage message) {
    String word = message.body().getString("word");
    
    Integer count = counts.get(word)==null? 0 : counts.get(word);
    count++;
    counts.put(word, count);
    
    JsonObject emit = new JsonObject().putString("word", word).putNumber("count", count);
    
    System.out.println("WC: "+emit);
    emit(emit, message);
    ack(message);
  }
}
