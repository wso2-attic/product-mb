package org.dna.mqtt.moquette.messaging.spi;

/**
 * TODO: ADD A SUITABLE COMMENT HERE
 */
public interface IMatchingCondition {
    boolean match(String key);
}

