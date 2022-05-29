package org.apache.druid.client.cache.ORF.online;


import org.apache.druid.client.cache.ORF.structure.Result;
import org.apache.druid.client.cache.ORF.structure.Sample;

public interface Classifier {
    enum Type {
        ONLINERANDOMFOREST;
    }
    void update(Sample sample);
    void eval(Sample sample, Result result);
}
