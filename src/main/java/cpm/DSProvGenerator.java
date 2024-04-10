package cpm;

import org.openprovenance.prov.model.Document;

/** A domain specific provenance generator.*/
public interface DSProvGenerator {
    Document generate();
}
