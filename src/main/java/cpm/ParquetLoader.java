package cpm;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

public class ParquetLoader extends MLFDataLoader {

    private static final String DATA_TYPE = "xsd:base64Binary";

    protected ParquetLoader(JSONObject bindings) {
        super(bindings, DATA_TYPE);
    }

    @Override
    protected String getData(File file) throws IOException {
        return Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath()));
    }
}
