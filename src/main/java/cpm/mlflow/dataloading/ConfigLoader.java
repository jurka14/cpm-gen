package cpm.mlflow.dataloading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ConfigLoader extends MLFDataLoader {

    private static final String DATA_TYPE = "xsd:string";

    public ConfigLoader(MlflowClient client, JSONObject bindings) {
        super(client, bindings, DATA_TYPE);
    }

    @Override
    protected String getData(File file) throws IOException {

        //convert yaml to json and save
        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        Object obj = yamlReader.readValue(new FileInputStream(file), Object.class);

        ObjectMapper jsonWriter = new ObjectMapper();
        return jsonWriter.writeValueAsString(obj);
    }
}