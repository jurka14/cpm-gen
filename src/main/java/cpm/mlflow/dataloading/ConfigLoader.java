package cpm.mlflow.dataloading;

import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;
import org.yaml.snakeyaml.Yaml;

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
        Object obj = new Yaml().load(new FileInputStream(file));
        JSONObject cfg = new JSONObject(obj);

        return cfg.toString();
    }
}