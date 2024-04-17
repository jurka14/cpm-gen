package cpm;

import com.github.wnameless.json.flattener.JsonFlattener;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;
import org.openprovenance.prov.model.Document;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.github.wnameless.json.flattener.JsonFlattener.flatten;

public class MLFlowGenerator implements DSProvGenerator {

    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";
    private final MlflowClient client;
    private final JSONObject config;
    private JSONObject bindings = new JSONObject();

    protected MLFlowGenerator(String configPath) throws IOException {
        client = new MlflowClient(TRACKING_URI);
        config = new JSONObject(Files.readString(Path.of(configPath)));
    }

    private void loadRunData(String runId) {
        JSONObject runCfg = config.getJSONObject(runId);
        JSONObject mlfCfg = runCfg.optJSONObject("config");
        JSONObject inParquet = runCfg.optJSONObject("inputParquet");
        JSONObject outParquet = runCfg.optJSONObject("outputParquet");

        if (mlfCfg != null) {
            loadConfig(runId, mlfCfg);
        }

    }

    void loadConfig(String runId, JSONObject mlfCfg) {

        String path = mlfCfg.getString("path");
        String name = mlfCfg.getString("name");

        try {
            //convert yaml to json, flatten and save
            Object obj = new Yaml().load(new FileInputStream(client.downloadArtifacts(runId, path)));
            JSONObject cfg = new JSONObject(obj);
            Map<String, Object> map = JsonFlattener.flattenAsMap(cfg.toString());

            // save to the template

        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }

    @Override
    public Document generate() {

        for(String runId : config.keySet()) {
            loadRunData(runId);
        }

        return null;
    }
}

