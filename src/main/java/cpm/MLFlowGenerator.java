package cpm;

import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;
import org.openprovenance.prov.model.Document;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

public class MLFlowGenerator implements DSProvGenerator {

    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";

    private static final Map<String, Object> METHOD_MAP = Map.of(
            "config", "loadConfig",
            "inParquet", "loadParquet",
            "outParquet", "loadParquet"
    );
    private final MlflowClient client;
    private final JSONObject config;
    private final JSONObject bindings = new JSONObject();
    

    public MLFlowGenerator(String configPath) throws IOException {
        client = new MlflowClient(TRACKING_URI);
        config = new JSONObject(Files.readString(Path.of(configPath)));
    }

    private File loadFile(String runId, JSONObject fileInfo) {
        String path = fileInfo.getString("path");
        String name = fileInfo.getString("name");
        bindings.put(name + "Path", path);

        return client.downloadArtifacts(runId, path);
    }

    private void loadParquet(String runId, JSONObject parquetInfo) {

        File file = loadFile(runId, parquetInfo);

        try {
            //convert to base64 data for json storage
            String b64parquet = Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath()));

            bindings.put(parquetInfo.getString("name"), b64parquet);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

    }

    private void loadConfig(String runId, JSONObject configInfo) {

        File file = loadFile(runId, configInfo);

        try {
            //convert yaml to json and save
            Object obj = new Yaml().load(new FileInputStream(file));
            JSONObject cfg = new JSONObject(obj);
            bindings.put(configInfo.getString("name"), cfg);

        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    private void loadData(String runId, String dataType, JSONObject runCfg) {

        JSONObject dataInfo = runCfg.getJSONObject(dataType);

        try { //choose the right method based on reflection
            Method m = this.getClass().getMethod((String) METHOD_MAP.get(dataType), String.class, JSONObject.class);
            m.invoke(this, runId, dataInfo);

        } catch (ReflectiveOperationException e) {
            System.err.println(e.getMessage());
        }
    }

    private Document generateDoc(String templatePath, String bindingsPath) {
        return null;
    }

    @Override
    public Document generate() {

        JSONObject globalCfg = config.getJSONObject("global");
        JSONObject runsCfg = config.getJSONObject("runs");

        for(String runId : runsCfg.keySet()) {
            JSONObject runCfg = runsCfg.getJSONObject(runId);

            for(String dataType : runCfg.keySet()) {
                loadData(runId, dataType, runCfg);
            }
        }

        return generateDoc(globalCfg.getString("templatePath"), globalCfg.getString("bindingsPath"));
    }
}

