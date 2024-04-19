package cpm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;
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

public class MLFlowGenerator {

    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";

    private static final Map<String, String> METHOD_MAP = Map.of(
            "config", "loadConfig",
            "inParquet", "loadParquet",
            "outParquet", "loadParquet"
    );
    private final MlflowClient client;
    private final JSONObject bindings = new JSONObject();
    

    public MLFlowGenerator() {
        client = new MlflowClient(TRACKING_URI);
    }

    private File loadFile(String runId, JSONObject fileInfo) {
        String path = fileInfo.getString("path");
        String name = fileInfo.getString("name");
        bindings.put(name + "Path", path);

        return client.downloadArtifacts(runId, path);
    }

    private void saveData(String name, String data, String type) {
        JSONArray arr = new JSONArray();
        arr.put(new JSONObject().put("@value", data).put("@type", type));
        bindings.put(name, arr);
    }

    private void loadParquet(String runId, JSONObject parquetInfo) {

        File file = loadFile(runId, parquetInfo);

        try {
            //convert to base64 data for string storage
            String b64parquet = Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath()));

            saveData(parquetInfo.getString("name"), b64parquet, "xsd:base64Binary");

        } catch (IOException e) {
            throw new MLFlowGenException(e);
        }

    }

    private void loadConfig(String runId, JSONObject configInfo) {

        File file = loadFile(runId, configInfo);

        try {
            //convert yaml to json and save
            Object obj = new Yaml().load(new FileInputStream(file));
            JSONObject cfg = new JSONObject(obj);
            saveData(configInfo.getString("name"), cfg.toString(), "xsd:string");

        } catch (FileNotFoundException e) {
            throw new MLFlowGenException(e);
        }
    }

    private void loadData(String runId, String dataType, JSONObject runCfg) {

        JSONObject dataInfo = runCfg.getJSONObject(dataType);
        var s = METHOD_MAP.get(dataType);

        try { //choose the right method based on the dataType
            Method m = this.getClass().getDeclaredMethod(METHOD_MAP.get(dataType), String.class, JSONObject.class);
            m.setAccessible(true);
            m.invoke(this, runId, dataInfo);

        } catch (ReflectiveOperationException e) {
            throw new MLFlowGenException(e);
        }
    }

    private Document generateDoc(String templatePath, String bindingsPath) {

        JSONObject finalBindings;
        //if the default bindings are not provided, create blank ones
        if (bindingsPath == null) {
            finalBindings = new JSONObject();
            finalBindings.put("var", new JSONObject());
            finalBindings.put("context", new JSONObject());
        } else {
            try {
                finalBindings = new JSONObject(Files.readString(Path.of(bindingsPath)));
            } catch (IOException e) {
                throw new MLFlowGenException(e);
            }
        }

        JSONObject varObj = finalBindings.getJSONObject("var");
        //append generated bindings to the defaults
        for (String name : bindings.keySet()) {
            varObj.put(name, bindings.get("name"));
        }

        //fill in the template
        ProvFactory pf = InteropFramework.getDefaultFactory();
        InteropFramework intF = new InteropFramework();
        Expand expand = new Expand(pf, false, false);
        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(finalBindings.toString(), Bindings.class);
        } catch (JsonProcessingException e) {
            throw new MLFlowGenException(e);
        }
        return expand.expander(intF.readDocumentFromFile(templatePath), bind);
    }

    public Document generate(String configPath) throws MLFlowGenException {

        //clear the bindings in case of repeated generation
        bindings.clear();

        JSONObject config;
        try {
            config = new JSONObject(Files.readString(Path.of(configPath)));
        } catch (IOException e) {
            throw new MLFlowGenException(e);
        }


        JSONObject globalCfg = config.getJSONObject("global");
        JSONObject runsCfg = config.getJSONObject("runs");

        for(String runId : runsCfg.keySet()) {
            JSONObject runCfg = runsCfg.getJSONObject(runId);

            for(String dataType : runCfg.keySet()) {
                loadData(runId, dataType, runCfg);
            }
        }

        return generateDoc(globalCfg.getString("templatePath"), globalCfg.optString("bindingsPath", null));
    }
}

