package cpm.mlflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cpm.mlflow.dataloading.ConfigLoader;
import cpm.mlflow.dataloading.MLFDataLoader;
import cpm.mlflow.dataloading.ParquetLoader;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class MLFlowGenerator {

    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";
    private static final MlflowClient CLIENT = new MlflowClient(TRACKING_URI);
    private static final JSONObject BINDINGS = new JSONObject();
    private static final ConfigLoader CONFIG_LOADER = new ConfigLoader(CLIENT, BINDINGS);
    private static final ParquetLoader PARQUET_LOADER = new ParquetLoader(CLIENT, BINDINGS);
    private static final Map<String, MLFDataLoader> LOADER_MAP = Map.of(
            "config", CONFIG_LOADER,
            "inParquet", PARQUET_LOADER,
            "outParquet", PARQUET_LOADER
    );

    private void loadData(String runId, String dataType, JSONObject runCfg) {

        JSONObject dataInfo = runCfg.getJSONObject(dataType);
        MLFDataLoader loader = LOADER_MAP.get(dataType);

        try { //choose the right loader based on the dataType
            loader.load(runId, dataInfo);

        } catch (IOException e) {
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
        for (String name : BINDINGS.keySet()) {
            varObj.put(name, BINDINGS.get(name));
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
        BINDINGS.clear();

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

