package cpm.mlflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cpm.mlflow.dataloading.DataLoaderProvider;
import org.json.JSONObject;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Document;
import org.openprovenance.prov.model.ProvFactory;
import org.openprovenance.prov.template.expander.Expand;
import org.openprovenance.prov.template.json.Bindings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MLFlowGenerator {
    private final JSONObject bindings;
    private final DataLoaderProvider dataLoaderProvider;

    public MLFlowGenerator(String trackingUri) {
        bindings = new JSONObject();
        dataLoaderProvider = new DataLoaderProvider(trackingUri, bindings);
    }

    private void saveRunData(String runId, String dataType, JSONObject runCfg) {

        JSONObject dataInfo = runCfg.getJSONObject(dataType);

        dataLoaderProvider.getLoader(dataType).loadData(runId, dataInfo);
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
                throw new MLFlowGenConfigException(e);
            }
        }

        JSONObject varObj = finalBindings.getJSONObject("var");
        //append generated bindings to the defaults
        for (String name : bindings.keySet()) {
            varObj.put(name, bindings.get(name));
        }

        //fill in the template
        ProvFactory pf = InteropFramework.getDefaultFactory();
        InteropFramework intF = new InteropFramework();
        Expand expand = new Expand(pf, false, false);
        Bindings bind;

        try {
            bind = new ObjectMapper().readValue(finalBindings.toString(), Bindings.class);
        } catch (JsonProcessingException e) {
            throw new MLFlowGenConfigException(e);
        }
        return expand.expander(intF.readDocumentFromFile(templatePath), bind);
    }

    public Document generate(String configPath) {

        //clear the bindings in case of repeated generation
        bindings.clear();

        JSONObject config;
        try {
            config = new JSONObject(Files.readString(Path.of(configPath)));
        } catch (IOException e) {
            throw new MLFlowGenConfigException(e);
        }


        JSONObject globalCfg = config.getJSONObject("global");
        JSONObject runsCfg = config.getJSONObject("runs");

        for(String runId : runsCfg.keySet()) {
            JSONObject runCfg = runsCfg.getJSONObject(runId);

            for(String dataType : runCfg.keySet()) {
                saveRunData(runId, dataType, runCfg);
            }
        }

        return generateDoc(globalCfg.getString("templatePath"), globalCfg.optString("bindingsPath", null));
    }
}

