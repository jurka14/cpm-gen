package cpm;

import org.json.JSONObject;

import java.io.IOException;

public abstract class MLFConfigGenerator extends MLFlowGenerator {

    protected String mlfConfigPath;
    protected JSONObject mlfConfig;

    protected MLFConfigGenerator(String configPath) throws IOException {
        super(configPath);
    }

    private void loadMLFConfig() {
        mlfConfigPath = "";
    }
}
