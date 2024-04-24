package cpm.mlflow.datasaving;

import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Basic class for saving data from the config to the bindings.
 */
public class DataSaver {
    protected final JSONObject bindings;
    protected final String dataType;

    public DataSaver(JSONObject bindings, String dataType) {
        this.bindings = bindings;
        this.dataType = dataType;
    }

    public void saveData(String runId, JSONObject dataInfo) {

        String name = dataInfo.getString("name");
        String data = getData(runId, dataInfo);

        saveBinding(name, data, dataType);
        saveBinding(name + "RunId", runId, "xsd:string");
    }

    protected void saveBinding(String name, String data, String dataType) {
        JSONArray arr = new JSONArray();
        arr.put(new JSONObject().put("@value", data).put("@type", dataType));
        bindings.put(name, arr);
    }

    /**
     * This method returns the data string to be stored as a binding.
     * Child classes override this method for their specific behaviour.
     * @param ignoredRunId This string may be used for further computation in overriding classes.
     */
    protected String getData(String ignoredRunId, JSONObject dataInfo) {
        return dataInfo.getString("value");
    }

}


