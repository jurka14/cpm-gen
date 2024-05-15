package cpm.mlflow.dataloading;

import org.json.JSONObject;
import org.mlflow.api.proto.Service.Metric;
import org.mlflow.tracking.MlflowClient;

import java.util.Base64;
import java.util.List;

/**
 * Saves the metrics based on the keys specified in config.
 * If no keys are specified, all metrics from the run are saved.
 * The metrics are saved as a JSON object string encoded in base64.
 */
public class MetricLoader extends DataLoader {

    private static final String DATA_TYPE = "xsd:string";
    private final MlflowClient client;

    public MetricLoader(MlflowClient client, JSONObject bindings) {
        super(bindings, DATA_TYPE);
        this.client = client;
    }

    @Override
    protected String getData(String runId, JSONObject dataInfo) {

        List<Object> list = dataInfo.getJSONArray("keyList").toList();
        JSONObject finalData = new JSONObject();

        List<Metric> allMetrics = client.getRun(runId).getData().getMetricsList();

        for (Metric m : allMetrics) {
            if (list.isEmpty() || list.contains(m.getKey())) {
                finalData.put(m.getKey(), m.getValue());
            }
        }

        return Base64.getEncoder().encodeToString(finalData.toString().getBytes());
    }

}
