package cpm.mlflow.dataloading;

import org.json.JSONObject;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.MlflowClient;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class RunInfoLoader extends DataLoader {

    private static final String DATA_TYPE = "xsd:string";
    private final MlflowClient client;

    public RunInfoLoader(MlflowClient client) {
        super(DATA_TYPE);
        this.client = client;
    }

    @Override
    public JSONObject loadData(String runId, JSONObject dataInfo) {
        //clear the bindings in case of repeated loading
        bindings.clear();

        String name = dataInfo.getString("name");

        Service.RunInfo runInfo = client.getRun(runId).getInfo();
        String userId = runInfo.getUserId();
        long start = runInfo.getStartTime();
        long end = runInfo.getEndTime();

        LocalDateTime startTime = LocalDateTime.ofEpochSecond(start / 1000, 0, ZoneOffset.UTC);
        LocalDateTime endTime = LocalDateTime.ofEpochSecond(end / 1000, 0, ZoneOffset.UTC);
        Duration dur = Duration.between(startTime, endTime);

        saveBinding(name + "RunId", runId, DATA_TYPE);
        saveBinding(name + "UserId", userId, DATA_TYPE);
        saveBinding(name + "Start", startTime.toString(), DATA_TYPE);
        saveBinding(name + "Duration", dur.toString(), DATA_TYPE);

        return bindings;
    }
}
