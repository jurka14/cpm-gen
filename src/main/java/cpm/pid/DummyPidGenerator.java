package cpm.pid;

import cpm.pid.uri.PidUriGenerator;

public class DummyPidGenerator extends PidGenerator {
    public DummyPidGenerator(PidUriGenerator uriGenerator) {
        super(uriGenerator);
    }

    @Override
    public void generate(String pid, String name) {
        // do not generate anything
    }

    @Override
    public String getNamespace() {
        return null;
    }
}
