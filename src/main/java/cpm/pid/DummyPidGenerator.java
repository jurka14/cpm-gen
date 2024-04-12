package cpm.pid;

import cpm.pid.PidGenerator;

public class DummyPidGenerator implements PidGenerator {
    @Override
    public void generate(String pid, String name) {
        // do not generate anything
    }

    @Override
    public String getNamespace() {
        return null;
    }
}
