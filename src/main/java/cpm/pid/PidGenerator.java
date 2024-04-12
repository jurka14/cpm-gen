package cpm.pid;

public interface PidGenerator {
    void generate(String pid, String name);
    String getNamespace();
}
